from string import Template as T
from origins import utils, provenance
from origins.graph import neo4j
from ..model import Result, Node, Edge
from ..packer import pack
from ..cypher import labels_string
from . import edges

__all__ = (
    'get',
    'get_by_id',
    'add',
    'set',
    'remove',
)


# Returns single node by UUID
GET_NODE = '''
MATCH (n:`origins:Node` {`origins:uuid`: { uuid }})
RETURN n
LIMIT 1
'''

# Returns the latest node by ID. This uses labels to constrain
# the lookup. If none are supplied, the general `origins:Node`
# will be used.
GET_NODE_BY_ID = T('''
MATCH (n:`origins:Node`$labels {`origins:id`: { id }})
WHERE NOT (n)<-[:`prov:entity`]-(:`prov:Invalidation`)
RETURN n
LIMIT 1
''')


# Creates and returns a node
# The `prov:Entity` label is added to hook into the provenance data model
ADD_NODE = T('''
CREATE (n:`origins:Node`:`prov:Entity`$labels { attrs })
''')


# Returns all outbound edges of the node. That is, the node
# is the start of a directed edge.
NODE_OUTBOUND_EDGES = '''
MATCH (s:`origins:Node` {`origins:uuid`: { uuid }})<-[:`origins:start`]-(n:`origins:Edge`)
WHERE NOT (n)<-[:`prov:entity`]-(:`prov:Invalidation`)
WITH n, s
MATCH (n)-[:`origins:end`]->(e:`origins:Node`)
RETURN n, s, e, labels(n)
'''  # noqa


def _prepare_statement(statement, label=None, **mapping):
    mapping['labels'] = labels_string(label)

    return statement.safe_substitute(mapping)


def get(uuid, labels=None, tx=neo4j.tx):
    t = utils.Timer()

    if isinstance(uuid, Node):
        uuid = uuid['uuid']
    elif isinstance(uuid, Result):
        uuid = uuid['data']['uuid']

    with t('prep'):
        query = {
            'statement': GET_NODE,
            'parameters': {
                'uuid': uuid,
            }
        }

    with t('exec'):
        result = tx.send(query)

        if not result:
            raise ValueError('node does not exist')

    return Result(
        data=Node.parse(result[0]),
        perf=t.results,
        prov=None,
        time=utils.timestamp(),
    )


def get_by_id(_id, label=None, tx=neo4j.tx):
    t = utils.Timer()

    if isinstance(_id, Node):
        _id = _id['id']
    elif isinstance(_id, Result):
        _id = _id['data']['id']

    with t('prep'):
        statement = _prepare_statement(GET_NODE_BY_ID,
                                       label=label)

        query = {
            'statement': statement,
            'parameters': {
                'id': _id,
            }
        }

    with t('exec'):
        result = tx.send(query)

        if not result:
            raise ValueError('node does not exist')

        data = Node.parse(result[0])

    return Result(
        data=data,
        prov=None,
        perf=t.results,
        time=utils.timestamp(),
    )


def add(attrs=None, new=True, label=None, tx=neo4j.tx):
    t = utils.Timer()

    with tx as tx:
        with t('prep'):
            node = Node.new(attrs)

            statement = _prepare_statement(ADD_NODE, label=label)

            parameters = {
                'attrs': pack(node),
            }

            query = {
                'statement': statement,
                'parameters': parameters,
            }

        with t('exec'):
            tx.send(query)

        # Provenance for add
        with t('prov'):
            prov_spec = provenance.add(node['uuid'], new=new)
            prov = provenance.load(prov_spec, tx=tx)

        return Result(
            perf=t.results,
            time=utils.timestamp(),
            data=node,
            prov=prov,
        )


def _update_edge(attrs, label, new, tx):
    """Updates an edge with new start and end nodes.

    This creates a new revision of the edge.
    """
    t = utils.Timer()

    previous = attrs.pop('uuid')

    # Removes the physical edge between the two nodes
    with t('exec'):
        etype = attrs.get('type', 'null')
        statement = edges._prepare_statement(edges.DELETE_EDGE, etype=etype)

        query = {
            'statement': statement,
            'parameters': {
                'start': attrs['start']['uuid'],
                'end': attrs['end']['uuid'],
            }
        }

        tx.send(query)

    with t('add'):
        edge = edges.add(new, attrs['end'], attrs=attrs, label=label,
                         tx=tx)

    with t('prov'):
        prov_spec = provenance.change(previous, edge['data']['uuid'],
                                      reason='origins:NodeChange')
        prov_spec['wasGeneratedBy'] = edge['prov']['wasGeneratedBy']
        prov_spec['wasDerivedFrom']['wdf']['prov:generation'] = 'wgb'
        prov = provenance.load(prov_spec, tx=tx)

    return Result(
        perf=t.results,
        time=utils.timestamp(),
        data=edge['data'],
        prov=prov,
    )


def _update_edges(old, new, tx):
    """Gets all directed edges where `old` is the start node and
    changes the edge to `new` (a new revision of old).
    """
    query = {
        'statement': NODE_OUTBOUND_EDGES,
        'parameters': {
            'uuid': old,
        }
    }

    result = tx.send(query)

    results = []

    # TODO, handle incoming edges
    for (edge, start, end, edge_labels) in result:
        edge = Edge.parse([edge, start, end])
        result = _update_edge(edge, edge_labels, new, tx)
        results.append(result)

    return results


def set(uuid, attrs=None, new=True, label=None, force=False, tx=neo4j.tx):
    t = utils.Timer()

    if isinstance(uuid, Node):
        uuid = uuid['uuid']
    elif isinstance(uuid, Result):
        uuid = uuid['data']['uuid']

    with tx as tx:
        with t('get'):
            prev = get(uuid, tx=tx)

        with t('prep'):
            # Create new node merged into the previous attributes
            node = Node.new(attrs, merge=prev['data'])

            # Calculate diff
            diff = node.diff(prev['data'])

            if not diff and not force:
                return

        with t('add'):
            # Create a new version of the entity
            rev = add(node, new=new, label=label, tx=tx)
            t.add_results('add', rev['perf'])

        with t('update_edges'):
            # Copy outbound relationships from the previous version
            _update_edges(prev['data']['uuid'],
                          rev['data']['uuid'], tx=tx)

        # Provenance for change
        with t('prov'):
            prov_spec = provenance.change(prev['data']['uuid'],
                                          rev['data']['uuid'])
            prov_spec['wasGeneratedBy'] = rev['prov']['wasGeneratedBy']
            prov_spec['wasDerivedFrom']['wdf']['prov:generation'] = 'wgb'

            prov = provenance.load(prov_spec, tx=tx)

        return Result(
            perf=t.results,
            time=utils.timestamp(),
            data=rev['data'],
            diff=diff,
            prov=prov,
        )


def remove(uuid, reason=None, tx=neo4j.tx):
    t = utils.Timer()

    if isinstance(uuid, Node):
        uuid = uuid['uuid']
    elif isinstance(uuid, Result):
        uuid = uuid['data']['uuid']

    with tx as tx:
        with t('get'):
            node = get(uuid, tx=tx)

        # Provenance for remove
        with t('prov'):
            prov_spec = provenance.remove(node['data']['uuid'], reason=reason)
            prov = provenance.load(prov_spec, tx=tx)

        return Result(
            perf=t.results,
            time=utils.timestamp(),
            data=node['data'],
            prov=prov,
        )

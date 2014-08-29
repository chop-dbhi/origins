from uuid import uuid4
from string import Template as T
from origins import utils, provenance
from origins.graph import neo4j
from ..model import Result, Node
from ..utils import merge_attrs
from ..packer import pack, unpack
from ..cypher import labels_string
from .parsers import parse_node
from . import edges

__all__ = (
    'get',
    'get_by_id',
    'add',
    'set',
    'remove',
)

DIFF_IGNORE = {
    'id',
    'uuid',
    'time',
}


# Returns single node by UUID
GET_NODE = '''
MATCH (n:`origins:Node` {`origins:uuid`: { uuid }})
RETURN n
LIMIT 1
'''

# Returns the latest node by ID
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
RETURN n
''')


# Returns all outbound edges of the node. That is, the node
# is the start of a directed edge.
NODE_OUTBOUND_EDGES = '''
MATCH (n:`origins:Node` {`origins:uuid`: { uuid }})<-[:`origins:start`]-(e:`origins:Edge`)
WHERE NOT (e)<-[:`prov:entity`]-(:`prov:Invalidation`)
WITH e
MATCH (e)-[:`origins:end`]->(o:`origins:Node`)
RETURN e, labels(e), o
'''  # noqa


def _prepare_attrs(attrs=None):
    if attrs is None:
        attrs = {}
    elif 'uuid' in attrs:
        raise KeyError('UUID cannot be provided')

    attrs['uuid'] = str(uuid4())
    attrs['time'] = utils.timestamp()

    return attrs


def _prepare_statement(statement, labels=None, **mapping):
    mapping['labels'] = labels_string(labels)

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
        data=parse_node(result[0]),
        perf=t.results,
        prov=None,
        time=utils.timestamp(),
    )


def get_by_id(_id, labels=None, tx=neo4j.tx):
    t = utils.Timer()

    if isinstance(_id, Node):
        _id = _id['id']
    elif isinstance(_id, Result):
        _id = _id['data']['id']

    with t('prep'):
        statement = _prepare_statement(GET_NODE_BY_ID,
                                       labels=labels)

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

        data = parse_node(result[0])

    return Result(
        data=data,
        prov=None,
        perf=t.results,
        time=utils.timestamp(),
    )


def add(attrs=None, new=True, labels=None, tx=neo4j.tx):
    t = utils.Timer()

    with tx as tx:
        with t('prep'):
            attrs = _prepare_attrs(attrs)

            if 'id' not in attrs:
                attrs['id'] = attrs['uuid']

            statement = _prepare_statement(ADD_NODE, labels=labels)

            parameters = {
                'attrs': pack(attrs),
            }

            query = {
                'statement': statement,
                'parameters': parameters,
            }

        with t('exec'):
            data = parse_node(tx.send(query)[0])

        # Provenance for add
        with t('prov'):
            prov_spec = provenance.add(data['uuid'], new=new)
            prov = provenance.load(prov_spec, tx=tx)

        return Result(
            perf=t.results,
            time=utils.timestamp(),
            data=data,
            prov=prov,
        )


def _update_edge(attrs, labels, start, end, tx):
    """Updates an edge with new start and end nodes.

    This creates a new revision of the edge.
    """
    t = utils.Timer()

    previous = attrs.pop('uuid')

    with t('add'):
        edge = edges.add(start, end, attrs=attrs, labels=labels, tx=tx)

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
    for (edge, edge_labels, end) in result:
        edge = unpack(edge)
        end = unpack(end)['uuid']
        result = _update_edge(edge, edge_labels, new, end, tx)
        results.append(result)

    return results


def set(uuid, attrs=None, new=True, labels=None, force=False, tx=neo4j.tx):
    t = utils.Timer()

    if isinstance(uuid, Node):
        uuid = uuid['uuid']
    elif isinstance(uuid, Result):
        uuid = uuid['data']['uuid']

    with tx as tx:
        with t('get'):
            prev = get(uuid, tx=tx)

        with t('prep'):
            # Merge the new into the old ones
            attrs = merge_attrs(prev['data'], attrs)

            # Calculate diff
            diff = utils.diff(attrs, prev['data'], ignore=DIFF_IGNORE)

            if not diff and not force:
                return

        with t('add'):
            # Create a new version of the entity
            rev = add(attrs, new=new, labels=labels, tx=tx)
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

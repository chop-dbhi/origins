from string import Template as T
from collections import defaultdict
from origins import provenance, events
from origins.exceptions import DoesNotExist, InvalidState, ValidationError
from ..model import Node, Edge
from ..packer import pack
from .. import neo4j, utils
from . import traverse


__all__ = (
    'match',
    'search',
    'get',
    'get_by_id',
    'add',
    'set',
    'remove',
)

# Do not shadow
pyset = set


EDGE_MODEL = 'origins:Edge'
EDGE_TYPE = 'Edge'


# Match edges with optional predicate
MATCH_EDGES = T('''
MATCH (n:`origins:Edge`$model$type $predicate)
WHERE NOT (n)<-[:`prov:entity`]-(:`prov:Invalidation`)
WITH n
MATCH (n)-[:`origins:start`]->(s:`origins:Node`),
      (n)-[:`origins:end`]->(e:`origins:Node`)
RETURN n, s, e
''')


# Search edges with predicate
SEARCH_EDGES = T('''
MATCH (n:`origins:Edge`$model$type)
WHERE NOT (n)<-[:`prov:entity`]-(:`prov:Invalidation`)
    AND $predicate
WITH n
MATCH (n)-[:`origins:start`]->(s:`origins:Node`),
      (n)-[:`origins:end`]->(e:`origins:Node`)
RETURN n, s, e
''')


# Returns a single edge with the start and end nodes by it's UUID
GET_EDGE = T('''
MATCH (n:`origins:Edge` {`origins:uuid`: { uuid }}),
      (n)-[:`origins:start`]->(s:`origins:Node`),
      (n)-[:`origins:end`]->(e:`origins:Node`)
RETURN n, s, e
LIMIT 1
''')

# Returns a single edge by UUID and includes the invalidation state
GET_EDGE_FOR_UPDATE = T('''
MATCH (n:`origins:Edge` {`origins:uuid`: { uuid }})
OPTIONAL MATCH (n)<-[:`prov:entity`]-(i:`prov:Invalidation`)
WITH n, i
MATCH (n)-[:`origins:start`]->(s:`origins:Node`),
      (n)-[:`origins:end`]->(e:`origins:Node`)
RETURN n, s, e, i
LIMIT 1
''')

# Creates and returns an edge
# The `prov:Entity` label is added to hook into the provenance data model
ADD_EDGE = T('''
MATCH (s:`origins:Node` {`origins:uuid`: { start }}),
      (e:`origins:Node` {`origins:uuid`: { end }})

CREATE (n$type$model:`origins:Edge`:`prov:Entity` { attrs }),
       (n)-[:`origins:start`]->(s),
       (n)-[:`origins:end`]->(e)

CREATE (s)-[r$type { props }]->(e)

RETURN n, s, e
''')


# Returns all edges and the *other* node of the edge
NODE_EDGES = '''
MATCH (s:`origins:Node` {`origins:uuid`: { uuid }})<-[:`origins:start`|`origins:end`]-(n:`origins:Edge`)
WHERE NOT (n)<-[:`prov:entity`]-(:`prov:Invalidation`)
WITH n, s

MATCH (n)-[r:`origins:end`|`origins:start`]->(o:`origins:Node`)
    WHERE o <> s

RETURN n, type(r), o
'''  # noqa


DEPENDENT_EDGES = '''
MATCH (n:`origins:Node` {`origins:uuid`: { uuid }}),
	(n)-[:`origins:start`|`origins:end`*]-(e:`origins:Edge`)
WHERE NOT (e)<-[:`prov:entity`]-(:`prov:Invalidation`)
WITH e AS n
MATCH (n)-[:`origins:start`]-(s:`origins:Node`),
	  (n)-[:`origins:end`]-(e:`origins:Node`)
RETURN n, s, e
'''  # noqa


# Removes the `$type` label for an edge. Type labels are only set while
# the node is valid.
REMOVE_EDGE_TYPE = T('''
MATCH (n:`origins:Edge` {`origins:uuid`: { uuid }})
REMOVE n$type
''')


DELETE_EDGE = T('''
MATCH (s:`origins:Node` {`origins:uuid`: { start }})-[r$type]->(e:`origins:Node` {`origins:uuid`: { end }})
DELETE r
''')  # noqa


def match(predicate=None, limit=None, skip=None, type=None, model=None,
          tx=neo4j.tx):

    query = traverse.match(MATCH_EDGES,
                           predicate=pack(predicate),
                           limit=limit,
                           skip=skip,
                           type=type,
                           model=model)

    result = tx.send(query)

    return [Edge.parse(*r) for r in result]


def search(predicate, operator=None, limit=None, skip=None, type=None,
           model=None, tx=neo4j.tx):

    query = traverse.search(SEARCH_EDGES,
                            predicate=pack(predicate),
                            operator=operator,
                            limit=limit,
                            skip=skip,
                            type=type,
                            model=model)

    result = tx.send(query)

    return [Edge.parse(*r) for r in result]


def get_by_id(id, type=None, model=None, tx=neo4j.tx):
    result = match({'id': id}, type=type, model=model, tx=tx)

    if not result:
        raise DoesNotExist('edge does not exist')

    return result[0]


def get(uuid, tx=neo4j.tx):
    statement = utils.prep(GET_EDGE)

    query = {
        'statement': statement,
        'parameters': {
            'uuid': uuid,
        }
    }

    result = tx.send(query)

    if not result:
        raise DoesNotExist('edge does not exist')

    return Edge.parse(*result[0])


def add(start, end, id=None, label=None, description=None, properties=None,
        direction=None, dependence=None, optimistic=None, type=None,
        model=None, tx=neo4j.tx):

    edge = Edge(start=start,
                end=end,
                id=id,
                label=label,
                description=description,
                properties=properties,
                type=type,
                model=model,
                direction=direction,
                dependence=dependence,
                optimistic=optimistic)

    with tx as tx:
        prov = _add(edge, tx)

        events.publish('edge.add', {
            'edge': edge.to_dict(),
            'prov': prov,
        })

        return edge


def set(uuid, label=None, description=None, properties=None,
        direction=None, dependence=None, optimistic=None, type=None,
        model=None, tx=neo4j.tx):

    with tx as tx:
        current, invalid = _get_for_update(uuid, tx=tx)

        if not current:
            raise DoesNotExist('edge does not exist')

        if invalid:
            raise InvalidState('cannot set invalid edge: {}'
                               .format(invalid['origins:reason']))

        # Create new edge merged into the previous attributes
        edge = Edge.derive(current, {
            'label': label,
            'description': description,
            'properties': properties,
            'type': type,
            'model': model,
            'direction': direction,
            'dependence': dependence,
            'optimistic': optimistic,
        })

        # Compare the new edge with the previous
        diff = current.diff(edge)

        if not diff:
            return

        prov = _add(edge, tx=tx)

        _dereference_edge(current, tx)

        # Provenance for change
        prov_spec = provenance.change(current.uuid, edge.uuid)
        prov_spec['wasGeneratedBy'] = prov['wasGeneratedBy']
        prov_spec['wasDerivedFrom']['wdf']['prov:generation'] = 'wgb'
        prov = provenance.load(prov_spec, tx=tx)

        events.publish('edge.change', {
            'node': edge.to_dict(),
            'prov': prov,
        })

        return edge


def remove(uuid, reason=None, tx=neo4j.tx):
    with tx as tx:
        edge, invalid = _get_for_update(uuid, tx=tx)

        if not edge:
            raise DoesNotExist('edge does not exist')

        if invalid:
            raise InvalidState('cannot set invalid edge: {}'
                               .format(invalid['origins:reason']))

        return _remove(edge, reason, tx)


def _get_for_update(uuid, tx):
    statement = utils.prep(GET_EDGE_FOR_UPDATE)

    query = {
        'statement': statement,
        'parameters': {
            'uuid': uuid,
        }
    }

    result = tx.send(query)

    if result:
        return Edge.parse(*result[0][:3]), result[0][3]

    return None, None


def _add(edge, tx):
    statement = utils.prep(ADD_EDGE, model=edge.model, type=edge.type)

    # Properties for the physical edge
    if edge.properties:
        props = edge.properties
    else:
        props = {}

    parameters = {
        'attrs': pack(edge.to_dict()),
        'start': edge.start.uuid,
        'end': edge.end.uuid,
        'props': props,
    }

    query = {
        'statement': statement,
        'parameters': parameters,
    }

    result = tx.send(query)

    if not result:
        raise ValidationError('start or end node does not exist')

    # Attach the start and end nodes in case the current
    # nodes are placeholders with only a UUID
    edge.start = Node.parse(result[0][1])
    edge.end = Node.parse(result[0][2])

    prov_spec = provenance.add(edge.uuid)
    return provenance.load(prov_spec, tx=tx)


def _remove(edge, reason, tx, trigger=None):
    with tx as tx:
        _dereference_edge(edge, tx)

        # TODO add trigger
        # Provenance for remove
        prov_spec = provenance.remove(edge.uuid, reason=reason)
        prov = provenance.load(prov_spec, tx=tx)

        events.publish('edge.remove', {
            'node': edge.to_dict(),
            'prov': prov,
        })

        return edge


def _dereference_edge(edge, tx):
    # Remove label from old node and physical edge between the two nodes
    queries = [{
        'statement': utils.prep(REMOVE_EDGE_TYPE, type=edge.type),
        'parameters': {'uuid': edge.uuid},
    }, {
        'statement': utils.prep(DELETE_EDGE, type=edge.type),
        'parameters': {
            'start': edge.start.uuid,
            'end': edge.end.uuid,
        },
    }]

    tx.send(queries)


def _update(current, start, end, tx):
    # Removes the physical edge between the two nodes
    _dereference_edge(current, tx)

    if current.start != start:
        trigger = start
    elif current.end != end:
        trigger = end
    else:
        trigger = None  # noqa

    edge = Edge.derive(current, {
        'start': start,
        'end': end,
    })

    prov = _add(edge, tx)

    # TODO add trigger
    prov_spec = provenance.change(current.uuid, edge.uuid,
                                  reason='origins:NodeChange')
    prov_spec['wasGeneratedBy'] = prov['wasGeneratedBy']
    prov_spec['wasDerivedFrom']['wdf']['prov:generation'] = 'wgb'
    prov = provenance.load(prov_spec, tx=tx)

    events.publish('edge.update', {
        'edge': edge.to_dict(),
        'prov': prov,
    })

    return edge


def _update_edges(old, new, tx):
    """Get all edges with a declared dependence for the `old` node
    and creates an edge to the `new` node.
    """
    query = {
        'statement': NODE_EDGES,
        'parameters': {
            'uuid': old.uuid,
        }
    }

    for e, t, o in tx.send(query):
        if t == 'origins:start':
            edge = Edge.parse(e, o, old)
        else:
            edge = Edge.parse(e, old, o)

        # Forward trigger
        if edge.start == old and edge.direction in ('directed', 'bidirected'):
            _update(edge, new, edge.end, tx=tx)

        # Reverse trigger
        elif edge.end == old and (edge.direction == 'bidirected' or edge.optimistic):  # noqa
            _update(edge, edge.start, new, tx=tx)

        # No updates, remove
        else:
            _remove(edge, reason='origins:NodeChange', trigger=new, tx=tx)


def _cascade_remove(node, tx):
    """Finds all edges and nodes that depend on the passed node for
    removal.
    """

    query = {
        'statement': DEPENDENT_EDGES,
        'parameters': {
            'uuid': node.uuid,
        }
    }

    # Map of nodes and edges to the triggers
    nodes = defaultdict(pyset)
    edges = defaultdict(pyset)

    # No triggers for initial node
    nodes[node] = pyset()

    for n, s, e in tx.send(query):
        edge = Edge.parse(n, s, e)

        # Node is being removed, remove the edge
        if edge.start in nodes:
            edges[edge].add(edge.start)

            # Mutual dependence or inverse will remove the end node
            if edge.dependence in ('mutual', 'inverse'):
                nodes[edge.end].add(edge.start)

        elif edge.end in nodes:
            edges[edge].add(edge.end)

            if edge.dependence in ('mutual', 'forward'):
                nodes[edge.start].add(edge.end)

    for edge, triggers in edges.items():
        _remove(edge, reason='origins:NodeRemoved', tx=tx)

    return nodes

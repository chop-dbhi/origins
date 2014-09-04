from string import Template as T
from origins import provenance, events
from origins.exceptions import DoesNotExist, InvalidState
from ..model import Node, Edge
from ..packer import pack
from .. import neo4j, utils, cypher


__all__ = (
    'match',
    'get',
    'get_by_id',
    'add',
    'set',
    'remove',
    'update',
    'outdated',
)


DEFAULT_TYPE = Edge.DEFAULT_TYPE


# Match edges with optional predicate
MATCH_EDGES = T('''
MATCH (n:`origins:Edge`$labels $predicate)

WITH n

MATCH (n)-[:`origins:start`]->(s:`origins:Node`),
      (n)-[:`origins:end`]->(e:`origins:Node`)

RETURN n, s, e
''')

# Returns a single edge with the start and end nodes by it's UUID
GET_EDGE = '''
MATCH (n:`origins:Edge` {`origins:uuid`: { uuid }}),
      (n)-[:`origins:start`]->(s:`origins:Node`),
      (n)-[:`origins:end`]->(e:`origins:Node`)
RETURN n, s, e
LIMIT 1
'''

# Returns a single edge by UUID and includes the invalidation state
GET_EDGE_FOR_UPDATE = '''
MATCH (n:`origins:Edge` {`origins:uuid`: { uuid }}),
      (n)-[:`origins:start`]->(s:`origins:Node`),
      (n)-[:`origins:end`]->(e:`origins:Node`)
OPTIONAL MATCH (n)<-[:`prov:entity`]-(i:`prov:Invalidation`)
RETURN n, s, e, i
LIMIT 1
'''

# Returns the latest edge by ID
GET_EDGE_BY_ID = T('''
MATCH (n:`origins:Edge`$labels {`origins:id`: { id }}),
      (n)-[:`origins:start`]->(s:`origins:Node`),
      (n)-[:`origins:end`]->(e:`origins:Node`)
RETURN n, s, e
LIMIT 1
''')


# Creates and returns an edge
# The `prov:Entity` label is added to hook into the provenance data model
ADD_EDGE = T('''
MATCH (s:`origins:Node` {`origins:uuid`: { start }}),
      (e:`origins:Node` {`origins:uuid`: { end }})

CREATE (n:`origins:Edge`:`prov:Entity`$labels { attrs }),
       (n)-[:`origins:start`]->(s),
       (n)-[:`origins:end`]->(e)

CREATE (s)-[r:`$etype` { props }]->(e)

RETURN n, s, e
''')


# Finds "all outdated edges"
# Matches all valid edges that have a invalid end node and finds the latest
# non-invalid revision of that node. The start node, edge and new end node
# is returned.
OUTDATED_EDGES = '''
// Valid edges
MATCH (s:`origins:Node`)<-[:`origins:start`]-(n:`origins:Edge`)
WHERE NOT (n)<-[:`prov:entity`]-(:`prov:Invalidation`)
    AND n.`origins:change_dependence` > 0
WITH n, s

// Pointing to an invalid end node
MATCH (n)<-[:`origins:end`]->(e:`origins:Node`)<-[:`prov:entity`]-(:`prov:Invalidation`)
WITH n, s, e

// That has a revision
MATCH (e)<-[:`prov:usedEntity`]-(:`prov:Derivation` {`prov:type`: 'prov:Revision'})-[:`prov:generatedEntity`|`prov:usedEntity`*]-(l:`origins:Node` {`origins:id`: e.`origins:id`})
WHERE NOT (l)<-[:`prov:entity`]-(:`prov:Invalidation`)

// Edge, start, end (current), latest
RETURN n, s, e, l
'''  # noqa


# Finds "outdated edges" for a node
# Matches all valid edges that have a invalid end node and finds the latest
# non-invalid revision of that node. The start node, edge and new end node
# is returned.
NODE_OUTDATED_EDGES = '''
MATCH (s:`origins:Node` {`origins:uuid`: { uuid }})<-[:`origins:start`]-(n:`origins:Edge`)
WHERE NOT (n)<-[:`prov:entity`]-(:`prov:Invalidation`)
    AND n.`origins:change_dependence` > 0
WITH n, s

MATCH (n)<-[:`origins:end`]->(e:`origins:Node`)<-[:`prov:entity`]-(:`prov:Invalidation`)
WITH n, s, e

MATCH (e)<-[:`prov:usedEntity`]-(:`prov:Derivation` {`prov:type`: 'prov:Revision'})-[:`prov:generatedEntity`|`prov:usedEntity`*]-(l:`origins:Node` {`origins:id`: e.`origins:id`})
WHERE NOT (l)<-[:`prov:entity`]-(:`prov:Invalidation`)

RETURN n, s, e, l
'''  # noqa


# Returns all edges of a node with dependence
NODE_EDGES = '''
MATCH (s:`origins:Node` {`origins:uuid`: { uuid }})<-[:`origins:start`]-(n:`origins:Edge`)
WHERE NOT (n)<-[:`prov:entity`]-(:`prov:Invalidation`)
    AND n.`origins:change_dependence` > 0
WITH n, s

MATCH (n)-[:`origins:end`]->(e:`origins:Node`)
RETURN n, s, e

UNION

MATCH (e:`origins:Node` {`origins:uuid`: { uuid }})<-[:`origins:end`]-(n:`origins:Edge`)
WHERE NOT (n)<-[:`prov:entity`]-(:`prov:Invalidation`)
    AND n.`origins:change_dependence` > 0

WITH n, e
MATCH (n)-[:`origins:start`]->(s:`origins:Node`)
RETURN n, s, e
'''  # noqa


UNSET_EDGE_LABEL = T('''
MATCH (n:`origins:Edge` {`origins:uuid`: { uuid }})
REMOVE n$labels
''')


DELETE_EDGE = T('''
MATCH (s:`origins:Node` {`origins:uuid`: { start }})-[r:`$etype`]->(e:`origins:Node` {`origins:uuid`: { end }})
DELETE r
''')  # noqa


def _deference_edge(edge, tx):
    # Remove label from old node and physical edge between the two nodes
    queries = [{
        'statement': utils.prep(UNSET_EDGE_LABEL, label=edge.type),
        'parameters': {'uuid': edge.uuid},
    }, {
        'statement': utils.prep(DELETE_EDGE, etype=edge.type),
        'parameters': {
            'start': edge.start.uuid,
            'end': edge.end.uuid,
        },
    }]

    tx.send(queries)


def match(predicate=None, type=DEFAULT_TYPE, limit=None, skip=None,
          tx=neo4j.tx):

    if predicate:
        placeholder = cypher.map(predicate.keys(), 'pred')
        parameters = {'pred': predicate}
    else:
        placeholder = ''
        parameters = {}

    statement = utils.prep(MATCH_EDGES, label=type, predicate=placeholder)

    if skip:
        statement += ' SKIP { skip }'
        parameters['skip'] = skip

    if limit:
        statement += ' LIMIT { limit }'
        parameters['limit'] = limit

    query = {
        'statement': statement,
        'parameters': parameters,
    }

    result = tx.send(query)

    return [Edge.parse(r) for r in result]


def _get_for_update(uuid, tx):
    query = {
        'statement': GET_EDGE_FOR_UPDATE,
        'parameters': {
            'uuid': uuid,
        }
    }

    result = tx.send(query)

    if result:
        return Edge.parse(result[0][:3]), result[0][3]

    return None, None


def get(uuid, tx=neo4j.tx):
    query = {
        'statement': GET_EDGE,
        'parameters': {
            'uuid': uuid,
        }
    }

    result = tx.send(query)

    if not result:
        raise DoesNotExist('edge does not exist')

    return Edge.parse(result[0])


def get_by_id(id, type=DEFAULT_TYPE, tx=neo4j.tx):
    statement = utils.prep(GET_EDGE_BY_ID, label=type)

    query = {
        'statement': statement,
        'parameters': {
            'id': id,
        }
    }

    result = tx.send(query)

    if not result:
        raise DoesNotExist('edge does not exist')

    return Edge.parse(result[0])


def _add(edge, tx):
    statement = utils.prep(ADD_EDGE, etype=edge.type, label=edge.type)

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
        raise ValueError('start or end node does not exist')

    # Attach the start and end nodes
    edge.start = Node.parse(result[0][1])
    edge.end = Node.parse(result[0][2])

    prov_spec = provenance.add(edge.uuid)
    return provenance.load(prov_spec, tx=tx)


def add(start, end, id=None, type=DEFAULT_TYPE, label=None, description=None,
        change_dependence=None, remove_dependence=None, properties=None,
        tx=neo4j.tx):

    if not isinstance(start, Node):
        start = Node(uuid=start)

    if not isinstance(end, Node):
        end = Node(uuid=end)

    with tx as tx:
        edge = Edge(id=id,
                    type=type,
                    label=label,
                    start=start,
                    end=end,
                    description=description,
                    change_dependence=change_dependence,
                    remove_dependence=remove_dependence,
                    properties=properties)

        prov = _add(edge, tx)

        events.publish('edge.add', {
            'edge': edge.to_dict(),
            'prov': prov,
        })

        return edge


def set(uuid, label=None, description=None, properties=None, type=DEFAULT_TYPE,
        change_dependence=None, remove_dependence=None, tx=neo4j.tx):

    with tx as tx:
        current, invalid = _get_for_update(uuid, tx=tx)

        if invalid:
            raise InvalidState('cannot set invalid edge: {}'
                               .format(invalid['origins:reason']))

        # Create new edge merged into the previous attributes
        edge = Edge.derive(current, {
            'label': label,
            'description': description,
            'properties': properties,
            'type': type,
            'change_dependence': change_dependence,
            'remove_dependence': remove_dependence,
        })

        # Compare the new edge with the previous
        diff = current.diff(edge)

        if not diff:
            return

        _deference_edge(current, tx)

        prov = _add(edge, tx=tx)

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

        # Already removed, just ignore
        if not edge:
            return

        _deference_edge(edge, tx)

        # Provenance for remove
        prov_spec = provenance.remove(edge.uuid, reason=reason)
        prov = provenance.load(prov_spec, tx=tx)

        events.publish('edge.remove', {
            'node': edge.to_dict(),
            'prov': prov,
        })

        return edge


def _update(current, start, end, tx):
    # Removes the physical edge between the two nodes
    _deference_edge(current, tx)

    edge = Edge.derive(current, {
        'start': start,
        'end': end,
    })

    prov = _add(edge, tx)

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


def _set_dependents(old, new, tx):
    """Get all edges with a declared dependence for the `old` node
    and creates an edge to the `new` node.
    """
    query = {
        'statement': NODE_EDGES,
        'parameters': {
            'uuid': old.uuid,
        }
    }

    results = []

    for row in tx.send(query):
        edge = Edge.parse(row)

        # Mutual and directed dependence apply to start nodes
        if edge.start == old:
            r = _update(edge, new, edge.end, tx=tx)
        elif edge.change_dependence == Edge.MUTUAL_DEPENDENCE:
            r = _update(edge, edge.start, new, tx=tx)
        else:
            # TODO notify of dependency update
            continue

        results.append(r)

    return results


def outdated(uuid=None, tx=neo4j.tx):
    """Returns a list of all outdated nodes or for a specific node. The format
    is the list of pairs containing the edge and the latest dependency.
    """
    if uuid:
        query = {
            'statement': NODE_OUTDATED_EDGES,
            'parameters': {
                'uuid': uuid,
            }
        }
    else:
        query = {
            'statement': OUTDATED_EDGES,
        }

    results = []

    for r in tx.send(query):
        results.append((Edge.parse(r[:3]), Node.parse(r[-1])))

    return results


def update(uuid=None, tx=neo4j.tx):
    """Updates all outdated edges or edges for a specific node to the latest
    revision of the dependency.
    """
    with tx as tx:
        results = []

        for edge, latest in outdated(uuid, tx=tx):
            r = _update(edge, edge.start, latest, tx=tx)
            results.append(r)

        return results

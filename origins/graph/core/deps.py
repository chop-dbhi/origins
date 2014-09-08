from collections import defaultdict
from origins import provenance, events
from ..model import Edge
from . import edges


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


def _update(current, start, end, tx):
    # Removes the physical edge between the two nodes
    edges._dereference_edge(current, tx)

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

    prov = edges._add(edge, tx)

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


def update_edges(old, new, tx):
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
            edges._remove(edge, reason='origins:NodeChange', trigger=new,
                          tx=tx)


def cascade_remove(node, tx):
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
    _nodes = defaultdict(set)
    _edges = defaultdict(set)

    # No triggers for initial node
    _nodes[node] = set()

    for n, s, e in tx.send(query):
        edge = Edge.parse(n, s, e)

        # Node is being removed, remove the edge
        if edge.start in _nodes:
            _edges[edge].add(edge.start)

            # Mutual dependence or inverse will remove the end node
            if edge.dependence in ('mutual', 'inverse'):
                _nodes[edge.end].add(edge.start)

        elif edge.end in _nodes:
            _edges[edge].add(edge.end)

            if edge.dependence in ('mutual', 'forward'):
                _nodes[edge.start].add(edge.end)

    for edge, triggers in _edges.items():
        edges._remove(edge, reason='origins:NodeRemoved', tx=tx)

    return _nodes

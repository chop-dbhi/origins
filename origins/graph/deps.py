from collections import defaultdict
from origins import provenance, events
from . import utils


# Returns all edges and the *other* node of the edge
NODE_EDGES = '''
MATCH (s$model {`origins:uuid`: { uuid }})<-[:`origins:start`|`origins:end`]-(n:`origins:Edge`)

WHERE NOT (n)<-[:`prov:entity`]-(:`prov:Invalidation`)

WITH n, s

MATCH (n)-[r:`origins:end`|`origins:start`]->(o)
    WHERE o <> s

RETURN n, type(r), o
'''  # noqa


DEPENDENT_EDGES = '''
MATCH (n$model {`origins:uuid`: { uuid }}),
	(n)-[:`origins:start`|`origins:end`*]-(e:`origins:Edge`)

WHERE NOT (e)<-[:`prov:entity`]-(:`prov:Invalidation`)

WITH e AS n

MATCH (n)-[:`origins:start`]->(s),
	  (n)-[:`origins:end`]->(e)

RETURN n, s, e
'''  # noqa


def _update(current, start, end, tx):
    # Removes the physical edge between the two nodes
    current._invalidate(current, tx)

    if current.start != start:
        trigger = start
    elif current.end != end:
        trigger = end
    else:
        trigger = None  # noqa

    edge = current.derive(current, {
        'start': start,
        'end': end,
    })

    prov = current._add(edge, tx)

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


def trigger_change(old, new, tx):
    """Get all edges with a declared dependence for the `old` node
    and creates an edge to the `new` node.
    """
    from . import models

    statement = utils.prep(NODE_EDGES, model=old.model_name)

    query = {
        'statement': statement,
        'parameters': {
            'uuid': old.uuid,
        }
    }

    for e, t, o in tx.send(query):
        # Get the corresponding model based on the attribute
        model = models[e['origins:model']]

        if t == 'origins:start':
            start = models[o['origins:model']].parse(o)
            end = old
        else:
            start = old
            end = models[o['origins:model']].parse(o)

        edge = model.parse(e, start=start, end=end)

        # Forward trigger
        if edge.start == old and edge.direction in {'bidirected', 'directed'}:
            _update(edge, new, edge.end, tx=tx)

        # Reverse trigger
        elif edge.end == old and edge.direction in {'bidirected', 'reverse'}:  # noqa
            _update(edge, edge.start, new, tx=tx)

        # No updates, remove
        else:
            model._remove(edge, reason='origins:NodeChange', trigger=new,
                          tx=tx)


def trigger_remove(node, tx):
    """Finds all edges and nodes that depend on the passed node for
    removal.
    """
    from . import models

    statement = utils.prep(DEPENDENT_EDGES, model=node.model_name)

    query = {
        'statement': statement,
        'parameters': {
            'uuid': node.uuid,
        }
    }

    # Map of nodes and edges to the triggers
    nodes = defaultdict(set)
    edges = defaultdict(set)

    # No triggers for initial node
    nodes[node] = set()

    for n, s, e in tx.send(query):
        start = models[s['origins:model']].parse(s)
        end = models[e['origins:model']].parse(e)

        model = models[n['origins:model']]
        edge = model.parse(n, start=start, end=end)

        # Node is being removed, remove the edge
        if start in nodes:
            edges[edge].add(start)

            # Mutual dependence or inverse will remove the end node
            if edge.dependence in {'mutual', 'inverse'}:
                nodes[end].add(start)

        elif end in nodes:
            edges[edge].add(end)

            if edge.dependence in {'mutual', 'forward'}:
                nodes[start].add(end)

    for edge, triggers in edges.items():
        model._remove(edge, reason='origins:NodeRemoved', tx=tx)

    return nodes

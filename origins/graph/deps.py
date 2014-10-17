from collections import defaultdict
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


# Finds all dependent nodes. Returns the dependent node along with
# the trigger. Dependent nodes should be grouped to collect all
# the possible triggers.
DEPENDENT_NODES = '''
MATCH ($model {`origins:uuid`: { uuid }})<-[_r:`origins:dependency`*]-(d)
WHERE NOT (d)<-[:`prov:entity`]-(:`prov:Invalidation`)

WITH _r UNWIND _r as r

RETURN startNode(r), endNode(r)
'''  # noqa


# Finds all edges to dependent nodes. The edges of the trigger node is
# unioned with all dependent nodes' edges. Returns the node and the edg .
DEPENDENT_EDGES = '''
MATCH (d$model {`origins:uuid`: { uuid }})<-[:`origins:start`|`origins:end`]-(e:`origins:Edge`)
WHERE NOT (e)<-[:`prov:entity`]-(:`prov:Invalidation`)

RETURN e, d

UNION

MATCH ($model {`origins:uuid`: { uuid }})<-[:`origins:dependency`*]-(d)
WHERE NOT (d)<-[:`prov:entity`]-(:`prov:Invalidation`)

WITH d

MATCH (d)<-[:`origins:start`|`origins:end`]-(e:`origins:Edge`)
WHERE NOT (e)<-[:`prov:entity`]-(:`prov:Invalidation`)

RETURN e, d
'''  # noqa


def trigger_change(old, new, validate, tx):
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
            edge.set(edge, start=new, end=edge.end, validate=validate, tx=tx)

        # Reverse trigger
        elif edge.end == old and edge.direction in {'bidirected', 'reverse'}:  # noqa
            edge.set(edge, start=edge.start, end=new, validate=validate, tx=tx)

        # No updates, remove
        else:
            edge._remove(edge, reason='origins:NodeChange', trigger=new,
                         validate=validate, tx=tx)


def trigger_remove(node, validate, tx):
    """Finds all edges and nodes that depend on the passed node for
    removal.
    """
    from . import models

    statement = utils.prep(DEPENDENT_NODES, model=node.model_name)

    query = {
        'statement': statement,
        'parameters': {
            'uuid': node.uuid,
        }
    }

    # Map of nodes and edges to the triggers
    nodes = defaultdict(set)

    # No triggers for initial node
    nodes[node] = set()

    # Dependent node and dependency trigger
    for d, t in tx.send(query):
        d = models[d['origins:model']].parse(d)
        t = models[t['origins:model']].parse(t)

        nodes[d].add(t)

    statement = utils.prep(DEPENDENT_EDGES, model=node.model_name)

    query = {
        'statement': statement,
        'parameters': {
            'uuid': node.uuid,
        }
    }

    edges = defaultdict(set)

    # Dependent edge and node trigger
    for e, t in tx.send(query):
        e = models[e['origins:model']].parse(e)
        t = models[t['origins:model']].parse(t)

        edges[e].add(t)

    for edge, triggers in edges.items():
        edge._remove(edge, reason='origins:NodeRemoved', validate=validate,
                     tx=tx)

    return dict(nodes)

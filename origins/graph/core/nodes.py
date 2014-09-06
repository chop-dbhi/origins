from string import Template as T
from origins import provenance, events
from origins.exceptions import DoesNotExist, InvalidState
from ..model import Node
from ..packer import pack
from .. import neo4j, utils
from .edges import _cascade_remove, _update_edges
from . import traverse


__all__ = (
    'match',
    'get',
    'get_by_id',
    'add',
    'set',
    'remove',
)


NODE_MODEL = 'origins:Node'
NODE_TYPE = 'Node'


# Match nodes with optional predicate.
# The model should be supplied
# TODO is this sane?
MATCH_NODES = T('''
MATCH (n$model$type $predicate)
WHERE NOT (n)<-[:`prov:entity`]-(:`prov:Invalidation`)
RETURN n
''')


# Returns single node by UUID. The model nor type is applied here since
# the lookup is by UUID
GET_NODE = T('''
MATCH (n:`origins:Node` {`origins:uuid`: { uuid }})
RETURN n
LIMIT 1
''')


# Same as GET_NODE but also returns the current state of the
# node conditional processing.
GET_NODE_FOR_UPDATE = T('''
MATCH (n:`origins:Node` {`origins:uuid`: { uuid }})
OPTIONAL MATCH (n)<-[:`prov:entity`]-(i:`prov:Invalidation`)
RETURN n, i
LIMIT 1
''')


# Creates a node
# The `prov:Entity` and `origins:Node` labels are fixed.
ADD_NODE = T('''
CREATE (n$model$type:`prov:Entity`:`origins:Node` { attrs })
''')


# Removes the `$type` label for a node. Type labels are only set while
# the node is valid.
REMOVE_NODE_TYPE = T('''
MATCH (n:`origins:Node` {`origins:uuid`: { uuid }})
REMOVE n$type
''')


def match(predicate=None, limit=None, skip=None, type=None, model=None,
          tx=neo4j.tx):

    if not model:
        model = NODE_MODEL

    query = traverse.match(MATCH_NODES,
                           predicate=pack(predicate),
                           limit=limit,
                           skip=skip,
                           type=type,
                           model=model)

    result = tx.send(query)

    return [Node.parse(r) for r in result]


def get_by_id(id, type=None, model=None, tx=neo4j.tx):
    result = match({'id': id}, limit=1, model=model, type=type, tx=tx)

    if not result:
        raise DoesNotExist('node does not exist')

    return result[0]


def get(uuid, tx=neo4j.tx):
    statement = utils.prep(GET_NODE)

    query = {
        'statement': statement,
        'parameters': {
            'uuid': uuid,
        }
    }

    result = tx.send(query)

    if not result:
        raise DoesNotExist('node does not exist')

    # Only pass data, invalidation is not relevant here
    return Node.parse([result[0][0]])


def add(id=None, label=None, description=None, properties=None,
        type=None, model=None, tx=neo4j.tx):

    node = Node(id=id,
                type=type,
                model=model,
                label=label,
                description=description,
                properties=properties)

    with tx as tx:
        prov = _add(node, tx)

        events.publish('node.add', {
            'node': node.to_dict(),
            'prov': prov,
        })

    return node


def set(uuid, label=None, description=None, properties=None, type=None,
        model=None, tx=neo4j.tx):

    with tx as tx:
        current, invalid = _get_for_update(uuid, tx=tx)

        if not current:
            raise DoesNotExist('node does not exist')

        if invalid:
            raise InvalidState('cannot set invalid node: {}'
                               .format(invalid['origins:reason']))

        # Derive from current node
        node = Node.derive(current, {
            'label': label,
            'description': description,
            'properties': properties,
            'type': type,
            'model': model,
        })

        # Compare the new node with the previous
        diff = current.diff(node)

        if not diff:
            return

        # Create a new version of the entity
        prov = _add(node, tx=tx)

        _dereference_node(current, tx)

        _update_edges(current, node, tx)

        # Provenance for change
        prov_spec = provenance.change(current.uuid, node.uuid)
        prov_spec['wasGeneratedBy'] = prov['wasGeneratedBy']
        prov_spec['wasDerivedFrom']['wdf']['prov:generation'] = 'wgb'
        prov = provenance.load(prov_spec, tx=tx)

        events.publish('node.change', {
            'node': node.to_dict(),
            'prov': prov,
        })

        return node


def remove(uuid, reason=None, tx=neo4j.tx):
    with tx as tx:
        node, invalid = _get_for_update(uuid, tx=tx)

        if not node:
            raise DoesNotExist('node does not exist')

        if invalid:
            raise InvalidState('cannot remove and invalid node')

        nodes = _cascade_remove(node, tx=tx)

        # Remove all nodes
        for n, triggers in nodes.items():
            _remove(n, reason='origins:DependentNodeRemoved', tx=tx)

        return node


def _get_for_update(uuid, tx):
    statement = utils.prep(GET_NODE_FOR_UPDATE)

    query = {
        'statement': statement,
        'parameters': {
            'uuid': uuid,
        }
    }

    result = tx.send(query)

    if result:
        return Node.parse(result[0][0]), result[0][1]

    return None, None


def _add(node, tx):
    statement = utils.prep(ADD_NODE, model=node.model, type=node.type)

    parameters = {
        'attrs': pack(node.to_dict()),
    }

    query = {
        'statement': statement,
        'parameters': parameters,
    }

    tx.send(query)

    prov_spec = provenance.add(node.uuid)
    return provenance.load(prov_spec, tx=tx)


def _remove(node, reason, tx, trigger=None):
    with tx as tx:
        _dereference_node(node, tx)

        # TODO do something with triggers
        # Provenance for remove
        prov_spec = provenance.remove(node.uuid, reason=reason)
        prov = provenance.load(prov_spec, tx=tx)

        events.publish('node.remove', {
            'node': node.to_dict(),
            'prov': prov,
        })


def _dereference_node(current, tx):
    "Update references and migrates edges from old to new revision."
    # Remove label from old node
    statement = utils.prep(REMOVE_NODE_TYPE, type=current.type)

    tx.send({
        'statement': statement,
        'parameters': {
            'uuid': current.uuid
        },
    })

from string import Template as T
from origins import provenance, events
from origins.exceptions import DoesNotExist, InvalidState
from ..model import Node
from ..packer import pack
from .. import neo4j, utils, cypher
from .edges import _set_dependents


__all__ = (
    'match',
    'get',
    'get_by_id',
    'add',
    'set',
    'remove',
)


DEFAULT_TYPE = Node.DEFAULT_TYPE


# Match nodes with zero or more labels and an optional predicate.
MATCH_NODES = T('''
MATCH (n:`origins:Node`$labels $predicate)
RETURN n
''')


# Returns single node by UUID
GET_NODE = '''
MATCH (n:`origins:Node` {`origins:uuid`: { uuid }})
RETURN n
LIMIT 1
'''

# Returns single node by UUID and includes the validation
# state.
GET_NODE_FOR_UPDATE = '''
MATCH (n:`origins:Node` {`origins:uuid`: { uuid }})
OPTIONAL MATCH (n)<-[:`prov:entity`]-(i:`prov:Invalidation`)
RETURN n, i
LIMIT 1
'''

# Returns the latest node by ID. This uses labels to constrain
# the lookup.
GET_NODE_BY_ID = T('''
MATCH (n:`origins:Node`$labels {`origins:id`: { id }})
RETURN n
LIMIT 1
''')


# Creates and returns a node
# The `prov:Entity` type is added to hook into the provenance data model
ADD_NODE = T('''
CREATE (n:`origins:Node`:`prov:Entity`$labels { attrs })
''')


UNSET_NODE_LABEL = T('''
MATCH (n:`origins:Node` {`origins:uuid`: { uuid }})
REMOVE n$labels
''')


def _set_references(old, new, tx):
    "Update references and migrates edges from old to new revision."
    # Remove label from old node
    tx.send(utils.prep(UNSET_NODE_LABEL, label=old.type), {
        'uuid': old.uuid
    })

    # Update edges
    _set_dependents(old, new, tx=tx)


def match(predicate=None, type=DEFAULT_TYPE, limit=None, skip=None,
          tx=neo4j.tx):

    if predicate:
        placeholder = cypher.map(predicate.keys(), 'pred')
        parameters = {'pred': predicate}
    else:
        placeholder = ''
        parameters = {}

    statement = utils.prep(MATCH_NODES, label=type, predicate=placeholder)

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

    return [Node.parse(r) for r in result]


def _get_for_update(uuid, tx):
    query = {
        'statement': GET_NODE_FOR_UPDATE,
        'parameters': {
            'uuid': uuid,
        }
    }

    result = tx.send(query)

    if result:
        return Node.parse(result[0][0]), result[0][1]

    return None, None


def get(uuid, tx=neo4j.tx):
    query = {
        'statement': GET_NODE,
        'parameters': {
            'uuid': uuid,
        }
    }

    result = tx.send(query)

    if not result:
        raise DoesNotExist('node does not exist')

    # Only pass data, invalidation is not relevant here
    return Node.parse([result[0][0]])


def get_by_id(id, type=DEFAULT_TYPE, tx=neo4j.tx):
    statement = utils.prep(GET_NODE_BY_ID, label=type)

    query = {
        'statement': statement,
        'parameters': {
            'id': id,
        }
    }

    result = tx.send(query)

    if not result:
        raise DoesNotExist('node does not exist')

    return Node.parse(result[0][0])


def _add(node, tx):
    statement = utils.prep(ADD_NODE, label=node.type)

    parameters = {
        'attrs': pack(node.to_dict()),
    }

    query = {
        'statement': statement,
        'parameters': parameters,
    }

    tx.send(query)

    # Provenance for add
    prov_spec = provenance.add(node.uuid)
    return provenance.load(prov_spec, tx=tx)


def add(id=None, label=None, description=None, properties=None,
        type=DEFAULT_TYPE, tx=neo4j.tx):

    node = Node(id=id,
                type=type,
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


def set(uuid, label=None, description=None, properties=None, type=DEFAULT_TYPE,
        tx=neo4j.tx):

    with tx as tx:
        current, invalid = _get_for_update(uuid, tx=tx)

        if invalid:
            raise InvalidState('cannot set invalid node: {}'
                               .format(invalid['origins:reason']))

        # Derive from current node
        node = Node.derive(current, {
            'label': label,
            'description': description,
            'properties': properties,
            'type': type,
        })

        # Compare the new node with the previous
        diff = current.diff(node)

        if not diff:
            return

        # Create a new version of the entity
        prov = _add(node, tx=tx)

        _set_references(current, node, tx)

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

        # Already removed, just ignore
        if not node:
            return

        _set_references(node, None, tx)

        # Provenance for remove
        prov_spec = provenance.remove(node.uuid, reason=reason)
        prov = provenance.load(prov_spec, tx=tx)

        events.publish('node.remove', {
            'node': node.to_dict(),
            'prov': prov,
        })

        return node

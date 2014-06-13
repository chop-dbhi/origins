from __future__ import unicode_literals, absolute_import

from uuid import uuid4
from . import neo4j, queries, utils

try:
    str = unicode
except NameError:
    pass


_constraints_created = False


def ensure_constraints(tx=neo4j):
    global _constraints_created

    if _constraints_created:
        return

    _constraints_created = True

    neo4j.send([
        queries.RESOURCE_ID_UNIQUE_CONSTRAINT,
        queries.RESOURCE_UUID_INDEX,
        queries.COMPONENT_UUID_INDEX,
    ])


def byid(_id, tx=neo4j):
    query = {
        'statement': queries.RESOURCE_BY_ID,
        'parameters': {
            'resource': {
                'origins:id': _id,
            }
        }
    }

    result = tx.send(query)

    if result:
        return utils.unpack(result[0])


def get(uuid=None, skip=None, limit=None, tx=neo4j):
    "Return all resources or a single one by UUID."
    ensure_constraints()

    if uuid:
        query = {
            'statement': queries.RESOURCE,
            'parameters': {
                'resource': {
                    'origins:uuid': uuid,
                }
            }
        }

        result = tx.send(query)

        if result:
            return utils.unpack(result[0])

    else:
        statement = queries.RESOURCES

        if skip:
            statement += ' SKIP ' + str(skip)

        if limit:
            statement += ' LIMIT ' + str(limit)

        query = {
            'statement': statement,
        }

        return [utils.unpack(r) for r in tx.send(query)]


def create(_id, properties=None, tx=neo4j):
    "Creates a bare resource."
    ensure_constraints()

    if isinstance(_id, dict):
        properties = _id
    elif properties is None:
        properties = {}
        properties['origins:id'] = _id

    properties['origins:uuid'] = str(uuid4())

    query = {
        'statement': queries.CREATE_RESOURCE,
        'parameters': {
            'properties': utils.pack(properties),
        }
    }

    return utils.unpack(tx.send(query)[0])


def delete(uuid, tx=neo4j):
    "Deletes a resources."
    ensure_constraints()

    query = {
        'statement': queries.DELETE_RESOURCE,
        'parameters': {
            'resource': {
                'origins:uuid': uuid,
            }
        }
    }

    result = counts(uuid, tx)

    tx.send(query)

    return result


def counts(uuid, tx=neo4j):
    "Return the count of components and inter-relationships."
    ensure_constraints()

    query = {
        'statement': queries.RESOURCE_COUNTS,
        'parameters': {
            'resource': {
                'origins:uuid': uuid,
            }
        }
    }

    result = tx.send(query)[0]

    return {
        'components': result[0],
        'relationships': result[1],
    }


def components(uuid, managed=False, skip=None, limit=None, tx=neo4j):
    "Gets components associated with this resource."
    if managed:
        statement = queries.RESOURCE_MANAGED_COMPONENTS
    else:
        statement = queries.RESOURCE_COMPONENTS

    if skip:
        statement += ' SKIP ' + str(skip)

    if limit:
        statement += ' LIMIT ' + str(limit)

    query = {
        'statement': statement,
        'parameters': {
            'resource': {
                'origins:uuid': uuid,
            }
        }
    }

    return [utils.unpack(r) for r in tx.send(query)]


def relationships(uuid, managed=False, skip=None, limit=None, tx=neo4j):
    "Gest relationships between components associated with this resource."
    if managed:
        statement = queries.RESOURCE_MANAGED_RELS
    else:
        statement = queries.RESOURCE_RELS

    if skip:
        statement += ' SKIP ' + str(skip)

    if limit:
        statement += ' LIMIT ' + str(limit)

    query = {
        'statement': statement,
        'parameters': {
            'resource': {
                'origins:uuid': uuid,
            }
        }
    }

    return [utils.unpack(r) for r in tx.send(query)]

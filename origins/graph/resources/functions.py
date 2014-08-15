from ..parsers import parse_resource, parse_component, parse_relationship, \
    parse_timeline
from .. import neo4j, utils, base
from . import queries


def match(predicate=None, skip=None, limit=None, tx=neo4j):
    query = base.match(queries.MATCH_RESOURCES, predicate, skip, limit)

    return [parse_resource(r) for r in tx.send(query)]


def search(predicate, skip=None, limit=None, tx=neo4j):
    query = base.search(queries.SEARCH_RESOURCES, 'res', predicate,
                        skip=skip, limit=limit)

    return [parse_resource(r) for r in tx.send(query)]


def count(tx=neo4j):
    query = {
        'statement': queries.RESOURCE_COUNT,
    }

    result = tx.send(query)

    if result:
        return result[0][0]


def get(predicate, tx=neo4j):
    "Gets a single resource given the predicate."
    query = base.get(queries.GET_RESOURCE, predicate)

    result = tx.send(query)

    if result:
        return parse_resource(result[0])


def create(_id, properties=None, tx=neo4j):
    "Creates a bare resource."
    query = base.create(queries.CREATE_RESOURCE, _id, properties)

    result = tx.send(query)

    if result:
        return parse_resource(result[0])


def update(_id, properties, tx=neo4j):
    "Updates a resource by UUID."
    if not properties:
        return

    query = base.update(queries.UPDATE_RESOURCE, 'res', _id, properties)

    result = tx.send(query)

    if result:
        return parse_resource(result[0])


def delete(_id, tx=neo4j):
    "Deletes a resource."
    query = base.delete(queries.DELETE_RESOURCE, _id)

    tx.send(query)


def components(_id, managed=False, skip=None, limit=None,
               tx=neo4j):
    "Gets components associated with this resource."

    if managed:
        statement = queries.RESOURCE_MANAGED_COMPONENTS
    else:
        statement = queries.RESOURCE_COMPONENTS

    parameters = {
        'id': utils._id(_id),
    }

    statement, parameters = base.skip_limit(statement, parameters, skip, limit)

    query = {
        'statement': statement,
        'parameters': parameters,
    }

    return [parse_component(r) for r in tx.send(query)]


def relationships(_id, managed=False, skip=None, limit=None,
                  tx=neo4j):
    "Gets components associated with this resource."

    if managed:
        statement = queries.RESOURCE_MANAGED_RELATIONSHIPS
    else:
        statement = queries.RESOURCE_RELATIONSHIPS

    parameters = {
        'id': utils._id(_id),
    }

    statement, parameters = base.skip_limit(statement, parameters, skip, limit)

    query = {
        'statement': statement,
        'parameters': parameters,
    }

    return [parse_relationship(r) for r in tx.send(query)]


def component_count(_id, tx=neo4j):
    "Returns the count of components included in this resource."
    query = {
        'statement': queries.RESOURCE_COMPONENT_COUNT,
        'parameters': {
            'id': utils._id(_id),
        }
    }

    result = tx.send(query)

    if result:
        return result[0][0]


def component_types(_id, tx=neo4j):
    """Returns the distinct type and count of component types included in
    this resource.
    """
    query = {
        'statement': queries.RESOURCE_COMPONENT_TYPES,
        'parameters': {
            'id': utils._id(_id),
        }
    }

    return [{'type': r[0], 'count': r[1]} for r in tx.send(query)]


def relationship_count(_id, tx=neo4j):
    "Returns the number of relationships this resource includes."
    query = {
        'statement': queries.RESOURCE_RELATIONSHIP_COUNT,
        'parameters': {
            'id': utils._id(_id),
        }
    }

    result = tx.send(query)

    if result:
        return result[0][0]


def collection_count(_id, tx=neo4j):
    "Returns the number of collections this resource is contained in."
    query = {
        'statement': queries.RESOURCE_COLLECTION_COUNT,
        'parameters': {
            'id': utils._id(_id),
        }
    }

    result = tx.send(query)

    if result:
        return result[0][0]


def collections(_id, tx=neo4j):
    "Returns all collections this resource is contained in."
    query = {
        'statement': queries.RESOURCE_COLLECTIONS,
        'parameters': {
            'id': utils._id(_id),
        }
    }

    result = tx.send(query)

    if result:
        return result[0][0]


def include_component(_id, revision, tx=neo4j):
    "Includes a component to the resource. The revision must be a UUID."
    query = {
        'statement': queries.RESOURCE_INCLUDE_COMPONENT,
        'parameters': {
            'id': utils._id(_id),
            'revision': utils._uuid(revision),
        }
    }

    result = tx.send(query)

    if result:
        return result[0][0]


def include_relationship(_id, revision, tx=neo4j):
    "Includes a relationship to the resource. The revision must be a UUID."
    query = {
        'statement': queries.RESOURCE_INCLUDE_RELATIONSHIP,
        'parameters': {
            'id': utils._id(_id),
            'revision': utils._uuid(revision),
        }
    }

    result = tx.send(query)

    if result:
        return result[0][0]


def timeline(_id, skip=None, limit=None, tx=neo4j):
    "Gets the timeline for this resource."
    statement = queries.RESOURCE_TIMELINE

    parameters = {
        'id': utils._id(_id),
    }

    statement, parameters = base.skip_limit(statement, parameters, skip, limit)

    query = {
        'statement': statement,
        'parameters': parameters,
    }

    return parse_timeline(tx.send(query))

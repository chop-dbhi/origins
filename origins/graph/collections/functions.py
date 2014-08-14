from __future__ import unicode_literals, absolute_import

from ..parsers import parse_collection, parse_resource
from .. import neo4j, utils, base
from . import queries

try:
    str = unicode
except NameError:
    pass


def match(predicate=None, skip=None, limit=None, tx=neo4j):
    "Returns collections matching the predicate."
    query = base.match(queries.MATCH_COLLECTIONS, predicate, skip, limit)

    return [parse_collection(r) for r in tx.send(query)]


def search(predicate=None, operator='OR', skip=None, limit=None, tx=neo4j):
    "Returns collections matching the search predicate."
    query = base.search(queries.SEARCH_COLLECTIONS, predicate, skip, limit)

    return [parse_collection(r) for r in tx.send(query)]


def get(predicate, tx=neo4j):
    "Gets a single collection given the predicate."
    query = base.get(queries.GET_COLLECTION, predicate)

    result = tx.send(query)

    if result:
        return parse_collection(result[0])


def create(_id, properties=None, tx=neo4j):
    "Creates a collection."
    query = base.create(queries.CREATE_COLLECTION, _id, properties)

    result = tx.send(query)

    if result:
        return parse_collection(result[0])


def update(_id, properties, tx=neo4j):
    "Updates a collection."
    if not properties:
        return

    query = base.update(queries.UPDATE_COLLECTION, 'col', _id, properties)

    result = tx.send(query)

    if result:
        return parse_collection(result[0])


def delete(_id, tx=neo4j):
    "Deletes a collection."
    query = base.delete(queries.DELETE_COLLECTION, _id)

    tx.send(query)


def add(_id, resource, tx=neo4j):
    "Adds a resource to the collection."
    query = {
        'statement': queries.COLLECTION_ADD_RESOURCE,
        'parameters': {
            'id': utils._id(_id),
            'resource': utils._id(resource),
        }
    }

    tx.send(query)


def remove(_id, resource, tx=neo4j):
    "Removes a resource from the collection."
    query = {
        'statement': queries.COLLECTION_REMOVE_RESOURCE,
        'parameters': {
            'id': utils._id(_id),
            'resource': utils._id(resource),
        }
    }

    tx.send(query)


def resources(_id, tx=neo4j):
    "Returns the resources in this collection."
    query = {
        'statement': queries.COLLECTION_RESOURCES,
        'parameters': {
            'id': utils._id(_id),
        }
    }

    result = tx.send(query)

    return [parse_resource(r) for r in result]


def resource_count(_id, tx=neo4j):
    "Returns the count of resources in this collection."
    query = {
        'statement': queries.COLLECTION_RESOURCE_COUNT,
        'parameters': {
            'id': utils._id(_id),
        }
    }

    result = tx.send(query)

    if result:
        return result[0][0]

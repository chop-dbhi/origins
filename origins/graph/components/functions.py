from __future__ import unicode_literals, absolute_import

from ..parsers import parse_component, parse_source, parse_timeline, \
    parse_relationship, parse_resource
from .. import utils, neo4j, base
from . import queries

try:
    str = unicode
except NameError:
    pass


def match(predicate=None, skip=None, limit=None, tx=neo4j):
    query = base.match(queries.MATCH_COMPONENTS, predicate, skip, limit)

    return [parse_component(r) for r in tx.send(query)]


def search(predicate, operator='OR', skip=None, limit=None, tx=neo4j):
    query = base.search(queries.SEARCH_COMPONENTS, 'rev', predicate,
                        operator, skip, limit)

    return [parse_component(r) for r in tx.send(query)]


def get(predicate, tx=neo4j):
    "Gets a single component given the predicate."
    query = base.get(queries.GET_COMPONENT, predicate)

    result = tx.send(query)

    if result:
        return parse_component(result[0])


def create(_id, properties=None, resource=None, tx=neo4j):
    "Creates a new component for a resource."
    query = base.create(queries.CREATE_COMPONENT, _id, properties, resource)

    result = tx.send(query)

    if result:
        return parse_component(result[0])


def update(_id, properties, tx=neo4j):
    "Updates a component."
    query = base.update(queries.UPDATE_COMPONENT, 'rev', _id, properties)
    properties = query['parameters']['properties']

    result = tx.send(query)

    if result:
        return parse_component(result[0])


def delete(_id, tx=neo4j):
    "Deletes a component."
    query = base.delete(queries.DELETE_COMPONENT, _id)

    result = tx.send(query)

    if result:
        return parse_component(result[0])


def relationships(_id, skip=None, limit=None, tx=neo4j):
    "Fetches component relationships for the component."
    statement = queries.COMPONENT_RELATIONSHIPS

    parameters = {
        'id': utils._id(_id),
    }

    statement, parameters = base.skip_limit(statement, parameters, skip, limit)

    query = {
        'statement': statement,
        'parameters': parameters,
    }

    return [parse_relationship(r) for r in tx.send(query)]


def resource(_id, tx=neo4j):
    "Gets the resource that manages this component."
    query = {
        'statement': queries.COMPONENT_RESOURCE,
        'parameters': {
            'id': utils._id(_id),
        }
    }

    result = tx.send(query)

    if result:
        return parse_resource(result[0])


def timeline(_id, skip=None, limit=None, tx=neo4j):
    "Gets the timeline for this component."
    query = base.timeline(queries.COMPONENT_TIMELINE, _id)

    return parse_timeline(tx.send(query))


def lineage(_id, tx=neo4j):
    "Gets the lineage of this component."
    query = base.lineage(queries.COMPONENT_LINEAGE)

    return [parse_source(r) for r in tx.send(query)]


def sources(_id, tx=neo4j):
    "Gets the immediate sources of this component."
    query = base.sources(queries.COMPONENT_SOURCES, _id)

    return [parse_component(r) for r in tx.send(query)]


def revisions(_id, tx=neo4j):
    "Gets the revision history of this component."
    query = base.revisions(queries.COMPONENT_REVISIONS, _id)

    return [parse_component(r) for r in tx.send(query)]


def revision(uuid, tx=neo4j):
    "Gets the revision history of this component."
    query = base.revision(queries.COMPONENT_REVISION, uuid)

    result = tx.send(query)

    if result:
        return parse_component(result[0])


def derive(generated, used, type='prov:PrimarySource', tx=neo4j):
    query = {
        'statement': queries.DERIVE_COMPONENT,
        'parameters': {
            'generated': utils._uuid(generated),
            'used': utils._uuid(used),
            'type': type,
        }
    }

    result = tx.send(query)

    if result:
        return utils.unpack(result[0])

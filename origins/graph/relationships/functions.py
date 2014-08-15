from ..parsers import parse_relationship, parse_timeline, parse_resource
from .. import neo4j, utils, base
from . import queries


def _prepare_query(query, start, type, end):
    properties = query['parameters']['properties']

    if not start:
        if 'origins:start' not in properties:
            raise ValueError('start required')
        start = properties.pop('origins:start')

    if not end:
        if 'origins:end' not in properties:
            raise ValueError('end required')
        end = properties.pop('origins:end')

    if not type:
        if 'origins:type' not in properties:
            raise ValueError('type required')
        type = properties.get('origins:type')
    else:
        properties['origins:type'] = type

    query['parameters']['start'] = utils._uuid(start)
    query['parameters']['end'] = utils._uuid(end)

    query['statement'] = query['statement'] % {
        'type': type,
    }

    return query


def match(predicate=None, skip=None, limit=None, tx=neo4j):
    query = base.match(queries.MATCH_RELATIONSHIPS, predicate, skip, limit)

    return [parse_relationship(r) for r in tx.send(query)]


def search(predicate, operator='OR', skip=None, limit=None, tx=neo4j):
    query = base.search(queries.SEARCH_RELATIONSHIPS, 'rev', predicate,
                        operator, skip, limit)

    return [parse_relationship(r) for r in tx.send(query)]


def get(predicate, tx=neo4j):
    "Gets a single resource given the predicate."
    query = base.get(queries.GET_RELATIONSHIP, predicate)

    result = tx.send(query)

    if result:
        return parse_relationship(result[0])


def create(_id, start=None, type=None, end=None, properties=None,
           resource=None, tx=neo4j):
    "Creates a new relationship between two components."
    query = base.create(queries.CREATE_RELATIONSHIP, _id, properties, resource)

    query = _prepare_query(query, start, type, end)

    result = tx.send(query)

    if result:
        return parse_relationship(result[0])


def update(_id, properties=None, tx=neo4j):
    "Updates a relationship."
    query = base.update(queries.UPDATE_RELATIONSHIP, 'rev', _id, properties)
    properties = query['parameters']['properties']

    # An update performed on a relationship can only change the properties,
    # but not start, end, or type
    if any([
        properties.pop('origins:start', None),
        properties.pop('origins:end', None),
        properties.pop('origins:type', None),
    ]):
        raise ValueError('update can only change properties not start '
                         'component, end component, or relationship type')

    result = tx.send(query)

    if result:
        return parse_relationship(result[0])


def delete(_id, tx=neo4j):
    "Deletes a relationship."
    query = base.delete(queries.DELETE_RELATIONSHIP, _id)

    result = tx.send(query)

    if result:
        return parse_relationship(result[0])


def descends(end, start, tx=neo4j):
    query = {
        'statement': queries.DESCENDS_RELATIONSHIP,
        'parameters': {
            'start': start,
            'end': end,
        }
    }

    result = tx.send(query)

    if result:
        return utils.unpack(result[0])


def derive(generated, used, type='prov:PrimarySource', tx=neo4j):
    query = {
        'statement': queries.DERIVE_RELATIONSHIP,
        'parameters': {
            'generated': generated,
            'used': used,
            'type': type,
        }
    }

    result = tx.send(query)

    if result:
        return utils.unpack(result[0])


def resource(_id, tx=neo4j):
    "Gets the resource that manages this relationship."
    query = {
        'statement': queries.RELATIONSHIP_RESOURCE,
        'parameters': {
            'id': utils._id(_id),
        }
    }

    result = tx.send(query)

    if result:
        return parse_resource(result[0])


def timeline(uuid, skip=None, limit=None, tx=neo4j):
    "Gets the timeline for this relationship."
    query = base.timeline(queries.RELATIONSHIP_TIMELINE, uuid, skip, limit)

    return parse_timeline(tx.send(query))


def revisions(_id, tx=neo4j):
    "Gets the revision history of this relationship."
    query = base.revisions(queries.RELATIONSHIP_REVISIONS, _id)

    return [parse_relationship(r) for r in tx.send(query)]


def revision(uuid, tx=neo4j):
    "Gets a single revision of this relationship."
    query = base.revision(queries.RELATIONSHIP_REVISION, uuid)

    result = tx.send(query)

    if result:
        return parse_relationship(result[0])

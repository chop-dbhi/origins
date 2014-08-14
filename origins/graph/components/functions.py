from __future__ import unicode_literals, absolute_import

from origins import provenance
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


def create(_id, properties=None, parent=None, resource=None, tx=neo4j):
    "Creates a new component for a resource."
    query = base.create(queries.CREATE_COMPONENT, _id, properties, resource)

    result = tx.send(query)[0][0]

    if result:
        provenance.load({
            'bundle': {
                'b1': {
                    'entity': {
                        'base': {
                            'origins:neo4j': result['component'],
                        },

                        'rev': {
                            'origins:neo4j': result['revision'],
                        },
                    },
                    'wasGeneratedBy': {
                        'wgb1': {
                            'prov:entity': 'base',
                        },
                        'wgb2': {
                            'prov:entity': 'rev',
                        }
                    },
                    'specializationOf': {
                        'so1': {
                            'prov:generalEntity': 'base',
                            'prov:specificEntity': 'rev',
                        }
                    }
                }
            }
        }, tx=tx)

        return result


def update(_id, properties, tx=neo4j):
    "Updates a component."
    query = base.update(queries.UPDATE_COMPONENT, 'rev', _id, properties)
    properties = query['parameters']['properties']

    result = tx.send(query)[0][0]

    if result:
        provenance.load({
            'bundle': {
                'b1': {
                    'entity': {
                        'base': {
                            'origins:neo4j': result['component'],
                        },
                        'pre': {
                            'origins:neo4j': result['previous'],
                        },
                        'rev': {
                            'origins:neo4j': result['revision'],
                        },
                    },
                    'wasGeneratedBy': {
                        'wgb1': {
                            'prov:entity': 'rev',
                        },
                    },
                    'wasInvalidatedBy': {
                        'wib1': {
                            'prov:entity': 'pre',
                            'origins:method': 'origins:InvalidatedByChange',
                        },
                    },
                    'wasDerivedFrom': {
                        'wdf1': {
                            'prov:generatedEntity': 'rev',
                            'prov:usedEntity': 'pre',
                            'prov:generation': 'wgb1',
                        },
                    },
                    'specializationOf': {
                        '_:so1': {
                            'prov:generalEntity': 'base',
                            'prov:specificEntity': 'rev',
                        }
                    },
                }
            }
        }, tx=tx)

        return result


def delete(_id, tx=neo4j):
    "Deletes a component."
    query = base.delete(queries.DELETE_COMPONENT, _id)

    result = tx.send(query)[0][0]

    if result:
        provenance.load({
            'bundle': {
                'b1': {
                    'entity': {
                        'base': {
                            'origins:neo4j': result['component'],
                        },
                        'revision': {
                            'origins:neo4j': result['revision'],
                        },
                    },
                    'wasInvalidatedBy': {
                        'wib': {
                            'prov:entity': 'revision',
                            'origins:method': 'origins:InvalidatedByRemoval',
                        },
                    },
                }
            }
        }, tx=tx)

        return result


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


def path(uuid, tx=neo4j):
    "List of components from the root."
    query = {
        'statement': queries.COMPONENT_PATH,
        'parameters': {
            'uuid': utils._uuid(uuid),
        }
    }

    return [parse_component(r) for r in tx.send(query)]


def parent(uuid, tx=neo4j):
    query = {
        'statement': queries.COMPONENT_PARENT,
        'parameters': {
            'uuid': utils._uuid(uuid),
        }
    }

    result = tx.send(query)

    if result:
        return parse_component(result[0])


def children(uuid, tx=neo4j):
    query = {
        'statement': queries.COMPONENT_CHILDREN,
        'parameters': {
            'uuid': utils._uuid(uuid),
        }
    }

    return [parse_component(r) for r in tx.send(query)]


def timeline(uuid, skip=None, limit=None, tx=neo4j):
    "Gets the timeline for this component."
    query = base.timeline(queries.COMPONENT_TIMELINE, uuid)

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
            'timestamp': utils.timestamp(),
        }
    }

    result = tx.send(query)

    if result:
        return utils.unpack(result[0])


def resource_count(_id, tx=neo4j):
    query = {
        'statement': queries.COMPONENT_RESOURCE_COUNT,
        'parameters': {
            'id': utils._id(_id),
        }
    }

    result = tx.send(query)

    if result:
        return result[0][0]


def relationship_count(_id, tx=neo4j):
    query = {
        'statement': queries.COMPONENT_RELATIONSHIP_COUNT,
        'parameters': {
            'id': utils._id(_id),
        }
    }

    result = tx.send(query)

    if result:
        return result[0][0]

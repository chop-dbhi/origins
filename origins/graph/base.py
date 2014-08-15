from uuid import uuid4
from . import utils, cypher


def skip_limit(statement, parameters, skip, limit):
    if skip:
        statement += ' SKIP {skip}'
        parameters['skip'] = skip

    if limit:
        statement += ' LIMIT {limit}'
        parameters['limit'] = limit

    return statement, parameters


def get(statement, predicate):
    predicate, placeholder = utils.resolve_predicate(predicate)

    return {
        'statement': statement % {
            'predicate': placeholder,
        },
        'parameters': {
            'predicate': predicate
        }
    }


def match(statement, predicate=None, skip=None, limit=None):
    if predicate:
        predicate, placeholder = utils.resolve_predicate(predicate)
        parameters = {'predicate': predicate}
    else:
        placeholder = ''
        parameters = {}

    statement, parameters = skip_limit(statement, parameters, skip, limit)

    return {
        'statement': statement % {
            'predicate': placeholder,
        },
        'parameters': parameters,
    }


def search(statement, var, predicate, operator='OR', skip=None, limit=None):
    keys = predicate.keys()
    placeholder = cypher.fuzzy_search(keys, var, 'predicate', operator)

    parameters = {
        'predicate': predicate,
    }

    statement, parameters = skip_limit(statement, parameters, skip, limit)

    return {
        'statement': statement % {
            'predicate': placeholder,
        },
        'parameters': parameters,
    }


def create(statement, _id, properties=None, resource=False):
    if isinstance(_id, dict):
        properties = _id
    else:
        if not properties:
            properties = {}

        properties['origins:id'] = _id

    if 'origins:id' not in properties:
        raise KeyError('origins:id required')

    if 'origins:uuid' in properties:
        raise KeyError('origins:uuid cannot be provided')

    timestamp = utils.timestamp()

    # Augment UUID and other metadata
    properties['origins:uuid'] = str(uuid4())
    properties['origins:timestamp'] = timestamp

    parameters = {
        'properties': properties,
        'timestamp': timestamp,
    }

    # Handle objects that are depend on a resource, component and relationship
    if resource is not False:
        if not resource:
            if 'origins:resource' not in properties:
                raise ValueError('resource must be specified')
            resource = properties.pop('origins:resource')

        if isinstance(resource, dict):
            if 'id' in resource:
                resource = resource['id']
            elif 'origins:id' in resource:
                resource = resource['origins:id']
            else:
                raise ValueError('resource does not have a ID')

        parameters['resource'] = resource

    return {
        'statement': statement,
        'parameters': parameters,
    }


def update(statement, var, _id, properties):
    if isinstance(properties.get('properties'), dict):
        properties = utils.pack(properties)

    timestamp = utils.timestamp()
    properties['origins:timestamp'] = timestamp
    properties['origins:uuid'] = str(uuid4())

    placeholder = cypher.set_parameters(properties, var, 'properties')

    return {
        'statement': statement % {
            'placeholder': placeholder,
        },
        'parameters': {
            'id': utils._id(_id),
            'properties': properties,
            'timestamp': timestamp,
        }
    }


def delete(statement, _id):
    timestamp = utils.timestamp()

    return {
        'statement': statement,
        'parameters': {
            'id': utils._id(_id),
            'timestamp': timestamp,
        }
    }


def revisions(statement, _id):
    return {
        'statement': statement,
        'parameters': {
            'id': utils._id(_id),
        }
    }


def revision(statement, uuid):
    return {
        'statement': statement,
        'parameters': {
            'uuid': uuid,
        }
    }


def timeline(statement, uuid, skip=None, limit=None):
    parameters = {
        'uuid': utils._uuid(uuid)
    }

    statement, parameters = skip_limit(statement, parameters, skip, limit)

    return {
        'statement': statement,
        'parameters': parameters,
    }


def lineage(statement, _id):
    return {
        'statement': statement,
        'parameters': {
            'id': utils._id(_id),
        }
    }


def sources(statement, _id):
    return {
        'statement': statement,
        'parameters': {
            'id': utils._id(_id),
        }
    }

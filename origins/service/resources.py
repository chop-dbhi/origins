from flask import request, url_for
from origins.exceptions import ValidationError
from origins.graph import resources, components
from .nodes import Nodes, Node
from .components import Component


def prepare(n):
    n = n.to_dict()

    n['links'] = {
        'self': {
            'href': url_for('resource', uuid=n['uuid'],
                            _external=True),
        },
        'components': {
            'href': url_for('resource-components', uuid=n['uuid'],
                            _external=True),
        },
    }

    return n


class Resources(Nodes):
    module = resources

    def prepare(self, n):
        return prepare(n)

    def get_attrs(self, data):
        return {
            'id': data.get('id'),
            'type': data.get('type'),
            'label': data.get('label'),
            'description': data.get('description'),
            'properties': data.get('properties'),
        }


class Resource(Node):
    module = resources

    def prepare(self, n):
        return prepare(n)

    def get_attrs(self, data):
        return {
            'label': data.get('label'),
            'type': data.get('type'),
            'description': data.get('description'),
            'properties': data.get('properties'),
        }


class ResourceComponents(Nodes):
    def prepare(self, uuid, n):
        n = n.to_dict()

        n['_links'] = {
            'self': {
                'href': url_for('component', uuid=n['uuid'],
                                _external=True),
            },
            'resource': {
                'href': url_for('resource', uuid=uuid,
                                _external=True),
            },
        }

        return n

    def get(self, uuid):
        params = self.get_params()

        if params['query']:
            predicate = self.get_search_predicate(params['query'])
        else:
            predicate = None

        try:
            cursor = resources.components(uuid,
                                          predicate=predicate,
                                          limit=params['limit'],
                                          skip=params['skip'])
        except ValidationError as e:
            return {'message': str(e)}, 404

        result = []

        for n in cursor:
            result.append(self.prepare(uuid, n))

        return result, 200

    def post(self, uuid):
        handler = Component()

        attrs = handler.get_attrs(request.json)
        attrs['resource'] = uuid

        try:
            n = components.add(**attrs)
        except ValidationError as e:
            return {'message': str(e)}, 404

        return handler.prepare(n, resource=uuid), 201

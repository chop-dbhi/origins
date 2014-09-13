from flask import request, url_for
from origins.exceptions import ValidationError
from origins.graph import Resource
from .nodes import NodesResource, NodeResource
from .components import ComponentResource
from .relationships import RelationshipResource


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
        'relationships': {
            'href': url_for('resource-relationships', uuid=n['uuid'],
                            _external=True),
        },
    }

    return n


class ResourcesResource(NodesResource):
    model = Resource

    def prepare(self, n):
        return prepare(n)


class ResourceResource(NodeResource):
    model = Resource

    def prepare(self, n):
        return prepare(n)


class ResourceComponentsResource(NodesResource):
    def prepare(self, uuid, n):
        n = n.to_dict()

        n['links'] = {
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
            cursor = Resource.components(uuid,
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
        handler = ComponentResource()

        attrs = handler.get_attrs(request.json)
        attrs['resource'] = Resource(uuid=uuid)

        try:
            n = handler.model.add(**attrs)
        except ValidationError as e:
            return {'message': str(e)}, 404

        return handler.prepare(n, resource=uuid), 201


class ResourceRelationshipsResource(NodesResource):
    def prepare(self, uuid, n):
        n = n.to_dict()

        n['links'] = {
            'self': {
                'href': url_for('relationship', uuid=n['uuid'],
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
            cursor = Resource.relationships(uuid,
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
        handler = RelationshipResource()

        attrs = handler.get_attrs(request.json)
        attrs['resource'] = Resource(uuid=uuid)

        try:
            n = handler.model.add(**attrs)
        except ValidationError as e:
            return {'message': str(e)}, 404

        return handler.prepare(n, resource=uuid), 201

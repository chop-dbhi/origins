from flask import request, url_for
from origins.exceptions import ValidationError
from origins.graph import Collection
from .nodes import NodesResource, NodeResource
from .resources import ResourceResource


def prepare(n):
    n = n.to_dict()

    n['links'] = {
        'self': {
            'href': url_for('collection', uuid=n['uuid'],
                            _external=True),
        },
        'resources': {
            'href': url_for('collection-resources', uuid=n['uuid'],
                            _external=True),
        },
    }

    return n


class CollectionsResource(NodesResource):
    model = Collection

    def prepare(self, n):
        return prepare(n)


class CollectionResource(NodeResource):
    model = Collection

    def prepare(self, n):
        return prepare(n)


class CollectionResourcesResource(NodesResource):
    def get_attrs(self, data):
        return {
            'resource': data.get('resource'),
        }

    def get(self, uuid):
        params = self.get_params()

        if params['query']:
            predicate = self.get_search_predicate(params['query'])
        else:
            predicate = None

        try:
            cursor = Collection.resources(uuid,
                                          predicate=predicate,
                                          limit=params['limit'],
                                          skip=params['skip'])
        except ValidationError as e:
            return {'message': str(e)}, 404

        result = []

        handler = ResourceResource()

        for n in cursor:
            result.append(handler.prepare(n))

        return result, 200

    def post(self, uuid):
        attrs = self.get_attrs(request.json)

        try:
            Collection.add_resource(uuid, **attrs)
        except ValidationError as e:
            return {'message': str(e)}, 422

        return '', 204

import json
import codecs
import traceback
from flask import request, url_for
from flask.ext import restful
from origins.exceptions import ValidationError, DoesNotExist
from origins.graph import Resource
from origins.graph.sync import sync
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


class ResourceSyncResource(restful.Resource):
    def post(self, uuid=None):
        if request.json:
            data = request.json
        elif request.files:
            try:
                # Wrap file to stream convert bytes to str
                reader = codecs.getreader('utf8')
                data = json.load(reader(request.files['file']))
            except ValueError:
                return {'message': 'file must be JSON'}, 422
        else:
            return {'message': 'request payload or file required'}, 422

        try:
            out = sync(data)
        except Exception:
            return {'message': traceback.format_exc()}, 422

        handler = ResourceResource()
        out['resource'] = handler.prepare(out['resource'])

        return out


class ResourceComponentsResource(NodesResource):
    def prepare(self, n, r):
        n = n.to_dict()

        n['resource'] = r

        n['links'] = {
            'self': {
                'href': url_for('component', uuid=n['uuid'],
                                _external=True),
            },
            'resource': {
                'href': url_for('resource', uuid=r['uuid'],
                                _external=True),
            },
        }

        return n

    def get(self, uuid):
        try:
            r = Resource.get(uuid).to_dict()
        except DoesNotExist as e:
            return {'message': str(e)}, 404

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
            result.append(self.prepare(n, r))

        return result, 200

    def post(self, uuid):
        try:
            r = Resource.get(uuid).to_dict()
        except DoesNotExist as e:
            return {'message': str(e)}, 404

        handler = ComponentResource()

        attrs = handler.get_attrs(request.json)
        attrs['resource'] = Resource(uuid=uuid)

        try:
            n = handler.model.add(**attrs)
        except ValidationError as e:
            return {'message': str(e)}, 404

        return handler.prepare(n, resource=r), 201


class ResourceRelationshipsResource(NodesResource):
    def prepare(self, n, r):
        n = n.to_dict()

        n['resource'] = r

        n['links'] = {
            'self': {
                'href': url_for('relationship', uuid=n['uuid'],
                                _external=True),
            },
            'resource': {
                'href': url_for('resource', uuid=r['uuid'],
                                _external=True),
            },
        }

        return n

    def get(self, uuid):
        try:
            r = Resource.get(uuid).to_dict()
        except DoesNotExist as e:
            return {'message': str(e)}, 404

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
            result.append(self.prepare(n, r))

        return result, 200

    def post(self, uuid):
        try:
            r = Resource.get(uuid).to_dict()
        except DoesNotExist as e:
            return {'message': str(e)}, 404

        handler = RelationshipResource()

        attrs = handler.get_attrs(request.json)
        attrs['resource'] = Resource(uuid=uuid)

        try:
            n = handler.model.add(**attrs)
        except ValidationError as e:
            return {'message': str(e)}, 404

        return handler.prepare(n, r), 201

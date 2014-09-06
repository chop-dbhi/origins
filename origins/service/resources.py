from flask import request
from flask.ext import restful
from origins.exceptions import DoesNotExist, InvalidState, ValidationError
from origins.graph import resources, components
from . import utils


class Resources(restful.Resource):
    def get(self):
        try:
            limit = int(request.args.get('limit'))
        except TypeError:
            limit = utils.DEFAULT_PAGE_LIMIT

        try:
            skip = int(request.args.get('skip'))
        except TypeError:
            skip = 0

        type = request.args.get('type')
        query = request.args.getlist('query')

        if query:
            param = '(?i)' + '|'.join(['.*' + q + '.*' for q in query])

            predicate = {
                'label': param,
                'description': param,
            }

            cursor = resources.search(predicate, limit=limit, skip=skip)
        else:
            cursor = resources.match(type=type, limit=limit, skip=skip)

        result = []

        for r in cursor:
            r = utils.add_resource_data(r)
            result.append(r)

        return result

    def post(self):
        data = request.json

        attrs = {
            'id': data.get('id'),
            'type': data.get('type'),
            'label': data.get('label'),
            'description': data.get('description'),
            'properties': data.get('properties'),
        }

        try:
            r = resources.add(**attrs)
        except ValidationError as e:
            return {'message': e.message}, 422

        return utils.add_resource_data(r), 201


class Resource(restful.Resource):
    def get(self, uuid):
        try:
            r = resources.get(uuid)
        except DoesNotExist:
            return {'message': 'resource does not exist'}, 404

        return utils.add_resource_data(r), 200

    def put(self, uuid):
        data = request.json

        attrs = {
            'label': data.get('label'),
            'type': data.get('type'),
            'description': data.get('description'),
            'properties': data.get('properties'),
        }

        try:
            r = resources.set(uuid, **attrs)
        except DoesNotExist:
            return {'message': 'resource does not exist'}, 404
        except InvalidState:
            return {'message': 'invalid resource cannot be updated'}, 422

        if request.args.get('quiet') == '1':
            return '', 204

        return utils.add_resource_data(r), 200

    def delete(self, uuid):
        try:
            resources.remove(uuid)
        except DoesNotExist:
            return {'message': 'resource does not exist'}, 404
        except InvalidState:
            pass

        return '', 204


class ResourceComponents(restful.Resource):
    def get(self, uuid):
        try:
            limit = int(request.args.get('limit'))
        except TypeError:
            limit = utils.DEFAULT_PAGE_LIMIT

        try:
            skip = int(request.args.get('skip'))
        except TypeError:
            skip = 0

        try:
            cursor = resources.components(uuid, limit=limit, skip=skip)
        except ValidationError:
            return {'message': 'resources does not exist'}, 404

        result = []

        for c in cursor:
            # c = utils.add_component_data(c)
            result.append(c)

        return result

    def post(self, uuid):
        c = components.add(resource=uuid)

        if c is None:
            return {'message': 'resources does not exist'}, 404

        return c, 201

from flask import request
from flask.ext import restful
from origins.graph import components
from origins.exceptions import ValidationError, DoesNotExist, InvalidState
from . import utils


class Components(restful.Resource):
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

            cursor = components.search(predicate, limit=limit, skip=skip)
        else:
            cursor = components.match(type=type, limit=limit, skip=skip)

        result = []

        for c in cursor:
            c = utils.add_component_data(c)
            result.append(c)

        return result

    def post(self):
        data = request.json

        attrs = {
            'id': data.get('id'),
            'type': data.get('type'),
            'label': data.get('label'),
            'description': data.get('description'),
            'properties': data.get('properties'),
            'resource': data.get('resource'),
        }

        try:
            c = components.add(**attrs)
        except ValidationError as e:
            return {'message': e.message}, 422

        return utils.add_component_data(c), 201


class Component(restful.Resource):
    def get(self, uuid):

        try:
            c = components.get(uuid)
        except DoesNotExist:
            return {'message': 'component does not exist'}, 404

        return utils.add_component_data(c), 200

    def put(self, uuid):
        data = request.json

        attrs = {
            'type': data.get('type'),
            'label': data.get('label'),
            'description': data.get('description'),
            'properties': data.get('properties'),
        }

        try:
            c = components.set(uuid, **attrs)
        except DoesNotExist:
            return {'message': 'component does not exist'}, 404
        except InvalidState:
            return {'message': 'invalid component cannot be updated'}, 422

        if request.args.get('quiet') == '1':
            return '', 204

        return utils.add_component_data(c), 200

    def delete(self, uuid):
        try:
            components.remove(uuid)
        except DoesNotExist:
            return {'message': 'components does not exist'}, 404
        except InvalidState:
            pass

        return '', 204

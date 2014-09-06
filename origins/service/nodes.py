from flask import request
from flask.ext import restful
from origins.graph.core import nodes
from origins.exceptions import DoesNotExist, InvalidState
from . import utils


class Nodes(restful.Resource):
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

        cursor = nodes.match(type=type, limit=limit, skip=skip)
        result = []

        for n in cursor:
            result.append(utils.add_node_data(n))

        return result, 200

    def post(self):
        data = request.json

        attrs = {
            'id': data.get('id'),
            'type': data.get('type'),
            'label': data.get('label'),
            'description': data.get('description'),
            'properties': data.get('properties'),
        }

        n = nodes.add(**attrs)

        return utils.add_node_data(n), 201


class Node(restful.Resource):
    def get(self, uuid):
        try:
            n = nodes.get(uuid)
        except DoesNotExist:
            return {'message': 'node does not exist'}, 404

        return utils.add_node_data(n), 200

    def put(self, uuid):
        data = request.json

        attrs = {
            'type': data.get('type'),
            'label': data.get('label'),
            'description': data.get('description'),
            'properties': data.get('properties'),
        }

        try:
            n = nodes.set(uuid, **attrs)
        except DoesNotExist:
            return {'message': 'node does not exist'}, 404
        except InvalidState as e:
            return {'message': e.message}, 422

        # Nothing change
        if not n:
            return '', 204

        return utils.add_node_data(n), 200

    def delete(self, uuid):
        try:
            nodes.remove(uuid)
        except DoesNotExist:
            return {'message': 'node does not exist'}, 404
        except InvalidState as e:
            return {'message': e.message}, 422

        return '', 204

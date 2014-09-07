from flask import url_for, request
from flask.ext import restful
from origins.graph.core import nodes
from origins.exceptions import DoesNotExist, InvalidState, ValidationError
from . import utils


def prepare(n):
    n = n.to_dict()

    n['links'] = {
        'self': {
            'href': url_for('nodes', _external=True),
        },
    }

    return n


class Nodes(restful.Resource):
    module = nodes

    def prepare(self, n):
        return prepare(n)

    def get_params(self):
        args = request.args

        try:
            limit = int(args.get('limit'))
        except TypeError:
            limit = utils.DEFAULT_PAGE_LIMIT

        try:
            skip = int(args.get('skip'))
        except TypeError:
            skip = 0

        type = args.get('type')
        query = args.getlist('query')

        return {
            'limit': limit,
            'skip': skip,
            'type': type,
            'query': query,
        }

    def get_attrs(self, data):
        return {
            'id': data.get('id'),
            'type': data.get('type'),
            'label': data.get('label'),
            'description': data.get('description'),
            'properties': data.get('properties'),
        }

    def get_search_predicate(self, query):
        param = '(?i)' + '|'.join(['.*' + q + '.*' for q in query])

        return {
            'label': param,
            'description': param,
        }

    def get(self):
        params = self.get_params()

        if params['query']:
            predicate = self.get_search_predicate(params['query'])

            cursor = self.module.search(predicate,
                                        operator='OR',
                                        limit=params['limit'],
                                        skip=params['skip'])
        else:
            cursor = self.module.match(type=params['type'],
                                       limit=params['limit'],
                                       skip=params['skip'])

        result = []

        for n in cursor:
            result.append(self.prepare(n))

        return result, 200

    def post(self):
        attrs = self.get_attrs(request.json)

        try:
            n = self.module.add(**attrs)
        except ValidationError as e:
            return {'message': str(e)}, 422

        return self.prepare(n), 201


class Node(restful.Resource):
    module = nodes

    def prepare(self, n):
        return prepare(n)

    def get_params(self):
        args = request.args

        quiet = args.get('quiet')

        if quiet == '1':
            quiet = True
        elif quiet == '0':
            quiet = False

        return {
            'quiet': quiet,
        }

    def get_attrs(self, data):
        return {
            'type': data.get('type'),
            'label': data.get('label'),
            'description': data.get('description'),
            'properties': data.get('properties'),
        }

    def get(self, uuid):
        try:
            n = self.module.get(uuid)
        except DoesNotExist as e:
            return {'message': str(e)}, 404

        return self.prepare(n), 200

    def put(self, uuid):
        attrs = self.get_attrs(request.json)

        try:
            n = self.module.set(uuid, **attrs)
        except DoesNotExist as e:
            return {'message': str(e)}, 404
        except InvalidState as e:
            return {'message': str(e)}, 422

        # Nothing change
        if not n:
            return '', 204

        return self.prepare(n), 200

    def delete(self, uuid):
        try:
            self.module.remove(uuid)
        except DoesNotExist as e:
            return {'message': str(e)}, 404
        except InvalidState as e:
            return {'message': str(e)}, 422

        return '', 204

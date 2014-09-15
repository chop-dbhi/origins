from flask import url_for, request
from flask.ext import restful
from origins.graph import Node
from origins.exceptions import DoesNotExist, ValidationError
from . import utils


def prepare(n):
    n = n.to_dict()

    n['links'] = {
        'self': {
            'href': url_for('nodes', _external=True),
        },
    }

    return n


class NodesResource(restful.Resource):
    model = Node

    attr_keys = (
        'id',
        'type',
        'label',
        'description',
        'properties',
    )

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
        attrs = {}

        if data:
            for key in self.attr_keys:
                if key in data:
                    attrs[key] = data[key]

        return attrs

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

            cursor = self.model.search(predicate,
                                       operator='OR',
                                       limit=params['limit'],
                                       skip=params['skip'])
        else:
            cursor = self.model.match(type=params['type'],
                                      limit=params['limit'],
                                      skip=params['skip'])

        result = []

        for n in cursor:
            result.append(self.prepare(n))

        return result, 200

    def post(self):
        try:
            attrs = self.get_attrs(request.json)
            n = self.model.add(**attrs)
        except ValidationError as e:
            return {'message': str(e)}, 422

        return self.prepare(n), 201


class NodeResource(restful.Resource):
    model = Node

    attr_keys = (
        'type',
        'label',
        'description',
        'properties',
    )

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
        attrs = {}

        if data:
            for key in self.attr_keys:
                if key in data:
                    attrs[key] = data[key]

        return attrs

    def get(self, uuid):
        try:
            n = self.model.get(uuid)
        except DoesNotExist as e:
            return {'message': str(e)}, 404

        return self.prepare(n), 200

    def put(self, uuid):
        try:
            attrs = self.get_attrs(request.json)
            n = self.model.set(uuid, **attrs)
        except DoesNotExist as e:
            return {'message': str(e)}, 404
        except ValidationError as e:
            return {'message': str(e)}, 422

        # Nothing change
        if not n:
            return '', 204

        return self.prepare(n), 200

    def delete(self, uuid):
        try:
            self.model.remove(uuid)
        except DoesNotExist as e:
            return {'message': str(e)}, 404
        except ValidationError as e:
            return {'message': str(e)}, 422

        return '', 204

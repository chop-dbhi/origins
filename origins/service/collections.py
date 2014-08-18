from flask import abort, request
from flask.ext import restful
from origins.graph import collections
from . import utils


class Collections(restful.Resource):
    def get(self):
        try:
            limit = int(request.args.get('limit'))
        except TypeError:
            limit = utils.DEFAULT_PAGE_LIMIT

        try:
            skip = int(request.args.get('skip'))
        except TypeError:
            skip = 0

        query = request.args.getlist('query')

        if query:
            param = '(?i)' + '|'.join(['.*' + q + '.*' for q in query])

            predicate = {
                'origins:label': param,
            }
            cursor = collections.search(predicate, limit=limit, skip=skip)
        else:
            cursor = collections.match(limit=limit, skip=skip)

        result = []

        for c in cursor:
            c = utils.add_collection_data(c)
            result.append(c)

        return result

    def post(self):
        c = collections.create(request.json)

        return utils.add_collection_data(c), 201


class Collection(restful.Resource):
    def get(self, uuid):
        c = collections.get(uuid)

        if not c:
            abort(404)

        cursor = collections.resources(c)
        result = []

        for r in cursor:
            r = utils.add_resource_data(r)
            result.append(r)

        c['resources'] = result

        return utils.add_collection_data(c)

    def put(self, uuid):
        c = collections.get(uuid)

        if not c:
            abort(404)

        c = collections.update(c, request.json)

        if request.args.get('quiet') == '1':
            return '', 204

        return utils.add_collection_data(c), 200

    def delete(self, uuid):
        c = collections.get(uuid)

        if not c:
            abort(404)

        collections.delete(c)
        return '', 200


class CollectionResources(restful.Resource):
    def get(self, uuid):
        c = collections.get(uuid)

        if not c:
            abort(404)

        cursor = collections.resources(c)
        result = []

        for r in cursor:
            r = utils.add_resource_data(r)
            result.append(r)

        return result

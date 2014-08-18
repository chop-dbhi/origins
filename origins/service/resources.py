from flask import abort, request
from flask.ext import restful
from origins.graph import resources, components, relationships
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

        query = request.args.getlist('query')

        if query:
            param = '(?i)' + '|'.join(['.*' + q + '.*' for q in query])

            predicate = {
                'origins:label': param,
            }
            cursor = resources.search(predicate, limit=limit, skip=skip)
        else:
            cursor = resources.match(limit=limit, skip=skip)

        result = []

        for r in cursor:
            r = utils.add_resource_data(r)
            result.append(r)

        return result

    def post(self):
        attrs = utils.pack(request.json)
        r = resources.create(attrs)
        return utils.add_resource_data(r), 201


class Resource(restful.Resource):
    def get(self, uuid):
        r = resources.get(uuid)

        if not r:
            abort(404)

        r = utils.add_resource_data(r)

        return r

    def put(self, uuid):
        r = resources.get(uuid)

        if not r:
            abort(404)

        r = resources.update(r, request.json)

        if request.args.get('quiet') == '1':
            return '', 204

        return utils.add_resource_data(r), 200


class ResourceComponents(restful.Resource):
    def get(self, uuid):
        r = resources.get(uuid)

        if not r:
            abort(404)

        try:
            limit = int(request.args.get('limit'))
        except TypeError:
            limit = utils.DEFAULT_PAGE_LIMIT

        try:
            skip = int(request.args.get('skip'))
        except TypeError:
            skip = 0

        cursor = resources.components(r, limit=limit, skip=skip)

        result = []

        for c in cursor:
            c['resource'] = r
            c = utils.add_component_data(c)
            result.append(c)

        return result

    def post(self, uuid):
        r = resources.get(uuid)

        if not r:
            abort(404)

        c = components.create(request.json, resource=r)

        return utils.add_component_data(c), 201


class ResourceRelationships(restful.Resource):
    def get(self, uuid):
        r = resources.get(uuid)

        if not r:
            abort(404)

        try:
            limit = int(request.args.get('limit'))
        except TypeError:
            limit = utils.DEFAULT_PAGE_LIMIT

        try:
            skip = int(request.args.get('skip'))
        except TypeError:
            skip = 0

        cursor = resources.relationships(r, limit=limit, skip=skip)

        result = []

        for c in cursor:
            c = utils.add_relationship_data(c)
            result.append(c)

        return result

    def post(self, uuid):
        r = resources.get(uuid)

        if not r:
            abort(404)

        c = relationships.create(request.json, resource=r)

        return utils.add_relationship_data(c), 201


class ResourceTimeline(restful.Resource):
    def get(self, uuid):
        r = resources.get(uuid)

        if not r:
            abort(404)

        try:
            limit = int(request.args.get('limit'))
        except TypeError:
            limit = utils.DEFAULT_PAGE_LIMIT

        try:
            skip = int(request.args.get('skip'))
        except TypeError:
            skip = 0

        return resources.timeline(r, limit=limit, skip=skip)

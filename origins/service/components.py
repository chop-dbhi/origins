from flask import abort, request
from flask.ext import restful
from origins.graph import components
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

        query = request.args.getlist('query')

        if query:
            param = '(?i)' + '|'.join(['.*' + q + '.*' for q in query])

            predicate = {
                'origins:label': param,
            }
            cursor = components.search(predicate, limit=limit, skip=skip)
        else:
            cursor = components.match(limit=limit, skip=skip)

        result = []

        for c in cursor:
            c = utils.add_component_data(c)
            result.append(c)

        return result


class Component(restful.Resource):
    def get(self, uuid):
        c = components.revision(uuid)

        if not c:
            abort(404)

        return utils.add_component_data(c)

    def put(self, uuid):
        c = components.get(uuid)

        if not c:
            abort(404)

        c = components.update(c, request.json)

        if request.args.get('quiet') == '1':
            return '', 204

        return utils.add_component_data(c), 200


class ComponentSources(restful.Resource):
    def get(self, uuid):
        c = components.get(uuid)

        if not c:
            abort(404)

        cursor = components.sources(c)

        result = []

        for s in cursor:
            s = utils.add_component_data(s)
            result.append(s)

        return result


class ComponentChildren(restful.Resource):
    def get(self, uuid):
        c = components.get(uuid)

        if not c:
            abort(404)

        cursor = components.children(c)

        result = []

        for s in cursor:
            s = utils.add_component_data(s)
            result.append(s)

        return result


class ComponentLineage(restful.Resource):
    def get(self, uuid):
        c = components.get(uuid)

        if not c:
            abort(404)

        cursor = components.lineage(c)

        result = []

        for s in cursor:
            s['component'] = utils.add_component_data(s['component'])
            result.append(s)

        return result


class ComponentRevisions(restful.Resource):
    def get(self, uuid):
        c = components.get(uuid)

        if not c:
            abort(404)

        cursor = components.revisions(c)

        result = []

        for r in cursor:
            r = utils.add_component_data(r)
            result.append(r)

        return result


class ComponentRevision(restful.Resource):
    def get(self, uuid):
        c = components.revision(uuid)

        if not c:
            abort(404)

        return utils.add_component_data(c)


class ComponentTimeline(restful.Resource):
    def get(self, uuid):
        c = components.revision(uuid)

        if not c:
            abort(404)

        return components.timeline(c)


class ComponentRelationships(restful.Resource):
    def get(self, uuid):
        c = components.get(uuid)

        if not c:
            abort(404)

        cursor = components.relationships(c)

        result = []

        for r in cursor:
            r = utils.add_relationship_data(r)
            result.append(r)

        return result

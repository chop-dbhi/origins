from flask import abort, request
from flask.ext import restful
from origins.graph import relationships
from . import utils


class Relationships(restful.Resource):
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
            cursor = relationships.search(predicate, limit=limit, skip=skip)
        else:
            cursor = relationships.match(limit=limit, skip=skip)

        result = []

        for c in cursor:
            c = utils.add_relationship_data(c)
            result.append(c)

        return result


class Relationship(restful.Resource):
    def get(self, uuid):
        c = relationships.get(uuid)

        if not c:
            abort(404)

        return utils.add_relationship_data(c)

    def put(self, uuid):
        c = relationships.get(uuid)

        if not c:
            abort(404)

        c = relationships.update(c, request.json)

        if request.args.get('quiet') == '1':
            return '', 204

        return utils.add_relationship_data(c), 200


class RelationshipRevisions(restful.Resource):
    def get(self, uuid):
        c = relationships.get(uuid)

        if not c:
            abort(404)

        cursor = relationships.revisions(c)

        result = []

        for r in cursor:
            r = utils.add_relationship_data(r)
            result.append(r)

        return result


class RelationshipRevision(restful.Resource):
    def get(self, uuid):
        c = relationships.revision(uuid)

        if not c:
            abort(404)

        return utils.add_relationship_data(c)


class RelationshipTimeline(restful.Resource):
    def get(self, uuid):
        c = relationships.revision(uuid)

        if not c:
            abort(404)

        return relationships.timeline(c)

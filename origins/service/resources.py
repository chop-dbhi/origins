import json
from urllib.parse import unquote
from flask import Flask, abort, url_for, make_response, request
from flask.ext import restful
from flask_cors import cross_origin
from origins.graph import resources, components, collections, \
    relationships, trends

DEFAULT_LIMIT = 200


try:
    str = unicode
except NameError:
    pass


def add_collection_data(c):
    uuid = c['uuid']

    c['resource_count'] = collections.resource_count(c)

    c['_links'] = {
        'self': {
            'href': url_for('collection', uuid=uuid,
                            _external=True),
        },
        'resources': {
            'href': url_for('collection-resources', uuid=uuid,
                            _external=True),
        }
    }

    return c


def add_resource_data(r):
    uuid = r['uuid']

    r['component_types'] = resources.component_types(r)
    r['component_count'] = resources.component_count(r)
    r['relationship_count'] = resources.relationship_count(r)
    r['collection_count'] = resources.collection_count(r)

    r['_links'] = {
        'self': {
            'href': url_for('resource', uuid=uuid,
                            _external=True),
        },
        'components': {
            'href': url_for('resource-components', uuid=uuid,
                            _external=True),
        },
        'relationships': {
            'href': url_for('resource-relationships', uuid=uuid,
                            _external=True),
        },
        'timeline': {
            'href': url_for('resource-timeline', uuid=uuid,
                            _external=True),
        }
    }

    return r


def add_component_data(c):
    uuid = c['uuid']

    if 'resource' in c:
        r = c['resource']
    else:
        r = components.resource(c)
        c['resource'] = r

    c['relationship_count'] = components.relationship_count(c)
    c['resource_count'] = components.resource_count(c)
    c['path'] = components.path(c)

    c['_links'] = {
        'self': {
            'href': url_for('component-revision', uuid=uuid,
                            _external=True),
        },
        'relationships': {
            'href': url_for('component-relationships', uuid=uuid,
                            _external=True),
        },
        'revisions': {
            'href': url_for('component-revisions', uuid=uuid,
                            _external=True),
        },
        'children': {
            'href': url_for('component-children', uuid=uuid,
                            _external=True),
        },
        'sources': {
            'href': url_for('component-sources', uuid=uuid,
                            _external=True),
        },
        'lineage': {
            'href': url_for('component-lineage', uuid=uuid,
                            _external=True),
        },
        'timeline': {
            'href': url_for('component-timeline', uuid=uuid,
                            _external=True)
        },
        'resource': {
            'href': url_for('resource', uuid=r['uuid'],
                            _external=True)
        },
    }

    return c


def add_relationship_data(c):
    uuid = c['uuid']

    r = relationships.resource(c)

    c['_links'] = {
        'self': {
            'href': url_for('relationship-revision', uuid=uuid,
                            _external=True),
        },
        'revisions': {
            'href': url_for('relationship-revisions', uuid=uuid,
                            _external=True),
        },
        'timeline': {
            'href': url_for('relationship-timeline', uuid=uuid,
                            _external=True)
        },
        'resource': {
            'href': url_for('resource', uuid=r['uuid'],
                            _external=True)
        },
    }

    return c


class Resources(restful.Resource):
    def get(self):
        try:
            limit = int(request.args.get('limit'))
        except TypeError:
            limit = DEFAULT_LIMIT

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
            r = add_resource_data(r)
            result.append(r)

        return result

    def post(self):
        r = resources.create(request.json)
        return add_resource_data(r), 201


class Resource(restful.Resource):
    def get(self, uuid):
        r = resources.get(uuid)

        if not r:
            abort(404)

        r = add_resource_data(r)

        return r

    def put(self, uuid):
        r = resources.get(uuid)

        if not r:
            abort(404)

        r = resources.update(r, request.json)

        if request.args.get('quiet') == '1':
            return '', 204

        return add_resource_data(r), 200


class ResourceComponents(restful.Resource):
    def get(self, uuid):
        r = resources.get(uuid)

        if not r:
            abort(404)

        try:
            limit = int(request.args.get('limit'))
        except TypeError:
            limit = DEFAULT_LIMIT

        try:
            skip = int(request.args.get('skip'))
        except TypeError:
            skip = 0

        cursor = resources.components(r, limit=limit, skip=skip)

        result = []

        for c in cursor:
            c['resource'] = r
            c = add_component_data(c)
            result.append(c)

        return result

    def post(self, uuid):
        r = resources.get(uuid)

        if not r:
            abort(404)

        c = components.create(request.json, resource=r)

        return add_component_data(c), 201


class ResourceRelationships(restful.Resource):
    def get(self, uuid):
        r = resources.get(uuid)

        if not r:
            abort(404)

        try:
            limit = int(request.args.get('limit'))
        except TypeError:
            limit = DEFAULT_LIMIT

        try:
            skip = int(request.args.get('skip'))
        except TypeError:
            skip = 0

        cursor = resources.relationships(r, limit=limit, skip=skip)

        result = []

        for c in cursor:
            c = add_relationship_data(c)
            result.append(c)

        return result

    def post(self, uuid):
        r = resources.get(uuid)

        if not r:
            abort(404)

        c = relationships.create(request.json, resource=r)

        return add_relationship_data(c), 201


class ResourceTimeline(restful.Resource):
    def get(self, uuid):
        r = resources.get(uuid)

        if not r:
            abort(404)

        try:
            limit = int(request.args.get('limit'))
        except TypeError:
            limit = DEFAULT_LIMIT

        try:
            skip = int(request.args.get('skip'))
        except TypeError:
            skip = 0

        return resources.timeline(r, limit=limit, skip=skip)


class Components(restful.Resource):
    def get(self):
        try:
            limit = int(request.args.get('limit'))
        except TypeError:
            limit = DEFAULT_LIMIT

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
            c = add_component_data(c)
            result.append(c)

        return result


class Component(restful.Resource):
    def get(self, uuid):
        c = components.revision(uuid)

        if not c:
            abort(404)

        return add_component_data(c)

    def put(self, uuid):
        c = components.get(uuid)

        if not c:
            abort(404)

        c = components.update(c, request.json)

        if request.args.get('quiet') == '1':
            return '', 204

        return add_component_data(c), 200


class ComponentSources(restful.Resource):
    def get(self, uuid):
        c = components.get(uuid)

        if not c:
            abort(404)

        cursor = components.sources(c)

        result = []

        for s in cursor:
            s = add_component_data(s)
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
            s = add_component_data(s)
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
            s['component'] = add_component_data(s['component'])
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
            r = add_component_data(r)
            result.append(r)

        return result


class ComponentRevision(restful.Resource):
    def get(self, uuid):
        c = components.revision(uuid)

        if not c:
            abort(404)

        return add_component_data(c)


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
            r = add_relationship_data(r)
            result.append(r)

        return result


class Relationships(restful.Resource):
    def get(self):
        try:
            limit = int(request.args.get('limit'))
        except TypeError:
            limit = DEFAULT_LIMIT

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
            c = add_relationship_data(c)
            result.append(c)

        return result


class Relationship(restful.Resource):
    def get(self, uuid):
        c = relationships.get(uuid)

        if not c:
            abort(404)

        return add_relationship_data(c)

    def put(self, uuid):
        c = relationships.get(uuid)

        if not c:
            abort(404)

        c = relationships.update(c, request.json)

        if request.args.get('quiet') == '1':
            return '', 204

        return add_relationship_data(c), 200


class RelationshipRevisions(restful.Resource):
    def get(self, uuid):
        c = relationships.get(uuid)

        if not c:
            abort(404)

        cursor = relationships.revisions(c)

        result = []

        for r in cursor:
            r = add_relationship_data(r)
            result.append(r)

        return result


class RelationshipRevision(restful.Resource):
    def get(self, uuid):
        c = relationships.revision(uuid)

        if not c:
            abort(404)

        return add_relationship_data(c)


class RelationshipTimeline(restful.Resource):
    def get(self, uuid):
        c = relationships.revision(uuid)

        if not c:
            abort(404)

        return relationships.timeline(c)


class Collections(restful.Resource):
    def get(self):
        try:
            limit = int(request.args.get('limit'))
        except TypeError:
            limit = DEFAULT_LIMIT

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
            c = add_collection_data(c)
            result.append(c)

        return result

    def post(self):
        c = collections.create(request.json)

        return add_collection_data(c), 201


class Collection(restful.Resource):
    def get(self, uuid):
        c = collections.get(uuid)

        if not c:
            abort(404)

        cursor = collections.resources(c)
        result = []

        for r in cursor:
            r = add_resource_data(r)
            result.append(r)

        c['resources'] = result

        return add_collection_data(c)

    def put(self, uuid):
        c = collections.get(uuid)

        if not c:
            abort(404)

        c = collections.update(c, request.json)

        if request.args.get('quiet') == '1':
            return '', 204

        return add_collection_data(c), 200

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
            r = add_resource_data(r)
            result.append(r)

        return result


class Trends(restful.Resource):
    def get(self):
        return {
            'title': 'Origins Service API - Trends',
            'version': 1.0,

            '_links': {
                'connected_components': {
                    'href': url_for('trend-components-connected',
                                    _external=True),
                },
                'used_components': {
                    'href': url_for('trend-used-components',
                                    _external=True),
                },
                'used_resources': {
                    'href': url_for('trend-used-resources',
                                    _external=True),
                },
                'connected_resources': {
                    'href': url_for('trend-connected-resources',
                                    _external=True),
                },
                'component_sources': {
                    'href': url_for('trend-component-sources',
                                    _external=True),
                },
                'common_relationships': {
                    'href': url_for('trend-common-relationships',
                                    _external=True),
                },
            }
        }


class ConnectedComponentsTrend(restful.Resource):
    def get(self):
        cursor = trends.connected_components()

        result = []

        for r in cursor:
            add_component_data(r['component'])
            result.append(r)

        return result


class UsedComponentsTrend(restful.Resource):
    def get(self):
        cursor = trends.used_components()

        result = []

        for r in cursor:
            add_component_data(r['component'])
            result.append(r)

        return result


class ConnectedResourcesTrend(restful.Resource):
    def get(self):
        cursor = trends.connected_resources()

        result = []

        for r in cursor:
            add_resource_data(r['resource'])
            result.append(r)

        return result


class UsedResourcesTrend(restful.Resource):
    def get(self):
        cursor = trends.used_resources()

        result = []

        for r in cursor:
            add_resource_data(r['resource'])
            result.append(r)

        return result


class ComponentSourcesTrend(restful.Resource):
    def get(self):
        cursor = trends.component_sources()

        result = []

        for r in cursor:
            add_component_data(r['component'])
            result.append(r)

        return result


class CommonRelationshipsTrend(restful.Resource):
    def get(self):
        return trends.common_relationships()


class Root(restful.Resource):
    def get(self):
        return {
            'title': 'Origins Service API',
            'version': 1.0,

            '_links': {
                'resources': {
                    'href': url_for('resources', _external=True),
                },
                'components': {
                    'href': url_for('components', _external=True),
                },
                'collections': {
                    'href': url_for('collections', _external=True),
                },
                'relationships': {
                    'href': url_for('relationships', _external=True),
                },
                'trends': {
                    'href': url_for('trends', _external=True),
                },

                # Templates for single objects
                'resource': {
                    'href': unquote(url_for('resource', uuid='{uuid}',
                                            _external=True)),
                },
                'component': {
                    'href': unquote(url_for('component', uuid='{uuid}',
                                            _external=True)),
                },
                'collection': {
                    'href': unquote(url_for('collection', uuid='{uuid}',
                                            _external=True)),
                },
                'relationship': {
                    'href': unquote(url_for('relationship', uuid='{uuid}',
                                            _external=True)),
                },
            }
        }


app = Flask(__name__)
api = restful.Api(app)

api.decorators = [
    cross_origin(supports_credentials=True)
]


@api.representation('application/json')
def json_representation(data, code, headers=None):
    if data is None:
        data = ''
    elif not isinstance(data, str):
        data = json.dumps(data)

    response = make_response(data, code)

    if headers:
        response.headers.update(headers)

    return response


api.add_resource(Root,
                 '/',
                 endpoint='root')

api.add_resource(Resources,
                 '/resources/',
                 endpoint='resources')

api.add_resource(Resource,
                 '/resources/<uuid>/',
                 endpoint='resource')

api.add_resource(ResourceComponents,
                 '/resources/<uuid>/components/',
                 endpoint='resource-components')

api.add_resource(ResourceRelationships,
                 '/resources/<uuid>/relationships/',
                 endpoint='resource-relationships')

api.add_resource(ResourceTimeline,
                 '/resources/<uuid>/timeline/',
                 endpoint='resource-timeline'),

api.add_resource(Collections,
                 '/collections/',
                 endpoint='collections')

api.add_resource(Collection,
                 '/collections/<uuid>/',
                 endpoint='collection')

api.add_resource(CollectionResources,
                 '/collections/<uuid>/resources/',
                 endpoint='collection-resources')

api.add_resource(Components,
                 '/components/',
                 endpoint='components')

api.add_resource(Component,
                 '/components/<uuid>/',
                 endpoint='component')

api.add_resource(ComponentSources,
                 '/components/<uuid>/sources/',
                 endpoint='component-sources')

api.add_resource(ComponentChildren,
                 '/components/<uuid>/children/',
                 endpoint='component-children')

api.add_resource(ComponentLineage,
                 '/components/<uuid>/lineage/',
                 endpoint='component-lineage')

api.add_resource(ComponentRevisions,
                 '/components/<uuid>/revisions/',
                 endpoint='component-revisions')

api.add_resource(ComponentRevision,
                 '/components/revisions/<uuid>/',
                 endpoint='component-revision')

api.add_resource(ComponentTimeline,
                 '/components/<uuid>/timeline/',
                 endpoint='component-timeline')

api.add_resource(ComponentRelationships,
                 '/components/<uuid>/relationships/',
                 endpoint='component-relationships')

api.add_resource(Relationships,
                 '/relationships/',
                 endpoint='relationships')

api.add_resource(Relationship,
                 '/relationships/<uuid>/',
                 endpoint='relationship')

api.add_resource(RelationshipRevisions,
                 '/relationships/<uuid>/revisions/',
                 endpoint='relationship-revisions')

api.add_resource(RelationshipRevision,
                 '/relationships/revisions/<uuid>/',
                 endpoint='relationship-revision')

api.add_resource(RelationshipTimeline,
                 '/relationships/<uuid>/timeline/',
                 endpoint='relationship-timeline')

api.add_resource(Trends,
                 '/trends/',
                 endpoint='trends')

api.add_resource(ConnectedComponentsTrend,
                 '/trends/connected-components/',
                 endpoint='trend-components-connected')

api.add_resource(UsedComponentsTrend,
                 '/trends/used-components/',
                 endpoint='trend-used-components')

api.add_resource(ConnectedResourcesTrend,
                 '/trends/connected-resources/',
                 endpoint='trend-connected-resources')

api.add_resource(UsedResourcesTrend,
                 '/trends/used-resources/',
                 endpoint='trend-used-resources')

api.add_resource(ComponentSourcesTrend,
                 '/trends/component-sources/',
                 endpoint='trend-component-sources')

api.add_resource(CommonRelationshipsTrend,
                 '/trends/common-relationships/',
                 endpoint='trend-common-relationships')

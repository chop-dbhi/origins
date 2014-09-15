from flask import url_for
from flask.ext import restful
from origins.graph import trends
from .components import ComponentResource
from .resources import ResourceResource


class Trends(restful.Resource):
    def get(self):
        return {
            'version': 1.0,
            'title': 'Origins Trends API',
            'links': {
                'self': {
                    'href': url_for('trends', _external=True),
                },
                'connected_components': {
                    'href': url_for('trend-connected-components',
                                    _external=True),
                },
                'used_components': {
                    'href': url_for('trend-used-components',
                                    _external=True),
                },
                'connected_resources': {
                    'href': url_for('trend-connected-resources',
                                    _external=True),
                },
                'used_resources': {
                    'href': url_for('trend-used-resources',
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


class ConnectedComponents(restful.Resource):
    def get(self):
        cursor = trends.connected_components()

        result = []
        handler = ComponentResource()

        for r in cursor:
            r['component'] = handler.prepare(r['component'])
            result.append(r)

        return result


class UsedComponents(restful.Resource):
    def get(self):
        cursor = trends.used_components()

        result = []
        handler = ComponentResource()

        for r in cursor:
            r['component'] = handler.prepare(r['component'])
            result.append(r)

        return result


class ConnectedResources(restful.Resource):
    def get(self):
        cursor = trends.connected_resources()

        result = []
        handler = ResourceResource()

        for r in cursor:
            r['resource'] = handler.prepare(r['resource'])
            result.append(r)

        return result


class UsedResources(restful.Resource):
    def get(self):
        cursor = trends.used_resources()

        result = []
        handler = ResourceResource()

        for r in cursor:
            r['resource'] = handler.prepare(r['resource'])
            result.append(r)

        return result


class ComponentSources(restful.Resource):
    def get(self):
        cursor = trends.component_sources()

        result = []
        handler = ComponentResource()

        for r in cursor:
            r['component'] = handler.prepare(r['component'])
            result.append(r)

        return result


class CommonRelationships(restful.Resource):
    def get(self):
        return trends.common_relationships()

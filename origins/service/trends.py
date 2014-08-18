from flask import url_for
from flask.ext import restful
from origins.graph import trends
from . import utils


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
            utils.add_component_data(r['component'])
            result.append(r)

        return result


class UsedComponentsTrend(restful.Resource):
    def get(self):
        cursor = trends.used_components()

        result = []

        for r in cursor:
            utils.add_component_data(r['component'])
            result.append(r)

        return result


class ConnectedResourcesTrend(restful.Resource):
    def get(self):
        cursor = trends.connected_resources()

        result = []

        for r in cursor:
            utils.add_resource_data(r['resource'])
            result.append(r)

        return result


class UsedResourcesTrend(restful.Resource):
    def get(self):
        cursor = trends.used_resources()

        result = []

        for r in cursor:
            utils.add_resource_data(r['resource'])
            result.append(r)

        return result


class ComponentSourcesTrend(restful.Resource):
    def get(self):
        cursor = trends.component_sources()

        result = []

        for r in cursor:
            utils.add_component_data(r['component'])
            result.append(r)

        return result


class CommonRelationshipsTrend(restful.Resource):
    def get(self):
        return trends.common_relationships()

import json
from flask import Flask, make_response
from flask.ext import restful
from flask_cors import CORS
from origins import config
from origins.graph.model import Node
from . import root, nodes, resources, components


app = Flask(__name__)
cors = CORS(app, headers='Content-Type')

api = restful.Api(app)


def json_node_encoder(o):
    if isinstance(o, Node):
        return o.to_dict()
    raise ValueError


@api.representation('application/json')
def json_representation(data, code, headers=None):
    if data is None:
        data = ''
    elif not isinstance(data, str):
        indent = 4 if config.options['debug'] else None
        data = json.dumps(data, indent=indent, default=json_node_encoder)

    response = make_response(data, code)

    if headers:
        response.headers.update(headers)

    return response


api.add_resource(root.Root,
                 '/',
                 endpoint='root')

api.add_resource(resources.Resources,
                 '/resources/',
                 endpoint='resources')

api.add_resource(resources.Resource,
                 '/resources/<uuid>/',
                 endpoint='resource')

api.add_resource(resources.ResourceComponents,
                 '/resources/<uuid>/components/',
                 endpoint='resource-components')

api.add_resource(components.Components,
                 '/components/',
                 endpoint='components')

api.add_resource(components.Component,
                 '/components/<uuid>/',
                 endpoint='component')

api.add_resource(nodes.Nodes,
                 '/nodes/',
                 endpoint='nodes')

api.add_resource(nodes.Node,
                 '/nodes/<uuid>/',
                 endpoint='node')

# api.add_resource(resources.ResourceRelationships,
#                  '/resources/<uuid>/relationships/',
#                  endpoint='resource-relationships')
#
# api.add_resource(resources.ResourceTimeline,
#                  '/resources/<uuid>/timeline/',
#                  endpoint='resource-timeline'),
#
# api.add_resource(collections.Collections,
#                  '/collections/',
#                  endpoint='collections')
#
# api.add_resource(collections.Collection,
#                  '/collections/<uuid>/',
#                  endpoint='collection')
#
# api.add_resource(collections.CollectionResources,
#                  '/collections/<uuid>/resources/',
#                  endpoint='collection-resources')
#
# api.add_resource(components.ComponentSources,
#                  '/components/<uuid>/sources/',
#                  endpoint='component-sources')
#
# api.add_resource(components.ComponentChildren,
#                  '/components/<uuid>/children/',
#                  endpoint='component-children')
#
# api.add_resource(components.ComponentLineage,
#                  '/components/<uuid>/lineage/',
#                  endpoint='component-lineage')
#
# api.add_resource(components.ComponentRevisions,
#                  '/components/<uuid>/revisions/',
#                  endpoint='component-revisions')
#
# api.add_resource(components.ComponentRevision,
#                  '/components/revisions/<uuid>/',
#                  endpoint='component-revision')
#
# api.add_resource(components.ComponentTimeline,
#                  '/components/<uuid>/timeline/',
#                  endpoint='component-timeline')
#
# api.add_resource(components.ComponentRelationships,
#                  '/components/<uuid>/relationships/',
#                  endpoint='component-relationships')
#
# api.add_resource(relationships.Relationships,
#                  '/relationships/',
#                  endpoint='relationships')
#
# api.add_resource(relationships.Relationship,
#                  '/relationships/<uuid>/',
#                  endpoint='relationship')
#
# api.add_resource(relationships.RelationshipRevisions,
#                  '/relationships/<uuid>/revisions/',
#                  endpoint='relationship-revisions')
#
# api.add_resource(relationships.RelationshipRevision,
#                  '/relationships/revisions/<uuid>/',
#                  endpoint='relationship-revision')
#
# api.add_resource(relationships.RelationshipTimeline,
#                  '/relationships/<uuid>/timeline/',
#                  endpoint='relationship-timeline')
#
# api.add_resource(trends.Trends,
#                  '/trends/',
#                  endpoint='trends')
#
# api.add_resource(trends.ConnectedComponentsTrend,
#                  '/trends/connected-components/',
#                  endpoint='trend-components-connected')
#
# api.add_resource(trends.UsedComponentsTrend,
#                  '/trends/used-components/',
#                  endpoint='trend-used-components')
#
# api.add_resource(trends.ConnectedResourcesTrend,
#                  '/trends/connected-resources/',
#                  endpoint='trend-connected-resources')
#
# api.add_resource(trends.UsedResourcesTrend,
#                  '/trends/used-resources/',
#                  endpoint='trend-used-resources')
#
# api.add_resource(trends.ComponentSourcesTrend,
#                  '/trends/component-sources/',
#                  endpoint='trend-component-sources')
#
# api.add_resource(trends.CommonRelationshipsTrend,
#                  '/trends/common-relationships/',
#                  endpoint='trend-common-relationships')

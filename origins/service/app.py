import json
from flask import Flask, make_response
from flask.ext import restful
from flask_cors import CORS
from origins import config
from origins.graph.model import Model
from . import (root, nodes, edges, resources, components,
               collections, relationships, trends)


app = Flask(__name__)
cors = CORS(app, headers='Content-Type')

api = restful.Api(app)


def json_node_encoder(o):
    if isinstance(o, Model):
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


routes = [
    (root.RootResource,
     '/',
     'root'),

    (nodes.NodesResource,
     '/nodes/', 'nodes'),

    (nodes.NodeResource,
     '/nodes/<uuid>/',
     'node'),

    (edges.EdgesResource,
     '/edges/',
     'edges'),

    (edges.EdgeResource,
     '/edges/<uuid>/',
     'edge'),

    (resources.ResourcesResource,
     '/resources/',
     'resources'),

    (resources.ResourceSyncResource,
     '/resources/sync/',
     'resource-sync'),

    (resources.ResourceResource,
     '/resources/<uuid>/',
     'resource'),

    (resources.ResourceComponentsResource,
     '/resources/<uuid>/components/',
     'resource-components'),

    (resources.ResourceRelationshipsResource,
     '/resources/<uuid>/relationships/',
     'resource-relationships'),

    (components.ComponentsResource,
     '/components/',
     'components'),

    (components.ComponentResource,
     '/components/<uuid>/',
     'component'),

    (relationships.RelationshipsResource,
     '/relationships/',
     'relationships'),

    (relationships.RelationshipResource,
     '/relationships/<uuid>/',
     'relationship'),

    (collections.CollectionsResource,
     '/collections/',
     'collections'),

    (collections.CollectionResource,
     '/collections/<uuid>/',
     'collection'),

    (collections.CollectionResourcesResource,
     '/collections/<uuid>/resources/',
     'collection-resources'),

    (trends.Trends,
     '/trends/',
     'trends'),

    (trends.ConnectedComponents,
     '/trends/connected-components/',
     'trend-connected-components'),

    (trends.UsedComponents,
     '/trends/used-components/',
     'trend-used-components'),

    (trends.ConnectedResources,
     '/trends/connected-resources/',
     'trend-connected-resources'),

    (trends.UsedResources,
     '/trends/used-resources/',
     'trend-used-resources'),

    (trends.ComponentSources,
     '/trends/component-sources/',
     'trend-component-sources'),

    (trends.ResourceTypes,
     '/trends/resource-types/',
     'trend-resource-types'),

    (trends.ComponentTypes,
     '/trends/component-types/',
     'trend-component-types'),

    (trends.RelationshipTypes,
     '/trends/relationship-types/',
     'trend-relationship-types'),
]

for resource, path, name in routes:
    api.add_resource(resource, path, endpoint=name)

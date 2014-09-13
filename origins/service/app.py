import json
from flask import Flask, make_response
from flask.ext import restful
from flask_cors import CORS
from origins import config
from origins.graph.model import Model
from . import (root, nodes, edges, resources, components,
               collections, relationships)


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


api.add_resource(root.RootResource,
                 '/',
                 endpoint='root')

api.add_resource(nodes.NodesResource,
                 '/nodes/',
                 endpoint='nodes')

api.add_resource(nodes.NodeResource,
                 '/nodes/<uuid>/',
                 endpoint='node')

api.add_resource(edges.EdgesResource,
                 '/edges/',
                 endpoint='edges')

api.add_resource(edges.EdgeResource,
                 '/edges/<uuid>/',
                 endpoint='edge')

api.add_resource(resources.ResourcesResource,
                 '/resources/',
                 endpoint='resources')

api.add_resource(resources.ResourceResource,
                 '/resources/<uuid>/',
                 endpoint='resource')

api.add_resource(resources.ResourceComponentsResource,
                 '/resources/<uuid>/components/',
                 endpoint='resource-components')

api.add_resource(resources.ResourceRelationshipsResource,
                 '/resources/<uuid>/relationships/',
                 endpoint='resource-relationships')

api.add_resource(components.ComponentsResource,
                 '/components/',
                 endpoint='components')

api.add_resource(components.ComponentResource,
                 '/components/<uuid>/',
                 endpoint='component')

api.add_resource(relationships.RelationshipsResource,
                 '/relationships/',
                 endpoint='relationships')

api.add_resource(relationships.RelationshipResource,
                 '/relationships/<uuid>/',
                 endpoint='relationship')

api.add_resource(collections.CollectionsResource,
                 '/collections/',
                 endpoint='collections')

api.add_resource(collections.CollectionResource,
                 '/collections/<uuid>/',
                 endpoint='collection')

api.add_resource(collections.CollectionResourcesResource,
                 '/collections/<uuid>/resources/',
                 endpoint='collection-resources')

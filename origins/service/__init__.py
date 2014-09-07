import json
from flask import Flask, make_response
from flask.ext import restful
from flask_cors import CORS
from origins import config
from origins.graph.model import Node
from . import root, nodes, edges, resources, components


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

api.add_resource(nodes.Nodes,
                 '/nodes/',
                 endpoint='nodes')

api.add_resource(nodes.Node,
                 '/nodes/<uuid>/',
                 endpoint='node')

api.add_resource(edges.Edges,
                 '/edges/',
                 endpoint='edges')

api.add_resource(edges.Edge,
                 '/edges/<uuid>/',
                 endpoint='edge')

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

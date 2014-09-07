from flask import url_for
from origins.graph.core import edges
from .nodes import Nodes, Node


def prepare(n):
    n = n.to_dict()

    n['links'] = {
        'self': {
            'href': url_for('edge', uuid=n['uuid'],
                            _external=True),
        },
    }

    return n


class Edges(Nodes):
    module = edges

    def prepare(self, n):
        return prepare(n)

    def get_attrs(self, data):
        return {
            'id': data.get('id'),
            'type': data.get('type'),
            'label': data.get('label'),
            'description': data.get('description'),
            'properties': data.get('properties'),
            'start': data.get('start'),
            'end': data.get('end'),
        }


class Edge(Node):
    module = edges

    def prepare(self, n):
        return prepare(n)

    def get_attrs(self, data):
        return {
            'label': data.get('label'),
            'type': data.get('type'),
            'description': data.get('description'),
            'properties': data.get('properties'),
        }

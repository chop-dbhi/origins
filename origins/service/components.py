from flask import url_for
from origins.graph import Component
from .nodes import NodesResource, NodeResource


def prepare(n, r=None):
    n = n.to_dict()

    if not r:
        r = Component.resource(n['uuid']).uuid

    n['links'] = {
        'self': {
            'href': url_for('component', uuid=n['uuid'],
                            _external=True),
        },
        'resource': {
            'href': url_for('resource', uuid=r, _external=True)
        },
    }

    return n


class ComponentsResource(NodesResource):
    model = Component

    def prepare(self, n, resource=None):
        return prepare(n, r=resource)

    def get_attrs(self, data):
        resource = data.get('resource')

        if isinstance(resource, str):
            resource = {'uuid': resource}

        return {
            'id': data.get('id'),
            'type': data.get('type'),
            'label': data.get('label'),
            'description': data.get('description'),
            'properties': data.get('properties'),
            'resource': resource,
        }


class ComponentResource(NodeResource):
    model = Component

    def prepare(self, n, resource=None):
        return prepare(n, r=resource)

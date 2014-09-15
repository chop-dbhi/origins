from flask import url_for
from origins.exceptions import ValidationError
from origins.graph import Component, Resource
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
        if not data or not data.get('resource'):
            raise ValidationError('resource required')

        attrs = super(ComponentsResource, self).get_attrs(data)

        attrs['resource'] = Resource(uuid=data['resource'])

        return attrs


class ComponentResource(NodeResource):
    model = Component

    def prepare(self, n, resource=None):
        return prepare(n, r=resource)

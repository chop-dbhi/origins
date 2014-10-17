from flask import url_for
from flask.ext import restful
from origins.exceptions import DoesNotExist, ValidationError
from origins.graph import Component, Resource, generic
from .nodes import NodesResource, NodeResource, NodeRevisionsResource


def prepare(n, r=None):
    n = n.to_dict()

    if not r:
        r = Component.resource(n['uuid']).to_dict()

    n['resource'] = r

    n['links'] = {
        'self': {
            'href': url_for('component', uuid=n['uuid'],
                            _external=True),
        },
        'revisions': {
            'href': url_for('component-revisions', uuid=n['uuid'],
                            _external=True)
        },
        'relationships': {
            'href': url_for('component-relationships', uuid=n['uuid'],
                            _external=True)
        },
        'resource': {
            'href': url_for('resource', uuid=r['uuid'], _external=True)
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


class ComponentRevisionsResource(NodeRevisionsResource):
    model = Component

    def prepare(self, n):
        return prepare(n)


class ComponentRelationshipsResource(restful.Resource):
    model = Component

    def prepare(self, r):
        from . import relationships
        return relationships.prepare(r)

    def get(self, uuid):
        try:
            n = self.model.get(uuid)
        except DoesNotExist as e:
            return {'message': str(e)}, 404

        edges = generic.edges(n)

        return [self.prepare(r) for r in edges], 200

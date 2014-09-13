from flask import url_for
from origins.exceptions import ValidationError
from origins.graph import Relationship, Resource
from .nodes import NodesResource, NodeResource


def prepare(n, r=None):
    n = n.to_dict()

    if not r:
        r = Relationship.resource(n['uuid']).uuid

    n['links'] = {
        'self': {
            'href': url_for('relationship', uuid=n['uuid'],
                            _external=True),
        },
        'resource': {
            'href': url_for('resource', uuid=r, _external=True)
        },
    }

    return n


class RelationshipsResource(NodesResource):
    model = Relationship

    def prepare(self, n, resource=None):
        return prepare(n, r=resource)

    def get_attrs(self, data):
        resource = data.get('resource')
        start = data.get('start')
        end = data.get('end')

        if not resource:
            raise ValidationError('resource required')

        if not start:
            raise ValidationError('start component required')

        if not end:
            raise ValidationError('end component required')

        resource = Resource(uuid=resource)
        start = Relationship.start_model(uuid=start)
        end = Relationship.end_model(uuid=end)

        return {
            'resource': resource,
            'start': start,
            'end': end,
            'id': data.get('id'),
            'type': data.get('type'),
            'label': data.get('label'),
            'description': data.get('description'),
            'properties': data.get('properties'),
            'dependence': data.get('dependence'),
            'direction': data.get('direction'),
        }


class RelationshipResource(NodeResource):
    model = Relationship

    def prepare(self, n, resource=None):
        return prepare(n, r=resource)

    def get_attrs(self, data):
        return {
            'label': data.get('label'),
            'type': data.get('type'),
            'description': data.get('description'),
            'properties': data.get('properties'),
            'dependence': data.get('dependence'),
            'direction': data.get('direction'),
        }

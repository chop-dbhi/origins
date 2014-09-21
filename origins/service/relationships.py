from flask import url_for
from origins.exceptions import ValidationError
from origins.graph import Relationship, Resource
from .edges import EdgesResource, EdgeResource


def prepare(n, r=None):
    n = n.to_dict()

    if not r:
        r = Relationship.resource(n['uuid']).to_dict()

    n['resource'] = r

    n['links'] = {
        'self': {
            'href': url_for('relationship', uuid=n['uuid'],
                            _external=True),
        },
        'resource': {
            'href': url_for('resource', uuid=r['uuid'], _external=True)
        },
    }

    return n


class RelationshipsResource(EdgesResource):
    model = Relationship

    def prepare(self, n, resource=None):
        return prepare(n, r=resource)

    def get_attrs(self, data):
        if not data or not data.get('resource'):
            raise ValidationError('resource required')

        if not data or not data.get('start'):
            raise ValidationError('start component required')

        if not data or not data.get('end'):
            raise ValidationError('end component required')

        # Parent, i.e. nodes
        attrs = super(EdgesResource, self).get_attrs(data)

        attrs['resource'] = Resource(uuid=data['resource'])
        attrs['start'] = Relationship.start_model(uuid=data['start'])
        attrs['end'] = Relationship.end_model(uuid=data['end'])

        return attrs


class RelationshipResource(EdgeResource):
    model = Relationship

    def prepare(self, n, resource=None):
        return prepare(n, r=resource)

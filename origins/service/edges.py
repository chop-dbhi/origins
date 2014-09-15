from flask import url_for
from origins.exceptions import ValidationError
from origins.graph import Edge, Node
from .nodes import NodesResource, NodeResource


def prepare(n):
    n = n.to_dict()

    n['links'] = {
        'self': {
            'href': url_for('edge', uuid=n['uuid'],
                            _external=True),
        },
    }

    return n


class EdgesResource(NodesResource):
    model = Edge

    attr_keys = (
        'id',
        'type',
        'label',
        'description',
        'properties',
        'dependence',
        'direction',
    )

    def prepare(self, n):
        return prepare(n)

    def get_attrs(self, data):
        if not data or not data.get('start'):
            raise ValidationError('start node required')

        if not data or not data.get('end'):
            raise ValidationError('end node required')

        attrs = super(EdgesResource, self).get_attrs(data)

        attrs['start'] = Node(uuid=data['start'])
        attrs['end'] = Node(uuid=data['end'])

        return attrs


class EdgeResource(NodeResource):
    model = Edge

    attr_keys = (
        'type',
        'label',
        'description',
        'properties',
        'dependence',
        'direction',
    )

    def prepare(self, n):
        return prepare(n)

from flask import url_for
from origins.exceptions import ValidationError
from origins.graph import Edge, Node
from . import nodes


def prepare(n):
    s = nodes.prepare(n.start)
    e = nodes.prepare(n.end)

    n = n.to_dict()
    n['start'] = s
    n['end'] = e

    n['links'] = {
        'self': {
            'href': url_for('edge', uuid=n['uuid'],
                            _external=True),
        },
        'start': {
            'href': url_for('node', uuid=s['uuid'],
                            _external=True),
        },
        'end': {
            'href': url_for('node', uuid=e['uuid'],
                            _external=True),
        },
    }

    return n


class EdgesResource(nodes.NodesResource):
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

        attrs['start'] = Node.get(data['start'])
        attrs['end'] = Node.get(data['end'])

        return attrs


class EdgeResource(nodes.NodeResource):
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

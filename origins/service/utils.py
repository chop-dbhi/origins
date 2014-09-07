from flask import url_for


DEFAULT_PAGE_LIMIT = 30


def add_node_data(n):
    n = n.to_dict()

    n['links'] = {
        'self': {
            'href': url_for('node', uuid=n['uuid'], _external=True),
        },
    }

    return n


def add_resource_data(n):
    n = n.to_dict()

    n['links'] = {
        'self': {
            'href': url_for('resource', uuid=n['uuid'],
                            _external=True),
        },
        'components': {
            'href': url_for('resource-components', uuid=n['uuid'],
                            _external=True),
        },
    }

    return n


def add_component_data(n):
    n = n.to_dict()

    n['_links'] = {
        'self': {
            'href': url_for('component', uuid=n['uuid'],
                            _external=True),
        },
    }

    return n


def add_relationship_data(n):
    uuid = n['uuid']

    n['_links'] = {
        'self': {
            'href': url_for('relationship', uuid=uuid,
                            _external=True),
        },
    }

    return n

from flask import url_for


DEFAULT_PAGE_LIMIT = 30


def add_relationship_data(n):
    uuid = n['uuid']

    n['_links'] = {
        'self': {
            'href': url_for('relationship', uuid=uuid,
                            _external=True),
        },
    }

    return n

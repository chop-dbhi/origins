from flask import url_for
from origins.graph import resources, components, collections, relationships


DEFAULT_PAGE_LIMIT = 30


def add_collection_data(c):
    uuid = c['uuid']

    c['resource_count'] = collections.resource_count(c)

    c['_links'] = {
        'self': {
            'href': url_for('collection', uuid=uuid,
                            _external=True),
        },
        'resources': {
            'href': url_for('collection-resources', uuid=uuid,
                            _external=True),
        }
    }

    return c


def add_resource_data(r):
    uuid = r['uuid']

    r['component_types'] = resources.component_types(r)
    r['component_count'] = resources.component_count(r)
    r['relationship_count'] = resources.relationship_count(r)
    r['collection_count'] = resources.collection_count(r)

    r['_links'] = {
        'self': {
            'href': url_for('resource', uuid=uuid,
                            _external=True),
        },
        'components': {
            'href': url_for('resource-components', uuid=uuid,
                            _external=True),
        },
        'relationships': {
            'href': url_for('resource-relationships', uuid=uuid,
                            _external=True),
        },
        'timeline': {
            'href': url_for('resource-timeline', uuid=uuid,
                            _external=True),
        }
    }

    return r


def add_component_data(c):
    uuid = c['uuid']

    if 'resource' in c:
        r = c['resource']
    else:
        r = components.resource(c)
        c['resource'] = r

    c['relationship_count'] = components.relationship_count(c)
    c['resource_count'] = components.resource_count(c)
    c['path'] = components.path(c)

    c['_links'] = {
        'self': {
            'href': url_for('component-revision', uuid=uuid,
                            _external=True),
        },
        'relationships': {
            'href': url_for('component-relationships', uuid=uuid,
                            _external=True),
        },
        'revisions': {
            'href': url_for('component-revisions', uuid=uuid,
                            _external=True),
        },
        'children': {
            'href': url_for('component-children', uuid=uuid,
                            _external=True),
        },
        'sources': {
            'href': url_for('component-sources', uuid=uuid,
                            _external=True),
        },
        'lineage': {
            'href': url_for('component-lineage', uuid=uuid,
                            _external=True),
        },
        'timeline': {
            'href': url_for('component-timeline', uuid=uuid,
                            _external=True)
        },
        'resource': {
            'href': url_for('resource', uuid=r['uuid'],
                            _external=True)
        },
    }

    return c


def add_relationship_data(c):
    uuid = c['uuid']

    r = relationships.resource(c)

    c['_links'] = {
        'self': {
            'href': url_for('relationship-revision', uuid=uuid,
                            _external=True),
        },
        'revisions': {
            'href': url_for('relationship-revisions', uuid=uuid,
                            _external=True),
        },
        'timeline': {
            'href': url_for('relationship-timeline', uuid=uuid,
                            _external=True)
        },
        'resource': {
            'href': url_for('resource', uuid=r['uuid'],
                            _external=True)
        },
    }

    return c

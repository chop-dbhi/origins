from __future__ import unicode_literals, absolute_import

from ..parsers import parse_component, parse_resource
from .. import neo4j
from . import queries

try:
    str = unicode
except NameError:
    pass


def connected_components(limit=10, tx=neo4j):
    query = {
        'statement': queries.CONNECTED_COMPONENTS,
        'parameters': {
            'limit': limit,
        }
    }

    return [{
        'component': parse_component(r[:-1]),
        'count': r[-1]
    } for r in tx.send(query)]


def used_components(limit=10, tx=neo4j):
    query = {
        'statement': queries.USED_COMPONENTS,
        'parameters': {
            'limit': limit,
        }
    }

    return [{
        'component': parse_component(r[:-1]),
        'count': r[-1]
    } for r in tx.send(query)]


def connected_resources(limit=10, tx=neo4j):
    query = {
        'statement': queries.CONNECTED_RESOURCES,
        'parameters': {
            'limit': limit,
        }
    }

    return [{
        'resource': parse_resource(r[:-1]),
        'count': r[-1]
    } for r in tx.send(query)]


def used_resources(limit=10, tx=neo4j):
    query = {
        'statement': queries.USED_RESOURCES,
        'parameters': {
            'limit': limit,
        }
    }

    return [{
        'resource': parse_resource(r[:-1]),
        'count': r[-1]
    } for r in tx.send(query)]


def component_sources(limit=10, tx=neo4j):
    query = {
        'statement': queries.COMPONENT_SOURCES,
        'parameters': {
            'limit': limit,
        }
    }

    return [{
        'component': parse_component(r[:-1]),
        'count': r[-1]
    } for r in tx.send(query)]


def common_relationships(limit=10, tx=neo4j):
    query = {
        'statement': queries.COMMON_RELATIONSHIPS,
        'parameters': {
            'limit': limit,
        }
    }

    return [{
        'type': r[0],
        'count': r[1],
    } for r in tx.send(query)]

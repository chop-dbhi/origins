from . import neo4j
from . import Resource, Component


CONNECTED_COMPONENTS = '''
MATCH (cmp:`origins:Component`)<-[con:`origins:start`|`origins:end`]-(:`origins:Relationship`)
WHERE NOT (cmp)<-[:`prov:entity`]-(:`prov:Invalidation`)
RETURN cmp, count(con) as cnt
ORDER BY cnt DESC
LIMIT { limit }
'''  # noqa


USED_COMPONENTS = '''
MATCH (cmp:`origins:Component`)<-[con:`includes`]-(res:`origins:Resource`)
WHERE NOT (cmp)<-[:`prov:entity`]-(:`prov:Invalidation`)
    AND NOT (res)-[:`manages`]->(cmp)
RETURN cmp, count(con) as cnt
ORDER BY cnt DESC
LIMIT { limit }
'''  # noqa


CONNECTED_RESOURCES = '''
MATCH (res:`origins:Resource`)-[:`manages`]->(rel:`origins:Relationship`)
WHERE NOT (rel)<-[:`prov:entity`]-(:`prov:Invalidation`)
RETURN res, count(rel) as cnt
ORDER BY cnt DESC
LIMIT { limit }
'''  # noqa


USED_RESOURCES = '''
MATCH (res:`origins:Resource`)-[:`includes`]->(cmp:`origins:Component`)
WHERE NOT (res)<-[:`prov:entity`]-(:`prov:Invalidation`)
    AND NOT (res)-[:`manages`]->(cmp)
WITH cmp
MATCH (cmp)<-[:`manages`]-(res:`origins:Resource`)
RETURN res, count(cmp) as cnt
ORDER BY cnt DESC
LIMIT { limit }
'''  # noqa


COMPONENT_SOURCES = '''
MATCH (cmp:`origins:Component`)<-[con:`prov:usedEntity`]-(:`prov:Derivation` {`prov:type`: 'prov:PrimarySource'})
WHERE NOT (cmp)<-[:`prov:entity`]-(:`prov:Invalidation`)
RETURN cmp, count(con) as cnt
ORDER BY cnt DESC
LIMIT { limit }
'''  # noqa


COMMON_RELATIONSHIPS = '''
MATCH (rel:`origins:Relationship`)
WHERE NOT (rel)<-[:`prov:entity`]-(:`prov:Invalidation`)
RETURN rel.`origins:type`, count(rel.`origins:type`) as cnt
ORDER BY cnt DESC
LIMIT { limit }
'''  # noqa


def connected_components(limit=10, tx=neo4j.tx):
    query = {
        'statement': CONNECTED_COMPONENTS,
        'parameters': {
            'limit': limit,
        }
    }

    return [{
        'component': Component.parse(r[0]),
        'count': r[1]
    } for r in tx.send(query)]


def used_components(limit=10, tx=neo4j.tx):
    query = {
        'statement': USED_COMPONENTS,
        'parameters': {
            'limit': limit,
        }
    }

    return [{
        'component': Component.parse(r[0]),
        'count': r[1]
    } for r in tx.send(query)]


def connected_resources(limit=10, tx=neo4j.tx):
    query = {
        'statement': CONNECTED_RESOURCES,
        'parameters': {
            'limit': limit,
        }
    }

    return [{
        'resource': Resource.parse(r[0]),
        'count': r[1]
    } for r in tx.send(query)]


def used_resources(limit=10, tx=neo4j.tx):
    query = {
        'statement': USED_RESOURCES,
        'parameters': {
            'limit': limit,
        }
    }

    return [{
        'resource': Resource.parse(r[0]),
        'count': r[1]
    } for r in tx.send(query)]


def component_sources(limit=10, tx=neo4j.tx):
    query = {
        'statement': COMPONENT_SOURCES,
        'parameters': {
            'limit': limit,
        }
    }

    return [{
        'component': Component.parse(r[0]),
        'count': r[1]
    } for r in tx.send(query)]


def common_relationships(limit=10, tx=neo4j.tx):
    query = {
        'statement': COMMON_RELATIONSHIPS,
        'parameters': {
            'limit': limit,
        }
    }

    return [{
        'type': r[0],
        'count': r[1],
    } for r in tx.send(query)]
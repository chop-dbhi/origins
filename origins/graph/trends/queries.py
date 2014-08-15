from ..utils import _


CONNECTED_COMPONENTS = _('''
MATCH (cmp:`origins:Component`)<-[:`prov:generalEntity`]-(:`prov:Specialization`)-[:`prov:specificEntity`]->(:`origins:ComponentRevision`)<-[con:`origins:start`|`origins:end`]-(:`origins:RelationshipRevision`)

WITH cmp, count(con) as `count`
ORDER BY `count` DESC
LIMIT { limit }

MATCH (cmp)-[:`origins:latest`]-(rev:`origins:ComponentRevision`)

OPTIONAL MATCH (rev)<-[:`prov:entity`]-(inv:`prov:Invalidation`)

RETURN id(cmp), cmp, id(rev), rev, inv IS NULL, `count`
''')  # noqa


USED_COMPONENTS = _('''
MATCH (cmp:`origins:Component`)<-[:`prov:generalEntity`]-(:`prov:Specialization`)-[:`prov:specificEntity`]->(:`origins:ComponentRevision`)<-[con:`origins:includes`]-(res:`origins:Resource`)

WHERE NOT (res)-[:`origins:manages`]->(cmp)

WITH cmp, count(con) as `count`
ORDER BY `count` DESC
LIMIT { limit }

MATCH (cmp)-[:`origins:latest`]-(rev:`origins:ComponentRevision`)

OPTIONAL MATCH (rev)<-[:`prov:entity`]-(inv:`prov:Invalidation`)

RETURN id(cmp), cmp, id(rev), rev, inv IS NULL, `count`
''')  # noqa


CONNECTED_RESOURCES = _('''
MATCH (res:`origins:Resource`)-[:`origins:manages`]->(rel:`origins:Relationship`)

WITH res, count(rel) as `count`
ORDER BY `count` DESC
LIMIT { limit }

RETURN id(res), res, `count`
''')  # noqa


USED_RESOURCES = _('''
MATCH (res:`origins:Resource`)-[:`origins:includes`]->(rev:`origins:ComponentRevision`),
      (rev)<-[:`prov:specificEntity`]-(:`prov:Specialization`)-[:`prov:generalEntity`]->(cmp:`origins:Component`)

WHERE (cmp)<-[:`origins:start`|`origins:end`]-(:`origins:RelationshipRevision`)
      AND NOT (res)-[:`origins:manages`]->(cmp)

WITH res, count(cmp) as `count`
ORDER BY `count` DESC
LIMIT { limit }

RETURN id(res), res, `count`
''')  # noqa


COMPONENT_SOURCES = _('''
MATCH (cmp:`origins:Component`)<-[:`prov:generalEntity`]-(:`prov:Specialization`)-[:`prov:specificEntity`]->(:`origins:ComponentRevision`)<-[con:`prov:usedEntity`]-(:`prov:Derivation` {`prov:type`: 'prov:PrimarySource'})

WITH cmp, count(con) as `count`
ORDER BY `count` DESC
LIMIT { limit }

MATCH (cmp)-[:`origins:latest`]-(rev:`origins:ComponentRevision`)

OPTIONAL MATCH (rev)<-[:`prov:entity`]-(inv:`prov:Invalidation`)

RETURN id(cmp), cmp, id(rev), rev, inv IS NULL, `count`
''')  # noqa


COMMON_RELATIONSHIPS = _('''
MATCH (:`origins:Relationship`)-[:`origins:latest`]->(rev:`origins:RelationshipRevision`)

RETURN rev.`origins:type` as type, count(rev.`origins:type`) as `count`

ORDER BY `count` DESC
LIMIT { limit }
''')  # noqa

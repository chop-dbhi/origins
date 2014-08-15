from ..utils import _


RESOURCE_COUNT = _('''
MATCH (res:`origins:Resource`)
RETURN count(res)
''')


MATCH_RESOURCES = _('''
MATCH (res:`origins:Resource` %(predicate)s)
RETURN DISTINCT id(res), res
''')


SEARCH_RESOURCES = _('''
MATCH (res:`origins:Resource`)
WHERE %(predicate)s
RETURN DISTINCT id(res), res
''')


GET_RESOURCE = _('''
MATCH (res:`origins:Resource` %(predicate)s)
RETURN id(res), res
LIMIT 1
''')


CREATE_RESOURCE = _('''
CREATE (res:`origins:Resource` { properties })
RETURN id(res), res
''')


UPDATE_RESOURCE = _('''
MATCH (res:`origins:Resource` {`origins:id`: { id }})
%(placeholder)s
RETURN id(res), res
''')


DELETE_RESOURCE = _('''
MATCH (res:`origins:Resource` {`origins:id`: { id }})

OPTIONAL MATCH (res)-[inc:`origins:includes`]->()
OPTIONAL MATCH (res)-[man:`origins:manages`]->(cmp)-[rel]-()

DELETE man, inc, rel, cmp

WITH res, cmp

OPTIONAL MATCH (res)-[rel]-()

DELETE rel, res, cmp
''')


RESOURCE_TIMELINE = _('''
MATCH (:`origins:Resource` {`origins:id`: { id }})-[:`origins:manages`]->(cmp),
    (cmp)<-[:`prov:generalEntity`]-(:`prov:Specialization`)-[:`prov:specificEntity`]->(rev),
    (rev)<--(rel:`prov:Relation`)<-[des:`origins:describes`]-(bun:`prov:Bundle`)

OPTIONAL MATCH (rel)-[lnk]->(obj)

RETURN id(rel), rel, type(lnk), id(obj), obj

ORDER BY bun.`origins:timestamp`,
         des.`origins:sequence`
''')  # noqa


# Returns the count of resources
RESOURCE_COUNT = _('''
MATCH (res:`origins:Resource`)
RETURN count(res)
''')

RESOURCE_COMPONENTS = _('''
MATCH (:`origins:Resource` {`origins:id`: { id }})-[:`origins:includes`]->(rev:`origins:ComponentRevision`)

WITH rev

MATCH (rev)<-[:`prov:specificEntity`]-(:`prov:Specialization`)-[:`prov:generalEntity`]->(cmp:`origins:Component`)

OPTIONAL MATCH (rev)<-[:`prov:entity`]-(inv:`prov:Invalidation`)

WITH cmp, rev, inv
WHERE inv IS NULL OR inv.`origins:method` <> 'origins:InvalidByRevision'

RETURN id(cmp), cmp, id(rev), rev, inv is null
ORDER BY rev.`origins:label`
''')  # noqa


RESOURCE_COMPONENT_TYPES = _('''
MATCH (:`origins:Resource` {`origins:id`: { id }})-[:`origins:includes`]->(rev:`origins:ComponentRevision`)
RETURN rev.`origins:type`, count(rev)
''')  # noqa

# Returns all components managed by this resource with the latest revision
RESOURCE_MANAGED_COMPONENTS = _('''
MATCH (:`origins:Resource` {`origins:id`: { id }})-[:`origins:manages`]->(cmp:`origins:Component`)-[:`origins:latest`]->(rev:`prov:Revision`)

OPTIONAL MATCH (rev)<-[:`prov:entity`]-(inv:`prov:Invalidation`)

WITH cmp, rev, inv
WHERE inv IS NULL OR inv.`origins:method` <> 'origins:InvalidByRevision'

RETURN id(cmp), cmp, id(rev), rev, inv is null
''')  # noqa


RESOURCE_RELATIONSHIPS = _('''
MATCH (:`origins:Resource` {`origins:id`: { id }})-[:`origins:includes`]->(r:`origins:RelationshipRevision`)<-[:`prov:specificEntity`]-(:`prov:Specialization`)-[:`prov:generalEntity`]->(c:`origins:Relationship`)

OPTIONAL MATCH (r)<-[:`prov:entity`]-(i:`prov:Invalidation`)

WITH c, r, i
WHERE i IS NULL OR i.`origins:method` <> 'origins:InvalidByRevision'

MATCH (r)-[:`origins:start`]->(sr:`origins:ComponentRevision`),
      (r)-[:`origins:end`]->(er:`origins:ComponentRevision`)

RETURN id(c), c, id(r), r, id(sr), sr, id(er), er, i is null
''')  # noqa

# Returns all relationships managed by this resource with the latest revision
RESOURCE_MANAGED_RELATIONSHIPS = _('''
MATCH (:`origins:Resource` {`origins:id`: { id }})-[:`origins:manages`]->(c:`origins:Relationship`)<-[:`prov:generalEntity`]-(:`prov:Specialization`)-[:`prov:specificEntity`]->(r:`origins:RelationshipRevision`)

OPTIONAL MATCH (r)<-[:`prov:entity`]-(i:`prov:Invalidation`)

WITH c, r, i
WHERE i IS NULL OR i.`origins:method` <> 'origins:InvalidByRevision'

MATCH (r)-[:`origins:start`]->(sr:`origins:ComponentRevision`),
      (r)-[:`origins:end`]->(er:`origins:ComponentRevision`)

RETURN id(c), c, id(r), r, id(sr), sr, id(er), er, i is null
''')  # noqa


RESOURCE_COMPONENT_COUNT = _('''
MATCH (:`origins:Resource` {`origins:id`: { id }})-[:`origins:includes`]->(:`origins:ComponentRevision`)<-[:`prov:specificEntity`]-(:`prov:Specialization`)-[:`prov:generalEntity`]->(cmp:`origins:Component`)
RETURN count(DISTINCT cmp)
''')  # noqa


RESOURCE_RELATIONSHIP_COUNT = _('''
MATCH (:`origins:Resource` {`origins:id`: { id }})-[:`origins:includes`]->(:`origins:RelationshipRevision`)<-[:`prov:specificEntity`]-(:`prov:Specialization`)-[:`prov:generalEntity`]->(rel:`origins:Relationship`)
RETURN count(DISTINCT rel)
''')  # noqa


RESOURCE_INCLUDE_COMPONENT = _('''
MATCH (o:`origins:Resource` {`origins:id`: { id }}),
      (c:`origins:ComponentRevision` {`origins:uuid`: { revision }})
MERGE (o)-[r:`origins:includes`]->(c)
RETURN r is not null
''')  # noqa


RESOURCE_INCLUDE_RELATIONSHIP = _('''
MATCH (o:`origins:Resource` {`origins:id`: { id }}),
      (c:`origins:RelationshipRevision` {`origins:uuid`: { revision }})
MERGE (o)-[r:`origins:includes`]->(c)
RETURN r is not null
''')  # noqa


RESOURCE_COLLECTIONS = _('''
MATCH (:`origins:Resource` {`origins:id`: { id }})<-[:`origins:contains`]-(col:`origins:Collection`)
RETURN id(col), col
''')  # noqa

RESOURCE_COLLECTION_COUNT = _('''
MATCH (:`origins:Resource` {`origins:id`: { id }})<-[:`origins:contains`]-(col:`origins:Collection`)
RETURN count(col)
''')  # noqa

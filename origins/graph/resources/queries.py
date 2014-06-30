from __future__ import unicode_literals, absolute_import

from ..utils import _


RESOURCE_COUNT = _('''
MATCH (res:`origins:Resource`)
RETURN count(res)
''')


MATCH_RESOURCES = _('''
MATCH (res:`origins:Resource` %(predicate)s)
RETURN id(res), res
''')


SEARCH_RESOURCES = _('''
MATCH (res:`origins:Resource`)
WHERE %(predicate)s
RETURN id(res), res
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
MATCH (:`origins:Resource` {`origins:id`: { id }})-[:`origins:includes`]->(:`prov:Revision`)<-[:`origins:describes`]-(b:`prov:Bundle`)

WITH b

MATCH (b)-[d:`origins:describes`]->(e:`prov:Relation`)-[r]->(a)

RETURN id(e), e, type(r), id(a), a
ORDER BY b.`origins:timestamp`, d.`origins:sequence`
''')  # noqa


# Returns the count of resources
RESOURCE_COUNT = _('''
MATCH (res:`origins:Resource`)
RETURN count(res)
''')

RESOURCE_COMPONENTS = _('''
MATCH (:`origins:Resource` {`origins:id`: { id }})-[:`origins:includes`]->(rev:`prov:Revision`)

WITH rev

MATCH (rev)<-[:`prov:specificEntity`]-(:`prov:Specialization`)-[:`prov:generalEntity`]->(cmp:`origins:Component`)

OPTIONAL MATCH (rev)<-[:`prov:entity`]-(inv:`prov:Invalidation`)

WITH cmp, rev, inv
WHERE inv IS NULL OR inv.`origins:method` <> 'origins:InvalidByRevision'

RETURN id(cmp), cmp, id(rev), rev, inv is null
''')  # noqa

# Returns all components managed by this resource with the latest revision
RESOURCE_MANAGED_COMPONENTS = _('''
MATCH (:`origins:Resource` {`origins:id`: { id }})-[:`origins:manages`]->(c:`origins:Component`)<-[:`prov:generalEntity`]-(:`prov:Specialization`)-[:`prov:specificEntity`]->(r)

OPTIONAL MATCH (r)<-[:`prov:entity`]-(i:`prov:Invalidation`)

WITH c, r, i
WHERE i IS NULL OR i.`origins:method` <> 'origins:InvalidByRevision'

RETURN id(c), c, id(r), r, i is null
''')  # noqa


RESOURCE_RELATIONSHIPS = _('''
MATCH (:`origins:Resource` {`origins:id`: { id }})-[:`origins:includes`]->(r:`prov:Revision`)<-[:`prov:specificEntity`]-(:`prov:Specialization`)-[:`prov:generalEntity`]->(c:`origins:Relationship`)

OPTIONAL MATCH (r)<-[:`prov:entity`]-(i:`prov:Invalidation`)

WITH c, r, i
WHERE i IS NULL OR i.`origins:method` <> 'origins:InvalidByRevision'

MATCH (r)-[:`origins:start`]->(sr:`prov:Revision`),
      (r)-[:`origins:end`]->(er:`prov:Revision`)

RETURN id(c), c, id(r), r, id(sr), sr, id(er), er, i is null
''')  # noqa

# Returns all relationships managed by this resource with the latest revision
RESOURCE_MANAGED_RELATIONSHIPS = _('''
MATCH (:`origins:Resource` {`origins:id`: { id }})-[:`origins:manages`]->(c:`origins:Relationship`)<-[:`prov:generalEntity`]-(:`prov:Specialization`)-[:`prov:specificEntity`]->(r:`prov:Revision`)

OPTIONAL MATCH (r)<-[:`prov:entity`]-(i:`prov:Invalidation`)

WITH c, r, i
WHERE i IS NULL OR i.`origins:method` <> 'origins:InvalidByRevision'

MATCH (r)-[:`origins:start`]->(sr:`prov:Revision`),
      (r)-[:`origins:end`]->(er:`prov:Revision`)

RETURN id(c), c, id(r), r, id(sr), sr, id(er), er, i is null
''')  # noqa


RESOURCE_INCLUDE = _('''
MATCH (o:`origins:Resource` {`origins:id`: { id }}),
      (c:`prov:Revision` {`origins:uuid`: { revision }})<-[:`prov:specificEntity`]-()
MERGE (o)-[r:`origins:includes`]->(c)
RETURN r is not null
''')  # noqa

from __future__ import unicode_literals, absolute_import

from ..utils import _


MATCH_COMPONENTS = _('''
MATCH (cmp:`origins:Component`)-[:`origins:latest`]->(rev:`origins:ComponentRevision` %(predicate)s)

WITH DISTINCT cmp, rev

// Get invalidation state if present
OPTIONAL MATCH (rev)<-[:`prov:entity`]-(inv:`prov:Invalidation`)

RETURN id(cmp), cmp, id(rev), rev, inv is null
''')  # noqa

SEARCH_COMPONENTS = _('''
MATCH (cmp:`origins:Component`)-[:`origins:latest`]->(rev:`origins:ComponentRevision`)
WHERE %(predicate)s

WITH DISTINCT cmp, rev

// Get invalidation state if present
OPTIONAL MATCH (rev)<-[:`prov:entity`]-(inv:`prov:Invalidation`)

RETURN id(cmp), cmp, id(rev), rev, inv is null
''')  # noqa


GET_COMPONENT = _('''
// Match component by predicate
MATCH (cmp:`origins:Component`)-[:`origins:latest`]->(rev:`origins:ComponentRevision` %(predicate)s)

WITH cmp, rev
LIMIT 1

// Get invalidation state if present
OPTIONAL MATCH (rev)<-[:`prov:entity`]-(inv:`prov:Invalidation`)

RETURN id(cmp), cmp, id(rev), rev, inv is null
''')  # noqa


# Create statement for node with labels and properties
CREATE_COMPONENT = _('''
// Managing resource
MATCH (res:`origins:Resource` {`origins:id`: { resource }})

CREATE
    // Create new general component
    (cmp:`origins:Component`:`prov:Entity` {
        `origins:id`: { properties }.`origins:id`
    }),

    // Component revision
    (rev:`origins:ComponentRevision`:`prov:Entity` { properties }),

    // Resource manages the component and includes the revision
    (res)-[:`origins:manages`]->(cmp),
    (res)-[:`origins:includes`]->(rev),

    // Latest revision link (optimization)
    (cmp)-[:`origins:latest`]->(rev)

RETURN {
    component: id(cmp),
    revision: id(rev)
}
''')  # noqa


UPDATE_COMPONENT = _('''
// Use the latest revision of the component to derive from
MATCH (cmp:`origins:Component` {`origins:id`: { id }})-[lat:`origins:latest`]->(pre:`origins:ComponentRevision`)

WITH cmp, lat, pre

// Get the inclusion relationship by the managing resource to be
// replaced with the new revision
MATCH (cmp)<-[:`origins:manages`]-(res:`origins:Resource`),
      (pre)<-[inc:`origins:includes`]-(res)

CREATE
    (rev:`origins:ComponentRevision`:`prov:Entity`),

    // Resource includes new revision (previous is deleted below)
    (res)-[:`origins:includes`]->(rev),

    (cmp)-[:`origins:latest`]->(rev)

// Copy properties from previous, then SET/UNSET new properties
// in placeholder
SET rev = pre
%(placeholder)s

DELETE inc, lat

RETURN {
    component: id(cmp),
    revision: id(rev),
    previous: id(pre)
}
''')  # noqa


DELETE_COMPONENT = _('''
MATCH (cmp:`origins:Component` {`origins:id`: { id }})-[:`origins:latest`]->(rev:`origins:ComponentRevision`)

RETURN {
    component: id(cmp),
    revision: id(rev)
}
''')  # noqa


DERIVE_COMPONENT = _('''
MATCH (gnd:`origins:ComponentRevision` {`origins:uuid`: { generated }}),
      (usd:`origins:ComponentRevision` {`origins:uuid`: { used }})

CREATE
    (der:`prov:Derivation`:`prov:Relation` {
        `origins:type`: 'prov:Derivation',
        `origins:timestamp`: { timestamp },
        `prov:type`: { type }
    }),

    (bun:`prov:Bundle` {
        `origins:timestamp`: { timestamp }
    }),

    (der)-[:`prov:generatedEntity`]->(gnd),
    (der)-[:`prov:usedEntity`]->(usd),

    (bun)-[:`origins:describes` {`origins:sequence`: 0}]->(gnd),
    (bun)-[:`origins:describes` {`origins:sequence`: 1}]->(usd),
    (bun)-[:`origins:describes` {`origins:sequence`: 2}]->(der)

RETURN id(der)
''')  # noqa


# TODO Managing resource
COMPONENT_RESOURCE = _('''
MATCH (:`origins:Component` {`origins:id`: { id }})<-[:`origins:manages`]-(res:`origins:Resource`)
RETURN id(res), res
''')  # noqa


COMPONENT_RELATIONSHIPS = _('''
MATCH (cmp:`origins:Component` {`origins:id`: { id }})-[:`origins:latest`]->(crev:`origins:ComponentRevision`)

MATCH (crev)<-[:`origins:links`]-(rev:`origins:RelationshipRevision`)

WITH rev

MATCH (rev)-[:`origins:start`]->(scr:`origins:ComponentRevision`),
      (rev)-[:`origins:end`]->(ecr:`origins:ComponentRevision`)

MATCH (rev)<-[:`prov:specificEntity`]-(:`prov:Specialization`)-[:`prov:generalEntity`]->(rel:`origins:Relationship`)

OPTIONAL MATCH (rev)<-[:`prov:entity`]-(inv:`prov:Invalidation`)

RETURN id(rel), rel, id(rev), rev, id(scr), scr, id(ecr), ecr, inv is null
''')  # noqa

COMPONENT_RELATIONSHIP_COUNT = _('''
MATCH (cmp:`origins:Component` {`origins:id`: { id }})-[:`origins:latest`]->(crev:`origins:ComponentRevision`)

MATCH (crev)<-[:`origins:start`|`origins:end`]-(rev:`origins:RelationshipRevision`)

WITH rev

MATCH (rev)<-[:`prov:specificEntity`]-(:`prov:Specialization`)-[:`prov:generalEntity`]->(rel:`origins:Relationship`)

RETURN count(distinct rel)
''')  # noqa


COMPONENT_REVISIONS = _('''
MATCH (cmp:`origins:Component` {`origins:id`: { id }})<-[:`prov:generalEntity`]-(:`prov:Specialization`)-[:`prov:specificEntity`]->(rev:`origins:ComponentRevision`)

OPTIONAL MATCH (rev)<-[:`prov:entity`]-(inv:`prov:Invalidation`)

RETURN DISTINCT id(cmp), cmp, id(rev), rev, inv is null

ORDER BY rev.`origins:timestamp`
''')  # noqa

COMPONENT_REVISION_COUNT = _('''
MATCH (cmp:`origins:Component` {`origins:uuid`: { uuid }})<-[:`prov:generalEntity`]-(:`prov:Specialization`)-[:`prov:specificEntity`]->(rev:`origins:ComponentRevision`)

RETURN count(rev)
''')  # noqa

COMPONENT_REVISION = _('''
MATCH (rev:`origins:ComponentRevision` {`origins:uuid`: { uuid }})<-[:`prov:specificEntity`]-(:`prov:Specialization`)-[:`prov:generalEntity`]->(cmp:`origins:Component`)

OPTIONAL MATCH (rev)<-[:`prov:entity`]-(inv:`prov:Invalidation`)

RETURN id(cmp), cmp, id(rev), rev, inv is null
''')  # noqa


# Returns the sources of the component
COMPONENT_SOURCES = _('''
MATCH (:`origins:Component` {`origins:id`: { id }})<-[:`prov:generalEntity`]-(:`prov:Specialization`)-[:`prov:specificEntity`]->(:`origins:ComponentRevision`)<-[:`prov:generatedEntity`]-(der:`prov:Derivation`)-[:`prov:usedEntity`]-(rev:`origins:ComponentRevision`)

WHERE der.`prov:type` <> 'prov:Revision'

WITH rev

MATCH (rev)<-[:`prov:specificEntity`]-(:`prov:Specialization`)-[:`prov:generalEntity`]->(cmp:`origins:Component`)
OPTIONAL MATCH (rev)<-[:`prov:entity`]-(inv:`prov:Invalidation`)

RETURN id(cmp), cmp, id(rev), rev, inv IS NULL
''')  # noqa


COMPONENT_PATH = _('''
MATCH (start:`origins:ComponentRevision` {`origins:uuid`: { uuid }})

MATCH path=(start)-[:`origins:descends`*]->(end:`origins:ComponentRevision`)

// Trim off start revision
WITH tail(nodes(path)) as revs

UNWIND revs as rev

MATCH (rev)<-[:`prov:specificEntity`]-(:`prov:Specialization`)-[:`prov:generalEntity`]->(cmp:`origins:Component`)
OPTIONAL MATCH (rev)<-[:`prov:entity`]-(inv:`prov:Invalidation`)

RETURN DISTINCT id(cmp), cmp, id(rev), rev, inv IS NULL
''')  # noqa


# TODO: change relationship
COMPONENT_PARENT = _('''
MATCH (:`origins:ComponentRevision` {`origins:uuid`: { uuid }})-[:`origins:descends`]->(rev:`origins:ComponentRevision`)

MATCH (rev)<-[:`prov:specificEntity`]-(:`prov:Specialization`)-[:`prov:generalEntity`]->(cmp:`origins:Component`)
OPTIONAL MATCH (rev)<-[:`prov:entity`]-(inv:`prov:Invalidation`)

RETURN id(cmp), cmp, id(rev), rev, inv IS NULL
''')  # noqa


# TODO: change relationship
COMPONENT_CHILDREN = _('''
MATCH (:`origins:ComponentRevision` {`origins:uuid`: { uuid }})<-[:`origins:descends`]-(rev:`origins:ComponentRevision`)

WITH rev

MATCH (rev)<-[:`prov:specificEntity`]-(:`prov:Specialization`)-[:`prov:generalEntity`]->(cmp:`origins:Component`)
OPTIONAL MATCH (rev)<-[:`prov:entity`]-(inv:`prov:Invalidation`)

RETURN id(cmp), cmp, id(rev), rev, inv IS NULL
ORDER BY rev.`origins:label`
''')  # noqa


# Returns the full lineage of sources with depth
COMPONENT_LINEAGE = _('''
MATCH (:`origins:Component` {`origins:id`: { id }})<-[:`prov:generalEntity`]-(:`prov:Specialization`)-[:`prov:specificEntity`]->(r:`origins:ComponentRevision`),
    p=(r)<-[:`prov:generatedEntity`]-(d:`prov:Derivation`)-[:`prov:generatedEntity`|`prov:usedEntity`*]-(:`origins:ComponentRevision`)

WHERE d.`prov:type` <> 'prov:Revision'

WITH length(p) / 2 as depth, last(nodes(p)) as n

MATCH (n)<-[:`prov:specificEntity`]-(:`prov:Specialization`)-[:`prov:generalEntity`]->(c:`origins:Component`)
OPTIONAL MATCH (n)<-[:`prov:entity`]-(i:`prov:Invalidation`)

RETURN id(c), c, id(n), n, i IS NULL, depth
''')  # noqa


COMPONENT_TIMELINE = _('''
MATCH (:`origins:Component`)<-[:`prov:generalEntity`]-(:`prov:Specialization`)-[:`prov:specificEntity`]->(rev:`origins:ComponentRevision` {`origins:uuid`: { uuid }}),
    (rev)<--(rel:`prov:Relation`)<-[des:`origins:describes`]-(bun:`prov:Bundle`)

OPTIONAL MATCH (rel)-[lnk]->(obj)

RETURN id(rel), rel, type(lnk), id(obj), obj

ORDER BY bun.`origins:timestamp`,
         des.`origins:sequence`
''')  # noqa


COMPONENT_RESOURCE_COUNT = _('''
MATCH (:`origins:Component` {`origins:id`: { id }})-[:`origins:latest`]->(:`origins:ComponentRevision`)<-[:`origins:includes`]-(res:`origins:Resource`)

// Take in account the managing resource
RETURN count(distinct res) - 1
''')  # noqa

DELETE_COMPONENT_REAL = _('''
MATCH (cmp:`origins:Component` {`origins:id`: { id }})

OPTIONAL MATCH (cmp)-[cmp_r]-()

OPTIONAL MATCH (cmp)<-[cmp_e:`prov:entity`]-(cmp_gen:`prov:Generation`),
               (cmp_gen)-[cmp_gen_r]-()

OPTIONAL MATCH (cmp)<-[cmp_ge:`prov:generalEntity`]-(spe:`prov:Specialization`)-[cmp_se:`prov:specificEntity`]->(rev:`origins:ComponentRevision`),
               (rev)<-[rev_e:`prov:entity`]-(rev_gen:`prov:Generation`),
               (spe)-[spe_r]-(),
               (rev)-[rev_r]-(),
               (rev_gen)-[rev_gen_r]-()

DELETE cmp_ge, cmp_se, rev_e, cmp_e, spe_r, rev_r, cmp_r, rev_gen_r, cmp_gen_r, spe, rev, cmp, rev_gen, cmp_gen
''')  # noqa


DEPENDENT_COMPONENTS = _('''
MATCH (:`origins:ComponentRevision` {`origins:uuid`: { uuid }})<-[:`origins:links`]-(rel:`origins:RelationshipRevision`})-[:`origins:links` {`origins:dependent`: true}]->(cmp:`origins:ComponentRevision`)


''')  # noqa

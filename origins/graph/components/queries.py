from __future__ import unicode_literals, absolute_import

from ..utils import _


MATCH_COMPONENTS = _('''
MATCH (cmp:`origins:Component`)-[:`origins:latest`]->(rev:`prov:Revision` %(predicate)s)

WITH cmp, rev

// Get invalidation state if present
OPTIONAL MATCH (rev)<-[:`prov:entity`]-(inv:`prov:Invalidation`)

RETURN id(cmp), cmp, id(rev), rev, inv is null
''')  # noqa

SEARCH_COMPONENTS = _('''
MATCH (cmp:`origins:Component`)-[:`origins:latest`]->(rev:`prov:Revision`)
WHERE %(predicate)s

WITH cmp, rev

// Get invalidation state if present
OPTIONAL MATCH (rev)<-[:`prov:entity`]-(inv:`prov:Invalidation`)

RETURN id(cmp), cmp, id(rev), rev, inv is null
''')  # noqa


GET_COMPONENT = _('''
// Match component by predicate
MATCH (cmp:`origins:Component`)-[:`origins:latest`]->(rev:`prov:Revision` %(predicate)s)

WITH cmp, rev LIMIT 1

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

    // Component generation event
    (gen1:`prov:Generation`:`prov:Relation`:`prov:Event` {
        `origins:type`: 'prov:Generation',
        `origins:timestamp`: { timestamp }
    }),

    // Component revision
    (rev:`prov:Revision`:`prov:Entity` { properties }),

    // Component revision generation
    (gen2:`prov:Generation`:`prov:Relation`:`prov:Event` {
        `origins:timestamp`: { timestamp }
    }),

    // Specialization of revision to component
    (spe:`prov:Specialization`:`prov:Relation` {
        `origins:timestamp`: { timestamp }
    }),

    // Bundle of descriptions with the timestamp
    (bun:`prov:Bundle` {
        `origins:timestamp`: { timestamp }
    }),

    // Resource manages the component and includes the revision
    (res)-[:`origins:manages`]->(cmp),
    (res)-[:`origins:includes`]->(rev),

    // Link generation events
    (gen1)-[:`prov:entity`]->(cmp),
    (gen2)-[:`prov:entity`]->(rev),

    // Link specialization
    (spe)-[:`prov:generalEntity`]->(cmp),
    (spe)-[:`prov:specificEntity`]->(rev),

    // Latest revision link (optimization)
    (cmp)-[:`origins:latest`]->(rev),

    // Link bundle to all descriptions
    (bun)-[:`origins:describes` {`origins:sequence`: 0}]->(res),
    (bun)-[:`origins:describes` {`origins:sequence`: 1}]->(cmp),
    (bun)-[:`origins:describes` {`origins:sequence`: 2}]->(gen1),
    (bun)-[:`origins:describes` {`origins:sequence`: 3}]->(rev),
    (bun)-[:`origins:describes` {`origins:sequence`: 4}]->(gen2),
    (bun)-[:`origins:describes` {`origins:sequence`: 5}]->(spe)

// Return component, revision, and true for invalidation state
RETURN id(cmp), cmp, id(rev), rev, true
''')  # noqa


UPDATE_COMPONENT = _('''
// Use the latest revision of the component to derive from
MATCH (cmp:`origins:Component` {`origins:id`: { id }})-[lat:`origins:latest`]->(pre:`prov:Revision`)

WITH cmp, lat, pre

// Get the inclusion relationship by the managing resource to be
// replaced with the new revision
MATCH (cmp)<-[:`origins:manages`]-(res:`origins:Resource`),
      (pre)<-[inc:`origins:includes`]-(res)

CREATE
    (rev:`prov:Revision`),

    (use:`prov:Usage`:`prov:Relation`:`prov:Event` {
        `origins:type`: 'prov:Usage',
        `origins:timestamp`: { timestamp }
    }),

    (gen:`prov:Generation`:`prov:Relation`:`prov:Event` {
        `origins:type`: 'prov:Generation',
        `origins:timestamp`: { timestamp }
    }),

    (inv:`prov:Invalidation`:`prov:Relation`:`prov:Event` {
        `origins:type`: 'prov:Invalidation',
        `origins:method`: 'origins:InvalidByRevision',
        `origins:timestamp`: { timestamp }
    }),

    (spe:`prov:Specialization`:`prov:Relation` {
        `origins:type`: 'prov:Specialization',
        `origins:timestamp`: { timestamp }
    }),

    (der:`prov:Derivation`:`prov:Relation` {
        `origins:type`: 'prov:Derivation',
        `prov:type`: 'prov:Revision',
        `origins:timestamp`: { timestamp }
    }),

    (bun:`prov:Bundle` {
        `origins:timestamp`: { timestamp }
    }),

    // Resource includes new revision (previous is deleted below)
    (res)-[:`origins:includes`]->(rev),

    (cmp)-[:`origins:latest`]->(rev),

    (gen)-[:`prov:entity`]->(rev),

    (spe)-[:`prov:generalEntity`]->(cmp),
    (spe)-[:`prov:specificEntity`]->(rev),

    (der)-[:`prov:generatedEntity`]->(rev),
    (der)-[:`prov:usedEntity`]->(pre),
    (der)-[:`prov:generation`]->(gen),
    (der)-[:`prov:usage`]->(use),

    (inv)-[:`prov:entity`]->(pre),

    (bun)-[:`origins:describes` {`origins:sequence`: 0}]->(rev),
    (bun)-[:`origins:describes` {`origins:sequence`: 1}]->(pre),
    (bun)-[:`origins:describes` {`origins:sequence`: 2}]->(use),
    (bun)-[:`origins:describes` {`origins:sequence`: 3}]->(gen),
    (bun)-[:`origins:describes` {`origins:sequence`: 4}]->(der),
    (bun)-[:`origins:describes` {`origins:sequence`: 5}]->(spe),
    (bun)-[:`origins:describes` {`origins:sequence`: 6}]->(inv)


// Copy properties from previous, then SET/UNSET new properties
// in placeholder
SET rev = pre
%(placeholder)s

DELETE inc, lat

RETURN id(cmp), cmp, id(rev), rev, true
''')  # noqa


DELETE_COMPONENT = _('''
MATCH (cmp:`origins:Component` {`origins:id`: { id }})-[:`origins:latest`]->(rev:`prov:Revision`)

CREATE
    (inv:`prov:Invalidation`:`prov:Relation`:`prov:Event` {
        `origins:type`: 'prov:Invalidation',
        `origins:method`: 'origins:InvalidByDeletion',
        `origins:timestamp`: { timestamp }
    }),

    (bun:`prov:Bundle` {
        `origin:timestamp`: { timestamp }
    }),

    (inv)-[:`prov:entity`]->(rev),

    (bun)-[:`origins:describes` {`origins:sequence`: 0}]->(cmp),
    (bun)-[:`origins:describes` {`origins:sequence`: 1}]->(rev),
    (bun)-[:`origins:describes` {`origins:sequence`: 2}]->(inv)

RETURN id(cmp), cmp, id(rev), rev, false
''')  # noqa


DERIVE_COMPONENT = _('''
MATCH (gnd:`prov:Revision` {`origins:uuid`: { generated }}),
      (usd:`prov:Revision` {`origins:uuid`: { used }})

CREATE
    (der:`prov:Derivation`:`prov:Relation` {
        `origins:type`: 'prov:Derivation',
        `prov:type`: { type }
    }),

    (bun:`prov:Bundle` {
        `origins:timestamp`: timestamp()
    }),

    (der)-[:`prov:generatedEntity`]->(gnd),
    (der)-[:`prov:usedEntity`]->(usd),

    (bun)-[:`origins:describes` {`origins:sequence`: 0}]->(gnd),
    (bun)-[:`origins:describes` {`origins:sequence`: 1}]->(usd),
    (bun)-[:`origins:describes` {`origins:sequence`: 2}]->(der)

RETURN id(der), der
''')  # noqa


COMPONENT_RESOURCE = _('''
MATCH (:`origins:Component` {`origins:id`: { id }})<-[:`origins:manages`]-(res:`origins:Resource`)
RETURN id(res), res
''')  # noqa


COMPONENT_RELATIONSHIPS = _('''
MATCH (cmp:`origins:Component` {`origins:id`: { id }})-[:`origins:latest`]->(:`prov:Revision`)<-[:`origins:start`|`origins:end`]-(rev:`prov:Revision`)<-[:`prov:specificEntity`]-(:`prov:Specialization`)-[:`prov:generalEntity`]->(rel:`origins:Relationship`)

WITH rel, rev

// Get the start and end component (revisions)
MATCH (rev)-[:`origins:start`]->(scr:`prov:Revision`),
      (rev)-[:`origins:end`]->(ecr:`prov:Revision`)

OPTIONAL MATCH (rev)<-[:`prov:entity`]-(inv:`prov:Invalidation`)

RETURN id(rel), rel, id(rev), rev, id(scr), scr, id(ecr), ecr, inv is null
''')  # noqa


COMPONENT_REVISIONS = _('''
MATCH (cmp:`origins:Component` {`origins:id`: { id }})<-[:`prov:generalEntity`]-(:`prov:Specialization`)-[:`prov:specificEntity`]->(rev:`prov:Revision`)

OPTIONAL MATCH (rev)<-[:`prov:entity`]-(inv:`prov:Invalidation`)

RETURN DISTINCT id(cmp), cmp, id(rev), rev, inv is null

ORDER BY rev.`origins:timestamp`
''')  # noqa

COMPONENT_REVISION = _('''
MATCH (rev:`prov:Revision` {`origins:uuid`: { uuid }})<-[:`prov:specificEntity`]-(:`prov:Specialization`)-[:`prov:generalEntity`]->(cmp:`origins:Component`)

OPTIONAL MATCH (rev)<-[:`prov:entity`]-(inv:`prov:Invalidation`)

RETURN id(cmp), cmp, id(rev), rev, inv is null
''')  # noqa


# Returns the sources of the component
COMPONENT_SOURCES = _('''
MATCH (:`origins:Component` {`origins:id`: { id }})<-[:`prov:generalEntity`]-(:`prov:Specialization`)-[:`prov:specificEntity`]->(:`prov:Entity`)<-[:`prov:generatedEntity`]-(der:`prov:Derivation`)-[:`prov:usedEntity`]-(rev:`prov:Entity`)

WHERE der.`prov:type` <> 'prov:Revision'

WITH rev

MATCH (rev)<-[:`prov:specificEntity`]-(:`prov:Specialization`)-[:`prov:generalEntity`]->(cmp:`origins:Component`)
OPTIONAL MATCH (rev)<-[:`prov:entity`]-(inv:`prov:Invalidation`)

RETURN id(cmp), cmp, id(rev), rev, inv IS NULL
''')  # noqa


# Returns the full lineage of sources with depth
COMPONENT_LINEAGE = _('''
MATCH (:`origins:Component` {`origins:id`: { id }})<--(:`prov:Specialization`)-[:`prov:specificEntity`]->(r:`prov:Entity`),
    p=(r)<-[:`prov:generatedEntity`]-(d:`prov:Derivation`)-[:`prov:generatedEntity`|`prov:usedEntity`*]-(:`prov:Entity`)
WHERE d.`prov:type` <> 'prov:Revision'

WITH length(p) / 2 as depth, last(nodes(p)) as n

MATCH (n)<-[:`prov:specificEntity`]-(:`prov:Specialization`)-[:`prov:generalEntity`]->(c:`origins:Component`)
OPTIONAL MATCH (n)<-[:`prov:entity`]-(i:`prov:Invalidation`)

RETURN id(c), c, id(n), n, i IS NULL, depth
''')  # noqa


COMPONENT_TIMELINE = _('''
MATCH (:`origins:Component` {`origins:id`: { id }})<-[:`origins:describes`]-(bun:`prov:Bundle`)-[des:`origins:describes`]->(rel:`prov:Relation`)-[lnk]->(obj)

RETURN id(rel), rel, type(lnk), id(obj), obj

ORDER BY bun.`origins:timestamp`,
         des.`origins:sequence`
''')  # noqa

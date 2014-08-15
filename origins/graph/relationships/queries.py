from ..utils import _


RELATIONSHIP_REVISION_UUID = _('''
(rev:`origins:RelationshipRevision` {`origins:uuid`: { uuid }})
''')

MATCH_RELATIONSHIPS = _('''
MATCH (rel:`origins:Relationship`)-[:`origins:latest`]->(rev:`origins:RelationshipRevision` %(predicate)s)

WITH DISTINCT rel, rev

// Get the start and end component (revisions)
MATCH (rev)-[:`origins:start`]->(scr:`origins:ComponentRevision`),
      (rev)-[:`origins:end`]->(ecr:`origins:ComponentRevision`)

OPTIONAL MATCH (rev)<-[:`prov:entity`]-(inv:`prov:Invalidation`)

RETURN id(rel), rel, id(rev), rev, id(scr), scr, id(ecr), ecr, inv is null
''')  # noqa


SEARCH_RELATIONSHIPS = _('''
MATCH (rel:`origins:Relationship`)-[:`origins:latest`]->(rev:`origins:RelationshipRevision`)
WHERE %(predicate)s

WITH DISTINCT rel, rev

// Get the start and end component (revisions)
MATCH (rev)-[:`origins:start`]->(scr:`origins:ComponentRevision`),
      (rev)-[:`origins:end`]->(ecr:`origins:ComponentRevision`)

OPTIONAL MATCH (r)<-[:`prov:entity`]-(inv:`prov:Invalidation`)

RETURN id(rel), rel, id(rev), rev, id(scr), scr, id(ecr), ecr, inv is null
''')  # noqa


GET_RELATIONSHIP = _('''
MATCH (rel:`origins:Relationship`)-[:`origins:latest`]->(rev:`origins:RelationshipRevision` %(predicate)s)

WITH rel, rev
LIMIT 1

// Get the start and end component (revisions)
MATCH (rev)-[:`origins:start`]->(scr:`origins:ComponentRevision`),
      (rev)-[:`origins:end`]->(ecr:`origins:ComponentRevision`)

OPTIONAL MATCH (r)<-[:`prov:entity`]-(inv:`prov:Invalidation`)

RETURN id(rel), rel, id(rev), rev, id(scr), scr, id(ecr), ecr, inv is null
''')  # noqa


# Create statement for node with labels and properties
CREATE_RELATIONSHIP = _('''
MATCH (res:`origins:Resource` {`origins:id`: { resource }}),
      (scr:`origins:ComponentRevision` {`origins:uuid`: { start }})<-[:`prov:specificEntity`]-(:`prov:Specialization`)-[:`prov:generalEntity`]->(scp:`origins:Component`),
      (ecr:`origins:ComponentRevision` {`origins:uuid`: { end }})<-[:`prov:specificEntity`]-(:`prov:Specialization`)-[:`prov:generalEntity`]->(ecp:`origins:Component`)

// Create literal relationship between components
MERGE (scp)-[:`%(type)s`]->(ecp)

CREATE
    (rel:`origins:Relationship`:`prov:Entity` {`origins:id`: { properties }.`origins:id`}),

    (gen1:`prov:Generation`:`prov:Relation`:`prov:Event` {
        `origins:type`: 'prov:Generation',
        `origins:timestamp`: { timestamp }
    }),

    (rev:`origins:RelationshipRevision`:`prov:Entity` { properties }),

    (gen2:`prov:Generation`:`prov:Relation`:`prov:Event` {
        `origins:type`: 'prov:Generation',
        `origins:timestamp`: { timestamp }
    }),

    (spe:`prov:Specialization`:`prov:Relation` {
        `origins:type`: 'prov:Specialization',
        `origins:timestamp`: { timestamp }
    }),

    (bun:`prov:Bundle` {
        `origins:timestamp`: { timestamp }
    }),

    (res)-[:`origins:manages`]->(rel),
    (res)-[:`origins:includes`]->(rev),

    (gen1)-[:`prov:entity`]->(rel),
    (gen2)-[:`prov:entity`]->(rev),

    (spe)-[:`prov:generalEntity`]->(rel),
    (spe)-[:`prov:specificEntity`]->(rev),

    // Revision to start and end component revisions
    (rev)-[:`origins:start`]->(scr),
    (rev)-[:`origins:end`]->(ecr),

    // Latest revision link (optimization)
    (rel)-[:`origins:latest`]->(rev),

    (bun)-[:`origins:describes` {`origins:sequence`: 0}]->(res),
    (bun)-[:`origins:describes` {`origins:sequence`: 1}]->(scr),
    (bun)-[:`origins:describes` {`origins:sequence`: 2}]->(ecr),
    (bun)-[:`origins:describes` {`origins:sequence`: 3}]->(rel),
    (bun)-[:`origins:describes` {`origins:sequence`: 4}]->(gen1),
    (bun)-[:`origins:describes` {`origins:sequence`: 5}]->(rev),
    (bun)-[:`origins:describes` {`origins:sequence`: 6}]->(gen2),
    (bun)-[:`origins:describes` {`origins:sequence`: 7}]->(spe)

RETURN id(rel), rel, id(rev), rev, id(scr), scr, id(ecr), ecr, true
''')  # noqa


DESCENDS_RELATIONSHIP = _('''
MATCH (start:`origins:ComponentRevision` {`origins:uuid`: { start }}),
      (end:`origins:ComponentRevision` {`origins:uuid`: { end }})

MERGE (start)-[rel:`origins:descends`]->(end)

RETURN id(rel), rel
''')


UPDATE_RELATIONSHIP = _('''
MATCH (rel:`origins:Relationship` {`origins:id`: { id }})-[lat:`origins:latest`]->(pre:`origins:RelationshipRevision`),
      // Get latest revision of each start and end components
      (pre)-[:`origins:start`]->(:`origins:ComponentRevision`)<-[:`prov:specificEntity`]-(:`prov:Specialization`)-[:`prov:generalEntity`]->(:`origins:Component`)-[:`origins:latest`]->(scr:`origins:ComponentRevision`),
      (pre)-[:`origins:end`]->(:`origins:ComponentRevision`)<-[:`prov:specificEntity`]-(:`prov:Specialization`)-[:`prov:generalEntity`]->(:`origins:Component`)-[:`origins:latest`]->(ecr:`origins:ComponentRevision`)


CREATE
    (rev:`origins:RelationshipRevision`:`prov:Entity` { properties }),

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

    (gen)-[:`prov:entity`]->(rev),

    (spe)-[:`prov:generalEntity`]->(rel),
    (spe)-[:`prov:specificEntity`]->(rev),

    (der)-[:`prov:generatedEntity`]->(rel),
    (der)-[:`prov:usedEntity`]->(pre),

    (inv)-[:`prov:entity`]->(pre),

    (rev)-[:`origins:start`]->(scr),
    (rev)-[:`origins:end`]->(ecr),

    (rel)-[:`origins:latest`]->(rev),

    (bun)-[:`origins:describes` {`origins:sequence`: 0}]->(rel),
    (bun)-[:`origins:describes` {`origins:sequence`: 1}]->(scr),
    (bun)-[:`origins:describes` {`origins:sequence`: 2}]->(ecr),
    (bun)-[:`origins:describes` {`origins:sequence`: 3}]->(pre),
    (bun)-[:`origins:describes` {`origins:sequence`: 4}]->(rev),
    (bun)-[:`origins:describes` {`origins:sequence`: 5}]->(gen),
    (bun)-[:`origins:describes` {`origins:sequence`: 6}]->(spe),
    (bun)-[:`origins:describes` {`origins:sequence`: 7}]->(der),
    (bun)-[:`origins:describes` {`origins:sequence`: 8}]->(inv)


// Copy properties from previous, then SET/UNSET new properties
// in placeholder
SET rev = pre
%(placeholder)s

DELETE lat

RETURN id(rel), rel, id(rev), rev, id(scr), scr, id(ecr), ecr, true
''')  # noqa


DELETE_RELATIONSHIP = _('''
MATCH (rel:`origins:Relationship` {`origins:id`: { id }})<-[:`prov:generalEntity`]-(:`prov:Specialization`)-[:`prov:specificEntity`]->(rev:`origins:RelationshipRevision`)

WITH rel, rev

MATCH (rev)-[:`origins:start`]->(scr:`origins:ComponentRevision`)<-[:`prov:specificEntity`]-(:`prov:Specialization`)-[:`prov:generalEntity`]->(scp:`origins:Component`),
      (rev)-[:`origins:end`]->(ecr:`origins:ComponentRevision`)<-[:`prov:specificEntity`]-(:`prov:Specialization`)-[:`prov:generalEntity`]->(ecp:`origins:Component`)

CREATE
    (inv:`prov:Relation`:`prov:Event` {
        `origins:type`: 'prov:Invalidation',
        `origins:method`: 'origins:InvalidByDeletion',
        `origins:timestamp`: { timestamp }
    }),

    (inv)-[:`prov:entity`]->(rel)

WITH rel, rev, scr, ecr, scp, ecp

OPTIONAL MATCH (scp)-[lit:`%(type)s`]->(ecp)

DELETE lit

RETURN id(rel), rel, id(rev), rev, id(scr), scr, id(ecr), ecr, false
''')  # noqa


RELATIONSHIP_REVISIONS = _('''
MATCH (rel:`origins:Relationship` {`origins:id`: { id }})<-[:`prov:generalEntity`]-(:`prov:Specialization`)-[:`prov:specificEntity`]->(rev:`origins:RelationshipRevision`)

WITH rel, rev

MATCH (rev)-[:`origins:start`]->(scr:`origins:ComponentRevision`),
      (rev)-[:`origins:end`]->(ecr:`origins:ComponentRevision`)

OPTIONAL MATCH (rev)<-[:`prov:entity`]-(inv:`prov:Invalidation`)

RETURN id(rel), rel, id(rev), rev, id(scr), scr, id(ecr), ecr, inv is null

''')  # noqa


RELATIONSHIP_REVISION = _('''
MATCH (rel:`origins:Relationship`)<-[:`prov:generalEntity`]-(:`prov:Specialization`)-[:`prov:specificEntity`]->(rev:`origins:RelationshipRevision` {`origins:uuid`: { uuid }})

WITH rel, rev

MATCH (rev)-[:`origins:start`]->(scr:`prov:Entity`),
      (rev)-[:`origins:end`]->(ecr:`prov:Entity`)

OPTIONAL MATCH (rev)<-[:`prov:entity`]-(inv:`prov:Invalidation`)

RETURN id(rel), rel, id(rev), rev, id(scr), scr, id(ecr), ecr, inv is null
''')  # noqa


RELATIONSHIP_RESOURCE = _('''
MATCH (:`origins:Relationship` {`origins:id`: { id }})<-[:`origins:manages`]-(res:`origins:Resource`)
RETURN id(res), res
''')  # noqa


# Returns the sources of the component
RELATIONSHIP_SOURCES = _('''
MATCH (:`origins:Relationship` {`origins:id`: { id }})<-[:`prov:generalEntity`]-(:`prov:Specialization`)-[:`prov:specificEntity`]->(:`prov:Entity`)<-[:`prov:generatedEntity`]-(der:`prov:Derivation`)-[:`prov:usedEntity`]-(rev:`prov:Entity`)

WHERE der.`prov:type` <> 'prov:Revision'

WITH rev

MATCH (rev)<-[:`prov:specificEntity`]-(:`prov:Specialization`)-[:`prov:generalEntity`]->(rel:`origins:Relationship`),
      (rev)-[:`origins:start`]->(scr:`prov:Entity`),
      (rev)-[:`origins:end`]->(ecr:`prov:Entity`)

OPTIONAL MATCH (rev)<-[:`prov:entity`]-(inv:`prov:Invalidation`)

RETURN id(rel), rel, id(rev), rev, id(scr), scr, id(ecr), ecr, inv is null
''')  # noqa


# Returns the full lineage of sources with depth
RELATIONSHIP_LINEAGE = _('''
MATCH (:`origins:Relationship` {`origins:id`: { id }})<-[:`prov:generalEntity`]-(:`prov:Specialization`)-[:`prov:specificEntity`]->(rev:`origins:RelationshipRevision`),
    pth=(rev)<-[:`prov:generatedEntity`]-(der:`prov:Derivation`)-[:`prov:generatedEntity`|`prov:usedEntity`*]-(:`prov:Entity`)
WHERE der.`prov:type` <> 'prov:Revision'

WITH length(pth) / 2 as depth, last(nodes(path)) as rev

MATCH (rev)<-[:`prov:specificEntity`]-(:`prov:Specialization`)-[:`prov:generalEntity`]->(rel:`origins:Relationship`),
      (rev)-[:`origins:start`]->(scr:`prov:Entity`),
      (rev)-[:`origins:end`]->(ecr:`prov:Entity`)

OPTIONAL MATCH (rev)<-[:`prov:entity`]-(inv:`prov:Invalidation`)

RETURN id(rel), rel, id(rev), rev, id(scr), scr, id(ecr), ecr, inv is null
''')  # noqa


RELATIONSHIP_TIMELINE = _('''
MATCH (:`origins:Relationship`)<-[:`prov:generalEntity`]-(:`prov:Specialization`)-[:`prov:specificEntity`]->(rev:`origins:RelationshipRevision` {`origins:uuid`: { uuid }}),
    (rev)<--(rel:`prov:Relation`)<-[des:`origins:describes`]-(bun:`prov:Bundle`)

OPTIONAL MATCH (rel)-[lnk]->(obj)

RETURN id(rel), rel, type(lnk), id(obj), obj

ORDER BY bun.`origins:timestamp`,
         des.`origins:sequence`
''')  # noqa


DELETE_RELATIONSHIP_REAL = _('''
MATCH (rel:`origins:Relationship` {`origins:id`: { id }})

OPTIONAL MATCH (rel)-[rel_r]-()

OPTIONAL MATCH (rel)<-[rel_e:`prov:entity`]-(rel_gen:`prov:Generation`),
               (rel_gen)-[rel_gen_r]-()

OPTIONAL MATCH (rel)<-[rel_ge:`prov:generalEntity`]-(spe:`prov:Specialization`)-[rel_se:`prov:specificEntity`]->(rev:`origins:RelationshipRevision`),
               (rev)<-[rev_e:`prov:entity`]-(rev_gen:`prov:Generation`),
               (spe)-[spe_r]-(),
               (rev)-[rev_r]-(),
               (rev_gen)-[rev_gen_r]-()

DELETE rel_ge, rel_se, rev_e, rel_e, spe_r, rev_r, rel_r, rev_gen_r, rel_gen_r, spe, rev, rel, rev_gen, rel_gen
''')  # noqa

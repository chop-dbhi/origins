from . import neo4j, utils

_ = utils._

COLLECTION_ID_UNIQUE_CONSTRAINT = _('''
CREATE CONSTRAINT ON (c:`origins:Collection`)
ASSERT c.`origins:id` IS UNIQUE
''')

COLLECTION_UUID_INDEX = _('''
CREATE INDEX ON :`origins:Collection`(`origins:uuid`)
''')

RESOURCE_ID_UNIQUE_CONSTRAINT = _('''
CREATE CONSTRAINT ON (r:`origins:Resource`)
ASSERT r.`origins:id` IS UNIQUE
''')

RESOURCE_UUID_INDEX = _('''
CREATE INDEX ON :`origins:Resource`(`origins:uuid`)
''')

RESOURCE_ID_UNIQUE_CONSTRAINT = _('''
CREATE CONSTRAINT ON (r:`origins:Resource`)
ASSERT r.`origins:id` IS UNIQUE
''')

RESOURCE_UUID_INDEX = _('''
CREATE INDEX ON :`origins:Resource`(`origins:uuid`)
''')

RELATIONSHIP_ID_INDEX = _('''
CREATE INDEX ON :`origins:Relationship`(`origins:id`)
''')

RELATIONSHIP_UUID_INDEX = _('''
CREATE INDEX ON :`origins:Relationship`(`origins:uuid`)
''')


COMPONENT_ID_INDEX = _('''
CREATE INDEX ON :`origins:Component`(`origins:id`)
''')

COMPONENT_UUID_INDEX = _('''
CREATE INDEX ON :`origins:Component`(`origins:uuid`)
''')


COMPONENT_REVISION_UUID_INDEX = _('''
CREATE INDEX ON :`origins:ComponentRevision`(`origins:uuid`)
''')

RELATIONSHIP_REVISION_UUID_INDEX = _('''
CREATE INDEX ON :`origins:RelationshipRevision`(`origins:uuid`)
''')


_created = False


def ensure():
    global _created

    if _created:
        return

    neo4j.send([
        COLLECTION_ID_UNIQUE_CONSTRAINT,
        COLLECTION_UUID_INDEX,
        RESOURCE_ID_UNIQUE_CONSTRAINT,
        RESOURCE_UUID_INDEX,
        RELATIONSHIP_ID_INDEX,
        RELATIONSHIP_UUID_INDEX,
        RELATIONSHIP_REVISION_UUID_INDEX,
        COMPONENT_ID_INDEX,
        COMPONENT_UUID_INDEX,
        COMPONENT_REVISION_UUID_INDEX,
    ])

    _created = True

// Resources
CREATE CONSTRAINT ON (r:`origins:Resource`) ASSERT r.`origins:id` IS UNIQUE;
CREATE INDEX ON :`origins:Resource`(`origins:uuid`);

// Collections
CREATE CONSTRAINT ON (c:`origins:Collection`) ASSERT c.`origins:id` IS UNIQUE;
CREATE INDEX ON :`origins:Collection`(`origins:uuid`);

// Components
CREATE INDEX ON :`origins:Component`(`origins:id`);
CREATE INDEX ON :`origins:Component`(`origins:uuid`);
CREATE INDEX ON :`origins:ComponentRevision`(`origins:uuid`);

// Relationships
CREATE INDEX ON :`origins:Relationship`(`origins:id`);
CREATE INDEX ON :`origins:Relationship`(`origins:uuid`);
CREATE INDEX ON :`origins:RelationshipRevision`(`origins:uuid`);

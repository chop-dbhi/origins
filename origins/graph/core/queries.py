from string import Template as T


# Returns single node by it's internal ID
GET_NODE = T('''
START n=node({ node })
RETURN id(n), n
LIMIT 1
''')

# Returns the latest node by ID
GET_NODE_BY_ID = T('''
MATCH (n:`origins:Node`$labels {`origins:id`: { id }})
WHERE NOT (n)<-[:`prov:entity`]-(:`prov:Invalidation`)
RETURN id(n), n
LIMIT 1
''')


# Creates and returns a node
ADD_NODE = T('''
CREATE (n:`origins:Node`$labels { attrs })
RETURN id(n), n
''')


# Returns a single edge with the start and end nodes by it's internal ID
GET_EDGE = T('''
START n=node({ edge })
MATCH (n)-[:`origins:start`]->(s:`origins:Node`),
      (n)-[:`origins:end`]->(e:`origins:Node`)
RETURN id(n), n, id(s), s, id(e), e
LIMIT 1
''')

# Returns the latest edge by ID
GET_EDGE_BY_ID = T('''
MATCH (n:`origins:Edge`$labels {`origins:id`: { id }})
WHERE NOT (n)<-[:`prov:entity`]-(:`prov:Invalidation`)
WITH n
MATCH (n)-[:`origins:start`]->(s:`origins:Node`),
      (n)-[:`origins:end`]->(e:`origins:Node`)
RETURN id(n), n, id(s), s, id(e), e
LIMIT 1
''')


# Creates and returns an edge
ADD_EDGE = T('''
START s=node({ start }), e=node({ end })
CREATE (n:`origins:Edge`$labels { attrs }),
       (n)-[:`origins:start`]->(s),
       (n)-[:`origins:end`]->(e)
RETURN id(n), n, id(s), s, id(e), e
''')


# Returns all outbound edges of the node. That is, the node
# is the start of a directed edge.
NODE_OUTBOUND_EDGES = '''
START n=node({ node })
MATCH (n)<-[:`origins:start`]-(e:`origins:Edge`)
WHERE NOT (e)<-[:`prov:entity`]-(:`prov:Invalidation`)
WITH e
MATCH (e)-[:`origins:end`]->(o:`origins:Node`)
RETURN id(e), labels(e), e, id(o)
'''

# Finds outdated dependencies.
# Matches all valid edges that have a invalid end node and finds the latest
# non-invalid revision of that node. The start node, edge and new end node
# is returned.
OUTDATED_DEPENDENCIES = '''
// Valid edges
MATCH (e:`origins:Edge`)
WHERE NOT (e)<-[:`prov:entity`]-(:`prov:Invalidation`)
WITH e
// Pointing to an invalid end node
MATCH (e)<-[:`origins:end`]->(n:`origins:Node`)<-[:`prov:entity`]-(:`prov:Invalidation`)
WITH e, n
// That has a revision
MATCH (n)<-[:`prov:usedEntity`]-(d:`prov:Derivation` {`prov:type`: 'prov:Revision'})
MATCH (d)-[:`prov:generatedEntity`|`prov:usedEntity`*]-(l:`origins:Node` {`origins:id`: n.`origins:id`})
WITH e, l
WHERE NOT (l)<-[:`prov:entity`]-(:`prov:Invalidation`)
MATCH (e)-[:`origins:start`]->(s:`origins:Node`)
RETURN id(s), s, id(e), e, id(l), l
'''  # noqa

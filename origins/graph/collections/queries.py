from __future__ import unicode_literals, absolute_import

from ..utils import _


COLLECTION_COUNT = _('''
MATCH (col:`origins:Collection`)
RETURN count(col)
''')


MATCH_COLLECTIONS = _('''
MATCH (col:`origins:Collection` %(predicate)s)
RETURN DISTINCT id(col), col
''')


SEARCH_COLLECTIONS = _('''
MATCH (col:`origins:Collection`)
WHERE %(predicate)s
RETURN DISTINCT id(col), col
''')


GET_COLLECTION = _('''
MATCH (col:`origins:Collection` %(predicate)s)
RETURN id(col), col
LIMIT 1
''')


CREATE_COLLECTION = _('''
CREATE (col:`origins:Collection` { properties })
RETURN id(col), col
''')


UPDATE_COLLECTION = _('''
MATCH (col:`origins:Collection` {`origins:id`: { id }})
%(placeholder)s
RETURN id(col), col
''')


DELETE_COLLECTION = _('''
MATCH (col:`origins:Collection` {`origins:id`: { id }})

OPTIONAL MATCH (col)-[con:`origins:contains`]->()

DELETE con, col
RETURN count(con)
''')


COLLECTION_RESOURCES = _('''
MATCH (:`origins:Collection` {`origins:id`: { id }})-[:`origins:contains`]->(res:`origins:Resource`)
RETURN id(res), res
''')  # noqa


COLLECTION_RESOURCE_COUNT = _('''
MATCH (:`origins:Collection` {`origins:id`: { id }})-[:`origins:contains`]->(res:`origins:Resource`)
RETURN count(res)
''')  # noqa


COLLECTION_ADD_RESOURCE = _('''
MATCH (col:`origins:Collection` {`origins:id`: { id }}),
      (res:`origins:Resource` {`origins:id`: { resource }})
MERGE (col)-[con:`origins:contains`]->(res)
RETURN col, res, con
''')  # noqa


COLLECTION_REMOVE_RESOURCE = _('''
MATCH (:`origins:Collection` {`origins:id`: { id }})-[con:`origins:contains`]->(:`origins:Resource` {`origins:id`: { resource }})
DELETE con
''')  # noqa

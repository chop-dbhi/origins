from functools import partial
from string import Template as T
from origins.exceptions import ValidationError, DoesNotExist
from .core import nodes, traverse
from .model import Node
from . import neo4j


__all__ = (
    'match',
    'search',
    'get',
    'get_by_id',
    'add',
    'set',
    'remove',
    'components',
)


RESOURCE_MODEL = 'origins:Resource'
RESOURCE_TYPE = 'Resource'


MATCH_RESOURCE_COMPONENTS = T('''
MATCH (n:`origins:Component`$type $predicate)<-[:includes]-(:`origins:Node` {`origins:uuid`: { uuid }})
RETURN n
''')  # noqa


match = partial(nodes.match, model=RESOURCE_MODEL)
search = partial(nodes.search, model=RESOURCE_MODEL)

get_by_id = partial(nodes.get_by_id, model=RESOURCE_MODEL)
get = nodes.get

set = nodes.set
remove = nodes.remove


def add(id=None, label=None, description=None, properties=None, type=None,
        tx=neo4j.tx):

    if not type:
        type = RESOURCE_TYPE

    with tx as tx:
        # The ID of a resource must be unique
        if id:
            try:
                result = get_by_id(id)
            except DoesNotExist:
                result = None

            if result:
                raise ValidationError('resource already exists with id')

        return nodes.add(id=id,
                         label=label,
                         description=description,
                         properties=properties,
                         type=type,
                         model=RESOURCE_MODEL,
                         tx=tx)


def components(uuid, predicate=None, type=None, limit=None, skip=None,
               tx=neo4j.tx):

    with tx as tx:
        try:
            get(uuid, tx=tx)
        except DoesNotExist:
            raise ValidationError('resource does not exist')

        query = traverse.match(MATCH_RESOURCE_COMPONENTS,
                               predicate=predicate,
                               type=type,
                               limit=limit,
                               skip=skip)

        query['parameters']['uuid'] = uuid

        result = tx.send(query)

        return [Node.parse(r) for r in result]

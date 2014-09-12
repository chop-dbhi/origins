from ..exceptions import ValidationError, DoesNotExist
from . import neo4j, traverse
from .model import Continuant
from .edges import Edge


class Collection(Continuant):
    model_name = 'origins:Collection'

    model_type = 'Collection'

    match_resources_statement = '''

        MATCH (n:`origins:Resource`$type $predicate)<-[:includes]-(:`origins:Collection` {`origins:uuid`: { uuid }})
        RETURN n

    '''  # noqa

    search_resources_statement = '''

        MATCH (n:`origins:Resource`$type)<-[:includes]-(:`origins:Collection` {`origins:uuid`: { uuid }})
        WHERE $predicate
        RETURN n

    '''  # noqa

    @classmethod
    def _validate_unique(cls, node, tx):
        # Enforce unique IDs on collections
        if node.uuid != node.id and cls.exists_by_id(node.id, tx):
            raise ValidationError('{} with id {} already exists'
                                  .format(cls.model_name, id))

    @classmethod
    def resources(cls, uuid, predicate=None, type=None, limit=None, skip=None,
                  tx=neo4j.tx):

        from .resources import Resource

        with tx as tx:
            try:
                cls.get(uuid, tx=tx)
            except DoesNotExist:
                raise ValidationError('collection does not exist')

            if predicate:
                query = traverse.search(cls.search_resources_statement,
                                        predicate=predicate,
                                        type=type,
                                        limit=limit,
                                        skip=skip)
            else:
                query = traverse.match(cls.match_resources_statement,
                                       type=type,
                                       limit=limit,
                                       skip=skip)

            query['parameters']['uuid'] = uuid

            result = tx.send(query)

            return [Resource.parse(r) for r in result]

    @classmethod
    def add_resource(cls, uuid, resource, tx=neo4j.tx):
        with tx as tx:
            try:
                collection = cls.get(uuid, tx=tx)
            except DoesNotExist:
                raise ValidationError('collection does not exist')

            Edge.add(start=collection,
                     end=resource,
                     type='includes',
                     direction='bidirected',
                     tx=tx)

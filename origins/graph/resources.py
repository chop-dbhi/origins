from origins.exceptions import ValidationError, DoesNotExist
from .model import Continuant
from .edges import Edge
from . import neo4j, traverse


class Resource(Continuant):
    model_name = 'origins:Resource'

    model_type = 'Resource'

    match_components_statement = '''

        MATCH (n:`origins:Component`$type $predicate)<-[:includes]-(:`origins:Resource` {`origins:uuid`: { uuid }})
        RETURN n

    '''  # noqa

    match_managed_components_statement = '''

        MATCH (n:`origins:Component`$type $predicate)<-[:manages]-(:`origins:Resource` {`origins:uuid`: { uuid }})
        RETURN n

    '''  # noqa

    search_components_statement = '''

        MATCH (n:`origins:Component`$type)<-[:includes]-(:`origins:Resource` {`origins:uuid`: { uuid }})
        WHERE $predicate
        RETURN n

    '''  # noqa

    @classmethod
    def _validate_unique(cls, node, tx):
        # Enforce unique IDs on resources
        if node.uuid != node.id and cls.exists_by_id(node.id, tx):
            raise ValidationError('{} with id {} already exists'
                                  .format(cls.model_name, id))

    @classmethod
    def components(cls, uuid, predicate=None, type=None, limit=None, skip=None,
                   tx=neo4j.tx):

        from .components import Component

        with tx as tx:
            try:
                cls.get(uuid, tx=tx)
            except DoesNotExist:
                raise ValidationError('resource does not exist')

            if predicate:
                query = traverse.search(cls.search_components_statement,
                                        predicate=predicate,
                                        type=type,
                                        limit=limit,
                                        skip=skip)
            else:
                query = traverse.match(cls.match_components_statement,
                                       type=type,
                                       limit=limit,
                                       skip=skip)

            query['parameters']['uuid'] = uuid

            result = tx.send(query)

            return [Component.parse(r) for r in result]

    @classmethod
    def managed_components(cls, uuid, type=None, limit=None, skip=None,
                           tx=neo4j.tx):

        from .components import Component

        with tx as tx:
            if not cls.exists(uuid, tx=tx):
                raise ValidationError('resource does not exist')

            query = traverse.match(cls.match_managed_components_statement,
                                   type=type,
                                   limit=limit,
                                   skip=skip)

            query['parameters']['uuid'] = uuid

            result = tx.send(query)

            return [Component.parse(r) for r in result]

    @classmethod
    def include_component(cls, uuid, component, tx=neo4j.tx):
        with tx as tx:
            try:
                resource = cls.get(uuid, tx=tx)
            except DoesNotExist:
                raise ValidationError('resource does not exist')

            Edge.add(start=resource,
                     end=component,
                     type='includes',
                     tx=tx)

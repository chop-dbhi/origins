from origins.exceptions import ValidationError, DoesNotExist
from .model import Continuant
from . import neo4j, traverse, utils


class Resource(Continuant):
    model_name = 'origins:Resource'

    model_type = 'Resource'

    match_included_statement = '''

        MATCH (:`origins:Resource` {`origins:uuid`: { uuid }})-[:`origins:includes`]->(n$model$type $predicate)
        WHERE NOT (n)<-[:`prov:entity`]-(:`prov:Invalidation`)
        RETURN n

    '''  # noqa

    match_managed_statement = '''

        MATCH (:`origins:Resource` {`origins:uuid`: { uuid }})-[:`origins:manages`]->(n$model$type $predicate)
        WHERE NOT (n)<-[:`prov:entity`]-(:`prov:Invalidation`)
        RETURN n

    '''  # noqa

    search_included_statement = '''

        MATCH (:`origins:Resource` {`origins:uuid`: { uuid }})-[:`origins:includes`]->(n$model$type)
        WHERE NOT (n)<-[:`prov:entity`]-(:`prov:Invalidation`)
            AND $predicate
        RETURN n

    '''  # noqa

    create_include_statement = '''

        MATCH (r:`origins:Resource` {`origins:uuid`: { resource }}),
              (n$model$type {`origins:uuid`: { item }})

        CREATE (r)-[:`origins:includes`]->(n)

    '''  # noqa

    copy_items_statement = '''

        MATCH (o:`origins:Resource` {`origins:uuid`: { old }}),
              (n:`origins:Resource` {`origins:uuid`: { new }})

        MATCH (o)-[:`origins:manages`]->(m)
            WHERE NOT (m)<-[:`prov:entity`]-(:`prov:Invalidation`)

        CREATE (n)-[:`origins:manages`]->(m)

        WITH DISTINCT o, n

        MATCH (o)-[:`origins:includes`]->(i)
            WHERE NOT (i)<-[:`prov:entity`]-(:`prov:Invalidation`)

        CREATE (n)-[:`origins:includes`]->(i)

    '''  # noqa

    @classmethod
    def _validate_unique(cls, node, tx):
        # Enforce unique IDs on resources
        if node.uuid != node.id and cls.exists_by_id(node.id, tx):
            raise ValidationError('{} with id {} already exists'
                                  .format(cls.model_name, id))

    @classmethod
    def _set(cls, old, new, validate, tx):
        prov = Continuant._set(old, new, validate, tx)

        statement = utils.prep(cls.copy_items_statement)

        query = {
            'statement': statement,
            'parameters': {
                'old': old.uuid,
                'new': new.uuid,
            }
        }

        tx.send(query, defer=not validate)

        return prov

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
                query = traverse.search(cls.search_included_statement,
                                        predicate=predicate,
                                        type=type,
                                        model=Component.model_name,
                                        limit=limit,
                                        skip=skip)
            else:
                query = traverse.match(cls.match_included_statement,
                                       type=type,
                                       model=Component.model_name,
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

            query = traverse.match(cls.match_managed_statement,
                                   type=type,
                                   model=Component.model_name,
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

            statement = utils.prep(cls.create_include_statement,
                                   model=component.model_name,
                                   type=component.type)

            query = {
                'statement': statement,
                'parameters': {
                    'resource': resource.uuid,
                    'item': component.uuid,
                }
            }

            tx.send(query, defer=True)

    @classmethod
    def relationships(cls, uuid, predicate=None, type=None, limit=None,
                      skip=None, tx=neo4j.tx):

        from .relationships import Relationship

        with tx as tx:
            try:
                cls.get(uuid, tx=tx)
            except DoesNotExist:
                raise ValidationError('resource does not exist')

            if predicate:
                query = traverse.search(cls.search_included_statement,
                                        predicate=predicate,
                                        type=type,
                                        model=Relationship.model_name,
                                        limit=limit,
                                        skip=skip)
            else:
                query = traverse.match(cls.match_included_statement,
                                       type=type,
                                       model=Relationship.model_name,
                                       limit=limit,
                                       skip=skip)

            query['parameters']['uuid'] = uuid

            result = tx.send(query)

            return [Relationship.parse(r) for r in result]

    @classmethod
    def managed_relationships(cls, uuid, type=None, limit=None, skip=None,
                              tx=neo4j.tx):

        from .relationships import Relationship

        with tx as tx:
            if not cls.exists(uuid, tx=tx):
                raise ValidationError('resource does not exist')

            query = traverse.match(cls.match_managed_statement,
                                   type=type,
                                   model=Relationship.model_name,
                                   limit=limit,
                                   skip=skip)

            query['parameters']['uuid'] = uuid

            result = tx.send(query)

            return [Relationship.parse(r) for r in result]

    @classmethod
    def include_relationship(cls, uuid, relationship, tx=neo4j.tx):
        with tx as tx:
            try:
                resource = cls.get(uuid, tx=tx)
            except DoesNotExist:
                raise ValidationError('resource does not exist')

            statement = utils.prep(cls.create_include_statement,
                                   model=relationship.model_name,
                                   type=relationship.type)

            query = {
                'statement': statement,
                'parameters': {
                    'resource': resource.uuid,
                    'item': relationship.uuid,
                }
            }

            tx.send(query, defer=True)

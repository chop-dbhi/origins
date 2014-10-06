from origins.exceptions import ValidationError, InvalidStateError
from .model import Continuant
from . import neo4j, utils, deps


class Component(Continuant):
    model_name = 'origins:Component'

    model_type = 'Component'

    get_by_id_statement = '''

        MATCH (:`origins:Resource` {`origins:uuid`: { resource }})-[:`origins:manages`]->(n:`origins:Component` {`origins:id`: { id }})
            WHERE NOT (n)<-[:`prov:entity`]-(:`prov:Invalidation`)
        RETURN n

    '''  # noqa

    # Gets the component's resource.
    get_resource_statement = '''

        MATCH (:`origins:Component` {`origins:uuid`: { uuid }})<-[:`origins:manages`]-(n:`origins:Resource`)
        RETURN n

    '''  # noqa

    link_resource_statement = '''

        MATCH (r:`origins:Resource` {`origins:uuid`: { resource }}),
              (n:`origins:Component` {`origins:uuid`: { component }})

        CREATE (r)-[:`origins:includes`]->(n),
               (r)-[:`origins:manages`]->(n)

    '''  # noqa

    copy_resources = '''

        MATCH (o:`origins:Component` {`origins:uuid`: { old }}),
              (n:`origins:Component` {`origins:uuid`: { new }}),
              (o)<-[:`origins:manages`]-(m:`origins:Resource`),
              (o)<-[:`origins:includes`]-(i:`origins:Resource`)

        CREATE (m)-[:`origins:manages`]->(n),
               (i)-[:`origins:includes`]->(n)

    '''  # noqa

    def __init__(self, *args, **kwargs):
        self.resource = kwargs.pop('resource', None)
        super(Component, self).__init__(*args, **kwargs)

    @classmethod
    def get_by_id_query(cls, id, resource):
        statement = utils.prep(cls.get_by_id_statement,
                               model=cls.model_name)

        return {
            'statement': statement,
            'parameters': {
                'id': id,
                'resource': resource.uuid,
            }
        }

    @classmethod
    def _validate_unique(cls, node, tx):
        query = {
            'statement': cls.get_by_id_statement,
            'parameters': {
                'id': node.id,
                'resource': node.resource.uuid,
            }
        }

        result = tx.send(query)

        if result:
            raise ValidationError('{} already exists by id'
                                  .format(node.model_type))

    @classmethod
    def _add(cls, node, validate, tx):
        prov = Continuant._add(node, validate=validate, tx=tx)

        statement = utils.prep(cls.link_resource_statement)

        query = {
            'statement': statement,
            'parameters': {
                'resource': node.resource.uuid,
                'component': node.uuid,
            }
        }

        tx.send(query, defer=not validate)

        return prov

    @classmethod
    def _set(cls, old, new, validate, tx):
        # Invalidate old version
        cls._invalidate(old, tx=tx)

        # Add new version
        prov = Continuant._add(new, validate=validate, tx=tx)

        statement = utils.prep(cls.copy_resources)

        query = {
            'statement': statement,
            'parameters': {
                'old': old.uuid,
                'new': new.uuid,
            }
        }

        tx.send(query, defer=not validate)

        # Trigger the change dependency. This must occur after the new
        # node has been added so it is visible in the graph.
        deps.trigger_change(old, new, validate=validate, tx=tx)

        return prov

    @classmethod
    def resource(cls, uuid, tx=neo4j.tx):
        from .resources import Resource

        query = {
            'statement': cls.get_resource_statement,
            'parameters': {
                'uuid': uuid,
            }
        }

        result = tx.send(query)

        if not result:
            raise InvalidStateError('No resource associated with component.')

        return Resource.parse(*result[0])

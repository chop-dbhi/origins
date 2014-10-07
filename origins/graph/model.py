"""
A model in Origins defines the semantics and structure of how a *value* based
on the model behaves and is represented in the graph.

A value is presented as a set of core metadata and optional set of properties.
Additional metadata may be supported when a model is applied to the value.

There are two basic concepts being modeled in Origins, *occurrents* and
*continuants*. An occurrent's identity is defined by it's *value*, while a
continuant retains it's identity regardless if it's state and relations change
over time. A continuant can be logically represented as a series of
occurrents.

In PROV, this corresponds to the `Activity` and `Entity` types, respectively.
PROV defines `Agent` as being an `Entity` or `Activity` depending on context,
but always having a responsibility over the thing it is influencing.

Origins defines both the `Occurrent` and `Continuant` types to represent these
two distinct concepts.
"""

from copy import deepcopy
from uuid import uuid4
from ..exceptions import DoesNotExist, ValidationError
from .. import events
from .packer import unpack, pack
from . import neo4j, utils, traverse, deps, provenance


DIFF_ATTRS = {
    'type',
    'label',
    'description',
    'direction',
    'dependence',
    'properties',
    'start',
    'end',
}


class Model(object):
    def __init__(self, id=None, type=None, label=None, description=None,
                 properties=None, uuid=None, time=None, model=None,
                 sha1=None, invalidation=None):

        if not type:
            type = self.model_type

        if not model:
            model = self.model_name

        if not sha1:
            sha1 = utils.dict_sha1(properties)

        if not time:
            time = utils.timestamp()

        if not uuid:
            uuid = str(uuid4())

        if not id:
            id = uuid

        self.uuid = uuid
        self.id = id
        self.time = time
        self.type = type
        self.model = model
        self.label = label
        self.description = description
        self.properties = properties
        self.sha1 = sha1
        self.invalidation = invalidation

    def __hash__(self):
        return hash(self.uuid)

    def __str__(self):
        if self.label:
            label = self.label
        elif not utils.is_uuid(self.id):
            label = self.id
        else:
            label = None

        if label:
            return '"{}" r.{}'.format(label, self.uuid[:8])

        return 'r.{}'.format(self.uuid[:8])

    def __repr__(self):
        return '<{}: {}>'.format(self.__class__.__name__, str(self))

    def __eq__(self, other):
        if hasattr(other, 'uuid'):
            return self.uuid == other.uuid

        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    @classmethod
    def parse(cls, attrs, invalidation=None):
        return cls(invalidation=unpack(invalidation), **unpack(attrs))

    @classmethod
    def derive(cls, node, attrs=None):
        if attrs is None:
            attrs = {}

        copy = node._derive_attrs()
        copy.update(attrs)

        # Copy properties
        if node.properties:
            props = deepcopy(node.properties)

            # New properties take precedence
            if attrs.get('properties'):
                props.update(attrs['properties'])

            copy['properties'] = props

        return cls(**copy)

    def _derive_attrs(self):
        return {
            'id': self.id,
            'label': self.label,
            'type': self.type,
            'model': self.model,
            'description': self.description,
        }

    def to_dict(self):
        return {
            'uuid': self.uuid,
            'id': self.id,
            'model': self.model,
            'type': self.type,
            'label': self.label,
            'description': self.description,
            'sha1': self.sha1,
            'time': self.time,
            'properties': deepcopy(self.properties),
            'invalidation': deepcopy(self.invalidation),
        }

    def diff(self, other):
        return utils.diff_attrs(self.to_dict(), other.to_dict(),
                                allowed=DIFF_ATTRS)

    # Returns all
    match_statement = '''
        MATCH (n$model$type $predicate)
        WHERE NOT (n)<-[:`prov:entity`]-(:`prov:Invalidation`)
        RETURN n, null
    '''

    # Search nodes with predicate
    search_statement = '''
        MATCH (n$model$type)
        WHERE NOT (n)<-[:`prov:entity`]-(:`prov:Invalidation`)
            AND $predicate
        RETURN n, null
    '''

    # Returns single node by UUID. The model nor type is applied here since
    # the lookup is by UUID
    get_statement = '''
        MATCH (n$model {`origins:uuid`: { uuid }})
        OPTIONAL MATCH (n)<-[:`prov:entity`]-(i:`prov:Invalidation`)
        RETURN n, i
        LIMIT 1
    '''

    get_by_id_statement = '''
        MATCH (n$model$type {`origins:id`: { id }})
            WHERE NOT (n)<-[:`prov:entity`]-(:`prov:Invalidation`)
        RETURN n, null
        LIMIT 1
    '''

    # Return 1 if the node exists
    exists_statement = '''
        MATCH (n$model {`origins:uuid`: { uuid }})
        RETURN true
        LIMIT 1
    '''

    # Return 1 if the node exists
    exists_by_id_statement = '''
        MATCH (n$model {`origins:id`: { id }})
        RETURN true
        LIMIT 1
    '''

    # Creates a node
    # The `prov:Entity` and `origins:Node` labels are fixed.
    add_statement = '''
        CREATE (n$model$type:`prov:Entity` { attrs })
        RETURN 1
    '''

    # Removes the `$type` label for a node. Type labels are only set while
    # the node is valid.
    invalidate_statement = '''
        MATCH (n$model {`origins:uuid`: { uuid }})
        REMOVE n$type
        RETURN 1
    '''

    @classmethod
    def match_query(cls, predicate=None, limit=None, skip=None, type=None):

        return traverse.match(cls.match_statement,
                              predicate=pack(predicate),
                              limit=limit,
                              skip=skip,
                              type=type,
                              model=cls.model_name)

    @classmethod
    def search_query(cls, predicate, operator=None, limit=None, skip=None,
                     type=None):

        return traverse.search(cls.search_statement,
                               predicate=pack(predicate),
                               operator=operator,
                               limit=limit,
                               skip=skip,
                               type=type,
                               model=cls.model_name)

    @classmethod
    def get_query(cls, uuid):
        statement = utils.prep(cls.get_statement,
                               model=cls.model_name)

        return {
            'statement': statement,
            'parameters': {
                'uuid': uuid,
            }
        }

    @classmethod
    def get_by_id_query(cls, id):
        statement = utils.prep(cls.get_by_id_statement,
                               model=cls.model_name)

        return {
            'statement': statement,
            'parameters': {
                'id': id,
            }
        }

    @classmethod
    def exists_query(cls, uuid):
        statement = utils.prep(cls.exists_statement,
                               model=cls.model_name)

        return {
            'statement': statement,
            'parameters': {
                'uuid': uuid,
            }
        }

    @classmethod
    def exists_by_id_query(cls, id):
        statement = utils.prep(cls.exists_by_id_statement,
                               model=cls.model_name)

        return {
            'statement': statement,
            'parameters': {
                'id': id,
            }
        }

    @classmethod
    def add_query(cls, node):
        # Get all labels for this node up the class hierarchy
        model_names = cls.model_names(node)

        statement = utils.prep(cls.add_statement,
                               type=node.type,
                               model=model_names)

        return {
            'statement': statement,
            'parameters': {
                'attrs': pack(node.to_dict()),
            },
        }

    @classmethod
    def invalidate_query(cls, node):
        statement = utils.prep(cls.invalidate_statement,
                               type=node.type,
                               model=node.model_name)

        return {
            'statement': statement,
            'parameters': {
                'uuid': node.uuid
            },
        }

    @classmethod
    def match(cls, predicate=None, limit=None, skip=None, type=None,
              tx=neo4j.tx):

        query = cls.match_query(predicate=predicate,
                                limit=limit,
                                skip=skip,
                                type=type)

        result = tx.send(query)

        return [cls.parse(*r) for r in result]

    @classmethod
    def search(cls, predicate, operator=None, limit=None, skip=None, type=None,
               tx=neo4j.tx):

        query = cls.search_query(predicate=predicate,
                                 operator=operator,
                                 limit=limit,
                                 skip=skip,
                                 type=type)

        result = tx.send(query)

        return [cls.parse(*r) for r in result]

    @classmethod
    def get(cls, uuid, tx=neo4j.tx):
        query = cls.get_query(uuid=uuid)

        result = tx.send(query)

        if not result:
            raise DoesNotExist('{} does not exist'.format(cls.model_type))

        return cls.parse(*result[0])

    @classmethod
    def get_by_id(cls, id, tx=neo4j.tx, **kwargs):
        query = cls.get_by_id_query(id=id, **kwargs)

        result = tx.send(query)

        if not result:
            raise DoesNotExist('{} does not exist'.format(cls.model_type))

        return cls.parse(*result[0])

    @classmethod
    def exists(cls, uuid, tx=neo4j.tx):
        query = cls.exists_query(uuid=uuid)

        result = tx.send(query)

        return True if result else False

    @classmethod
    def exists_by_id(cls, id, tx=neo4j.tx):
        query = cls.exists_by_id_query(id)

        result = tx.send(query)

        return True if result else False

    @classmethod
    def _add(cls, node, validate, tx):
        query = cls.add_query(node)

        if not validate:
            tx.send(query, defer=True)
        else:
            if not tx.send(query):
                raise Exception('did not successfully add {}'.format(node))

        prov_spec = provenance.add(node.uuid)

        return provenance.load(prov_spec, tx=tx)

    @classmethod
    def _set(cls, old, new, validate, tx):
        # Invalidate old version
        cls._invalidate(old, tx=tx)

        # Add new version
        prov = cls._add(new, validate=validate, tx=tx)

        # Trigger the change dependency. This must occur after the new
        # node has been added so it is visible in the graph.
        deps.trigger_change(old, new, validate=validate, tx=tx)

        return prov

    @classmethod
    def _invalidate(cls, node, tx):
        query = cls.invalidate_query(node)

        if not tx.send(query):
            raise Exception('did not successfully delete {}'.format(node))

    @classmethod
    def _remove(cls, node, reason, validate, tx, trigger=None):
        with tx as tx:
            cls._invalidate(node, tx)

            # TODO do something with triggers
            # Provenance for remove
            prov_spec = provenance.remove(node.uuid, reason=reason)
            return provenance.load(prov_spec, tx=tx)

    @classmethod
    def _validate_unique(cls, node, tx):
        pass

    @classmethod
    def _validate(cls, node, tx):
        pass

    @classmethod
    def add(cls, *args, **kwargs):
        tx = kwargs.pop('tx', neo4j.tx)
        validate = kwargs.pop('validate', True)

        with tx as tx:
            node = cls(*args, **kwargs)

            if validate:
                cls._validate_unique(node, tx=tx)
                cls._validate(node, tx=tx)

            prov = cls._add(node, validate=validate, tx=tx)

            events.publish('{}.add'.format(cls.model_type), {
                'node': node.to_dict(),
                'prov': prov,
            })

            return node

    @classmethod
    def set(cls, uuid, validate=True, **kwargs):
        tx = kwargs.pop('tx', neo4j.tx)

        with tx as tx:
            if isinstance(uuid, cls):
                previous = uuid
            else:
                previous = cls.get(uuid, tx=tx)

                if previous.invalidation:
                    raise ValidationError('cannot set invalidation {}'
                                          .format(cls.model_type))

            # Derive from previous
            node = cls.derive(previous, kwargs)

            # Compare the new node with the previous
            diff = previous.diff(node)

            if not diff:
                return

            prov = cls._set(previous, node, validate=validate, tx=tx)

            # Provenance for change
            prov_spec = provenance.set(previous.uuid, node.uuid)
            prov_spec['wasGeneratedBy'] = prov['wasGeneratedBy']
            prov_spec['wasDerivedFrom']['wdf']['prov:generation'] = 'wgb'
            prov = provenance.load(prov_spec, tx=tx)

            events.publish('{}.change'.format(cls.model_type), {
                'node': node.to_dict(),
                'diff': diff,
                'prov': prov,
            })

            return node

    @classmethod
    def remove(cls, uuid, reason=None, validate=True, tx=neo4j.tx):
        with tx as tx:
            if isinstance(uuid, cls):
                node = uuid
            else:
                node = cls.get(uuid, tx=tx)

                if node.invalidation:
                    raise ValidationError('{} already invalidated: {}'
                                          .format(cls.model_type, uuid))

            nodes = deps.trigger_remove(node, validate=validate, tx=tx)

            # Remove all nodes
            for n, triggers in nodes.items():
                if n != node:
                    r = 'origins:DependentNodeRemoved'
                else:
                    r = reason

                prov = cls._remove(n, reason=r, validate=validate, tx=tx)

                events.publish('{}.remove'.format(n.model_type), {
                    'node': n.to_dict(),
                    'prov': prov,
                })

            return node

    @classmethod
    def model_names(cls, node):
        # Get all labels for this node up the class hierarchy
        return tuple([m.model_name for m in node.__class__.__mro__
                      if hasattr(m, 'model_name')])


class Occurrent(Model):
    model_name = 'origins:Occurrent'

    model_type = 'Occurrent'


class Continuant(Model):
    model_name = 'origins:Continuant'

    model_type = 'Continuant'

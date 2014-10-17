from origins.exceptions import DoesNotExist, ValidationError
from . import traverse, utils, provenance
from .model import Continuant
from .packer import pack, unpack


NO_DEPENDENCE = 'none'
FORWARD_DEPENDENCE = 'forward'
INVERSE_DEPENDENCE = 'inverse'
MUTUAL_DEPENDENCE = 'mutual'

DEPENDENCE_TYPES = (
    NO_DEPENDENCE,
    FORWARD_DEPENDENCE,
    INVERSE_DEPENDENCE,
    MUTUAL_DEPENDENCE,
)

UNDIRECTED = 'undirected'
DIRECTED = 'directed'
REVERSE = 'reverse'
BIDIRECTED = 'bidirected'

DIRECTION_TYPES = (
    UNDIRECTED,
    DIRECTED,
    REVERSE,
    BIDIRECTED,
)


class Edge(Continuant):
    model_name = 'origins:Edge'

    model_type = 'related'

    # Declares the model used for representing the start and end nodes
    start_model = Continuant

    end_model = Continuant

    match_statement = '''

        MATCH (n$model$type $predicate)
            WHERE NOT (n)<-[:`prov:entity`]-(:`prov:Invalidation`)
        WITH n
        MATCH (n)-[:`origins:start`]->(s$start_model),
              (n)-[:`origins:end`]->(e$end_model)
        RETURN n, null, s, e

    '''

    search_statement = '''

        MATCH (n$model$type)
        WHERE NOT (n)<-[:`prov:entity`]-(:`prov:Invalidation`)
            AND $predicate
        WITH n
        MATCH (n)-[:`origins:start`]->(s$start_model),
              (n)-[:`origins:end`]->(e$end_model)
        RETURN n, null, s, e

    '''

    get_statement = '''

        MATCH (n$model {`origins:uuid`: { uuid }}),
              (n)-[:`origins:start`]->(s$start_model),
              (n)-[:`origins:end`]->(e$end_model)
        OPTIONAL MATCH (n)<-[:`prov:entity`]-(i:`prov:Invalidation`)
        RETURN n, i, s, e
        LIMIT 1

    '''

    get_by_id_statement = '''

        MATCH (n$model {`origins:id`: { id }})
        WHERE NOT (n)<-[:`prov:entity`]-(:`prov:Invalidation`)
        WITH n
        MATCH (n)-[:`origins:start`]->(s$start_model),
              (n)-[:`origins:end`]->(e$end_model)
        RETURN n, null, s, e
        LIMIT 1

    '''

    # Creates an edge
    # The `prov:Entity` label is added to hook into the provenance data model
    add_statement = '''

        MATCH (s$start_model {`origins:uuid`: { start }}),
              (e$end_model {`origins:uuid`: { end }})

        CREATE (n$model$type:`prov:Entity` { attrs }),
               (n)-[:`origins:start`]->(s),
               (n)-[:`origins:end`]->(e),
               (s)-[$type { props }]->(e)

        RETURN 1

    '''

    # Removes the `$type` label for the node. Type labels are only set while
    # the node is valid. Deletes the graph edge between the start and end
    # nodes.
    invalidate_statement = '''

        MATCH (n$model {`origins:uuid`: { uuid }})
        REMOVE n$type

        WITH n

        MATCH (n)-[:`origins:start`]->(s$start_model),
              (n)-[:`origins:end`]->(e$end_model),
              (s)-[r$type]->(e)

        DELETE r
        RETURN 1

    '''

    dependency_edge_statement = '''

        MATCH (s$start_model {`origins:uuid`: { start }}),
              (e$end_model {`origins:uuid`: { end }})

        CREATE (s)-[:`origins:dependency`]->(e)

    '''

    def __init__(self, start=None, end=None, id=None, type=None, label=None,
                 description=None, uuid=None, time=None, sha1=None,
                 properties=None, model=None, dependence=None, direction=None,
                 invalidation=None):

        super(Edge, self).__init__(
            uuid=uuid,
            id=id,
            type=type,
            label=label,
            description=description,
            properties=properties,
            time=time,
            sha1=sha1,
            invalidation=invalidation,
        )

        if direction is None:
            direction = DIRECTED

        if dependence is None:
            dependence = NO_DEPENDENCE

        self.direction = direction
        self.dependence = dependence

        # Initialize as nodes assuming these are UUIDs
        if start and not isinstance(start, self.start_model):
            start = self.start_model(uuid=start)

        if end and not isinstance(end, self.end_model):
            end = self.end_model(uuid=end)

        self.start = start
        self.end = end

    @classmethod
    def parse(cls, attrs, invalidation=None, start=None, end=None):
        if isinstance(start, dict):
            start = cls.start_model.parse(start)

        if isinstance(end, dict):
            end = cls.end_model.parse(end)

        return cls(start=start, end=end, invalidation=invalidation,
                   **unpack(attrs))

    def _derive_attrs(self):
        return {
            'id': self.id,
            'label': self.label,
            'type': self.type,
            'model': self.model,
            'start': self.start,
            'end': self.end,
            'description': self.description,
            'direction': self.direction,
            'dependence': self.dependence,
        }

    def to_dict(self):
        attrs = super(Edge, self).to_dict()

        attrs['start'] = self.start.uuid
        attrs['end'] = self.end.uuid
        attrs['dependence'] = self.dependence
        attrs['direction'] = self.direction

        return attrs

    @classmethod
    def match_query(cls, predicate=None, limit=None, skip=None, type=None):

        return traverse.match(cls.match_statement,
                              predicate=pack(predicate),
                              limit=limit,
                              skip=skip,
                              type=type,
                              start_model=cls.start_model.model_name,
                              end_model=cls.end_model.model_name,
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
                               start_model=cls.start_model.model_name,
                               end_model=cls.end_model.model_name,
                               model=cls.model_name)

    @classmethod
    def get_query(cls, uuid):
        statement = utils.prep(cls.get_statement,
                               model=cls.model_name,
                               start_model=cls.start_model.model_name,
                               end_model=cls.end_model.model_name)

        return {
            'statement': statement,
            'parameters': {
                'uuid': uuid,
            }
        }

    # Implemented by parent
    # def exists_query(): pass
    # def exists_by_id_query(): pass

    @classmethod
    def add_query(cls, node):
        model_names = cls.model_names(node)

        statement = utils.prep(cls.add_statement,
                               type=node.type,
                               model=model_names,
                               start_model=cls.start_model.model_name,
                               end_model=cls.end_model.model_name)

        return {
            'statement': statement,
            'parameters': {
                'attrs': pack(node.to_dict()),
                'start': node.start.uuid,
                'end': node.end.uuid,
                'props': node.properties or {},
            },
        }

    @classmethod
    def invalidate_query(cls, node):
        statement = utils.prep(cls.invalidate_statement,
                               type=node.type,
                               model=node.model_name,
                               start_model=cls.start_model.model_name,
                               end_model=cls.end_model.model_name)

        return {
            'statement': statement,
            'parameters': {
                'uuid': node.uuid
            },
        }

    # Implemented by parent
    # def match(): pass
    # def search(): pass
    # def get(): pass
    # def get_by_id(): pass
    # def exists(): pass
    # def exists_by_id(): pass

    @classmethod
    def _validate(cls, node, tx):
        if node.direction not in DIRECTION_TYPES:
            raise ValidationError('direction not valid. choices are: {}'
                                  .format(', '.join(DIRECTION_TYPES)))

        if node.dependence not in DEPENDENCE_TYPES:
            raise ValidationError('dependence not valid: choices are: {}'
                                  .format(', '.join(DEPENDENCE_TYPES)))

        start = node.start
        end = node.end

        if not start:
            raise ValidationError('start node required')

        if not end:
            raise ValidationError('end node required')

        if not isinstance(start, cls.start_model):
            raise ValidationError('start node must be a {}'
                                  .format(cls.start_model.model_name))

        if not isinstance(end, cls.end_model):
            raise ValidationError('end node must be a {}'
                                  .format(cls.end_model.model_name))

        try:
            start = start.get(start.uuid, tx)
        except DoesNotExist as e:
            raise ValidationError(str(e))

        if start.invalidation:
            raise ValidationError('invalid start node {}'.format(start))

        try:
            end = end.get(end.uuid, tx)
        except DoesNotExist as e:
            raise ValidationError(str(e))

        if end.invalidation:
            raise ValidationError('invalid end node {}'.format(end))

    @classmethod
    def _add(cls, node, validate, tx):
        query = cls.add_query(node)
        defer = not validate

        if defer:
            tx.send(query, defer=defer)
        elif not tx.send(query):
            raise Exception('did not successfully add {}'.format(node))

        if node.dependence == MUTUAL_DEPENDENCE \
                or node.dependence == FORWARD_DEPENDENCE:

            statement = utils.prep(cls.dependency_edge_statement,
                                   start_model=node.start.model_name,
                                   end_model=node.end.model_name)

            tx.send({
                'statement': statement,
                'parameters': {
                    'start': node.start.uuid,
                    'end': node.end.uuid,
                }
            }, defer=defer)

        if node.dependence == MUTUAL_DEPENDENCE \
                or node.dependence == INVERSE_DEPENDENCE:

            statement = utils.prep(cls.dependency_edge_statement,
                                   start_model=node.end.model_name,
                                   end_model=node.start.model_name)

            tx.send({
                'statement': statement,
                'parameters': {
                    'start': node.end.uuid,
                    'end': node.start.uuid,
                }
            }, defer=defer)

        prov_spec = provenance.add(node.uuid)

        return provenance.load(prov_spec, tx=tx)

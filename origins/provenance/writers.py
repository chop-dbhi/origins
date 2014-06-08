from __future__ import unicode_literals

import sys
from collections import defaultdict
from ..graph import cypher
from ..graph import neo4j
from .identifier import QualifiedName
from .model import Relation, Namespace, Mention
from .constants import ORIGINS_REL_SEMANTICS, \
    ORIGINS_ATTR_PREFIX, ORIGINS_ATTR_URI, PROV_ATTR_GENERAL_ENTITY, \
    PROV_ATTR_BUNDLE, ORIGINS_REL_DESCRIPTION


try:
    str = unicode
except NameError:
    pass


class InvalidProvenance(Exception):
    pass


class RefGen(object):
    "Reference generator."
    def __init__(self, prefix='x'):
        self.prefix = prefix
        self.counter = 0

    def __call__(self):
        c = self.counter
        self.counter += 1
        return self.prefix + str(c)


class Writer(object):
    """A writer is an interface that prepares and emits Cypher statements as
    provenance data is added to it. The following statement types are handled:

    - Non-namespace nodes are created
    - Relationships between nodes are created for relations/events
    - Namespaces that have been referenced are merged
    - Relationships between nodes and the namespaces they referenced
      are created

    Relationships between nodes will rely on the nodes's unique pattern
    (usually via the `origins:uuid` attribute:

        MATCH (x0:`prov:Relation`:`prov:Usage` {`origins:uuid`: 'b87cef'}),
              (x1:`prov:Entity` {`origins:uuid`: 'eb23ae'}),
              (x2:`prov:Activity` {`origins:uuid`: '7c3b24'})

        CREATE (x0)-[:`prov:entity`]->(x1),
               (x0)-[:`prov:activity`]->(x2);

    This class supports the `with` statement:

        with Writer() as w:
            utils.load_document('document.json', w)

    """
    def __init__(self, bufsize=100):
        # Unique reference generator
        self.gen_ref = RefGen()

        # Map of prov ID to their corresponding UUID nodes
        self.nodes = {}

        # Map of namespaces, bundles associated to a set of names
        self.namespaces = defaultdict(set)
        self.bundles = defaultdict(set)

        # Defer relations for finalize step
        self._relations = []

        # Query parts of the current statement. If
        self._matches = []
        self._merges = []
        self._creates = []

        # Queue of statements
        self.bufsize = bufsize

        self.statements = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.done()

    def _finalize(self):
        """Performs the final preparation of statements which includes all the
        relationships between nodes.
        """
        # Create relationships between relations and related nodes
        # TODO could use bulk import
        for name, instance, bundle in self._relations:
            try:
                self._add_relation(name, instance, bundle)
            except Exception as e:
                raise InvalidProvenance(str(e) + ' for relation `{}`'
                                        .format(name))

        for ns in self.namespaces:
            instance = Namespace({
                ORIGINS_ATTR_URI: ns.uri,
                ORIGINS_ATTR_PREFIX: ns.prefix,
            })

            ns_ref = self.gen_ref()
            ns_node = instance.match_node(ns_ref)
            self.nodes[(ns, None)] = (ns_ref, ns_node)

            self._merges.append((ns_ref, ns_node, instance.props))

        self.queue()

        # Link namespaces to nodes
        for ns, keys in self.namespaces.items():
            start = self.nodes[(ns, None)]

            for key in keys:
                end = self.nodes[key]
                self._add_rel(start, ORIGINS_REL_SEMANTICS, end)

        # Links bundles to nodes
        for b, keys in self.bundles.items():
            start = self.nodes[(b, None)]

            for key in keys:
                end = self.nodes[key]
                self._add_rel(start, ORIGINS_REL_DESCRIPTION, end)

    def _add_rel(self, start, rel, end):
        start_ref, start_node = start
        end_ref, end_node = end

        self._matches.append(start_node)
        self._matches.append(end_node)
        self._creates.append('({})-[:`{}`]->({})'
                             .format(start_ref, rel, end_ref))

        self.queue()

    def _resolve_name(self, name, bundle=None):
        if bundle:
            try:
                return self.nodes[(name, bundle)]
            except KeyError:
                pass

        try:
            return self.nodes[(name, None)]
        except KeyError:
            pass

        if bundle:
            message = 'name `{}` not defined in bundle `{}` nor document' \
                      .format(name, bundle)
        else:
            message = 'name `{}` not defined in document'.format(name)

        raise InvalidProvenance(message)

    def _relation_rel(self, instance, rel):
        "Gets the relation instance attribute."
        value = instance.props.get(rel)

        if instance.relations[rel] and value is None:
            raise ValueError('relation {} required for {}'
                             .format(rel, instance.__class__.__name__))

        return value

    def _add_relation(self, name, instance, bundle):
        start = self._resolve_name(name, bundle)

        # Special handling for mention. The `generalEntity` is located in
        # the bundle specified by the instance attribute `bundle`.
        if isinstance(instance, Mention):
            for rel in instance.relations:
                end_name = self._relation_rel(instance, rel)

                if end_name:
                    if rel == PROV_ATTR_GENERAL_ENTITY:
                        _bundle = instance.props[PROV_ATTR_BUNDLE]
                        end = self._resolve_name(end_name, _bundle)
                    else:
                        end = self._resolve_name(end_name, bundle)

                    self._add_rel(start, rel, end)
            return

        for rel in instance.relations:
            end_name = self._relation_rel(instance, rel)

            if end_name:
                end = self._resolve_name(end_name, bundle)
                self._add_rel(start, rel, end)

    def done(self):
        "Done is called once all nodes for this batch has been added."
        self._finalize()
        self.flush()
        self.close()

    def write(self, statements):
        "Takes a batch of statements and writes it to the output stream."
        for s in statements:
            sys.stdout.write(s + ';\n')

        sys.stdout.flush()

    def close(self):
        """Performs and close-like operations such as closing a file handler
        or committing a transaction.
        """

    def flush(self):
        "Flushes the statements by writing them, then clearing the queue."
        self.write(self.statements[:])
        self.statements = []

    def queue(self):
        "Constructs a statement based on the components and queues it."
        query = []

        if self._matches:
            query.append('MATCH ' +
                         ', '.join(str(s) for s in set(self._matches)))
            self._matches = []

        if self._merges:
            for ref, pattern, props in self._merges:
                query.append('MERGE {}'.format(pattern))
                query.append('ON CREATE SET {} = {}'
                             .format(ref, cypher.map_string(props)))

            self._merges = []

        if self._creates:
            query.append('CREATE ' + ', '.join(str(s) for s in self._creates))
            self._creates = []

        if query:
            self.statements.append(' '.join(str(s) for s in query))

            # Flush once the number of statements have reached the buffer size
            if len(self.statements) >= self.bufsize:
                self.flush()

    def add(self, name, instance, bundle):
        """Creates a node based off the model instance and the qualified name.
        If the qualified is associated with a namespace, it will be linked.
        """
        ref = self.gen_ref()
        node = instance.node(ref)
        self._creates.append(node)

        pair = (name, bundle)

        # Add match node for later reference by dependents
        self.nodes[pair] = (ref, instance.match_node(ref))

        if bundle:
            self.bundles[bundle].add(pair)

        # Namespaces this instance relies on
        if isinstance(name, QualifiedName) and name.namespace:
            self.namespaces[name.namespace].add(pair)

        for key, value in instance.props.items():
            if isinstance(key, QualifiedName) and key.namespace:
                self.namespaces[key.namespace].add(pair)
            elif isinstance(value, QualifiedName) and value.namespace:
                self.namespaces[value.namespace].add(pair)

        # Creating relationships between relations are deferred since
        # they may depend on instances that have not yet been processed
        if isinstance(instance, Relation):
            self._relations.append((name, instance, bundle))

        self.queue()


class Neo4jWriter(Writer):
    def __init__(self, tx=None, *args, **kwargs):
        # Create a managed transaction if none is passed
        if tx is None:
            managed = True
            tx = neo4j.Transaction()
        else:
            managed = False

        self.tx = tx
        self.managed = managed

        super(Neo4jWriter, self).__init__(*args, **kwargs)

    def write(self, statements):
        self.tx.send(statements)

    def close(self):
        if self.managed:
            self.tx.commit()


class StreamWriter(Writer):
    def __init__(self, stream, delimiter='\n'):
        self.stream = stream
        self.delimiter = delimiter
        super(StreamWriter, self).__init__()

    def write(self, statements):
        for statement in statements:
            self.stream.write(str(statement) + ';' + self.delimiter)

        if hasattr(self.stream, 'flush'):
            self.stream.flush()

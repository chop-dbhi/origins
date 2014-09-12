from origins.graph.nodes import Node
from origins.graph import neo4j, cypher
from origins.exceptions import DoesNotExist, ValidationError
from .base import TestCase


# Utilities for Node and edges in the graph
def _nodes(labels=None, count=True):
    labels = cypher.labels(labels)
    result = neo4j.tx.send('MATCH (n{}) RETURN n'.format(labels))

    if count:
        return len(result)

    # Unpack nested lists
    return [r[0] for r in result]


class NodeTestCase(TestCase):
    def test_match(self):
        n0 = Node.add(type='Foo')
        m0 = Node.add(type='Bar')

        self.assertEqual(Node.match(), [n0, m0])
        self.assertEqual(Node.match(type='Foo'), [n0])
        self.assertEqual(Node.match(type='Bar'), [m0])

    def test_search(self):
        n0 = Node.add(label='Hello world')
        m0 = Node.add(label='Hello moto')

        # Metadata
        predicate = {'label': 'Hello.*'}
        self.assertEqual(Node.search(predicate), [n0, m0])

        p0 = Node.add(properties={
            'color': 'blue',
            'size': 10,
        })

        # Properties
        predicate = {'properties': {'color': 'bl.*', 'size': 10}}
        self.assertEqual(Node.search(predicate), [p0])

    def test_get(self):
        self.assertRaises(DoesNotExist, Node.get, 'abc123')

        n = Node.add()
        _n = Node.get(n.uuid)

        self.assertEqual(n, _n)

    def test_get_by_id(self):
        self.assertRaises(DoesNotExist, Node.get_by_id, 'abc123')

        n0 = Node.add()
        n0_ = Node.get_by_id(n0.id)

        self.assertEqual(n0, n0_)

        # ID is persisted
        n1 = Node.set(n0.uuid, label='n0')
        self.assertEqual(n0.id, n1.id)

        n1_ = Node.get_by_id(n0.id)

        self.assertEqual(n1, n1_)

        # Remove makes it no longer visible
        Node.remove(n1.uuid)
        self.assertRaises(DoesNotExist, Node.get_by_id, n0.id)

    # TODO move to provenance test case
    def test_add(self):
        Node.add()

        self.assertEqual(_nodes('origins:Continuant'), 1)
        self.assertEqual(_nodes('origins:Node'), 1)
        self.assertEqual(_nodes('prov:Generation'), 1)

    def test_set(self):
        n0 = Node.add()

        n0_ = Node.set(n0.uuid)

        # Nothing changed
        self.assertIsNone(n0_)

        n1 = Node.set(n0.uuid, label='n0', type='Term')

        self.assertEqual(n1.diff(n0), {
            'label': (None, 'n0'),
            'type': ('Node', 'Term'),
        })
        self.assertEqual(n1.id, n0.id)
        self.assertNotEqual(n1.uuid, n0.uuid)

        self.assertEqual(_nodes('prov:Derivation'), 1)
        self.assertEqual(_nodes('prov:Invalidation'), 1)

    def test_set_not_exists(self):
        self.assertRaises(DoesNotExist, Node.set, 'abc123')

    def test_set_invalid(self):
        n0 = Node.add()
        Node.remove(n0.uuid)

        self.assertRaises(ValidationError, Node.set, n0.uuid)

    def test_remove(self):
        n0 = Node.add()

        n1 = Node.remove(n0.uuid)

        self.assertEqual(n1.uuid, n0.uuid)
        self.assertEqual(_nodes('prov:Invalidation'), 1)

    def test_remove_not_exists(self):
        self.assertRaises(DoesNotExist, Node.remove, 'abc123')

    def test_remove_invalid(self):
        n0 = Node.add()

        Node.remove(n0.uuid)

        self.assertRaises(ValidationError, Node.remove, n0.uuid)

from origins.graph import neo4j, nodes
from origins.exceptions import DoesNotExist
from origins.graph import cypher
from ._base import TestCase


# Utilities for nodes and edges in the graph
def _nodes(labels=None, count=True):
    labels = cypher.labels(labels)
    result = neo4j.tx.send('MATCH (n{}) RETURN n'.format(labels))

    if count:
        return len(result)

    # Unpack nested lists
    return [r[0] for r in result]


class NodeTestCase(TestCase):
    def test_get(self):
        self.assertRaises(DoesNotExist, nodes.get, 'abc123')

        n = nodes.add()
        _n = nodes.get(n.uuid)

        self.assertEqual(n, _n)

    def test_get_by_id(self):
        self.assertRaises(DoesNotExist, nodes.get_by_id, 'abc123')

        n0 = nodes.add()
        n0_ = nodes.get_by_id(n0.id)

        self.assertEqual(n0, n0_)

        # ID is persisted
        n1 = nodes.set(n0.uuid, label='n0')
        self.assertEqual(n0.id, n1.id)
        n1_ = nodes.get_by_id(n0.id)

        self.assertEqual(n1, n1_)

        # Remove makes it no longer visible
        nodes.remove(n1.uuid)
        self.assertRaises(DoesNotExist, nodes.get_by_id, n0.id)

    # TODO move to provenance test case
    def test_add(self):
        nodes.add()

        self.assertEqual(_nodes('origins:Node'), 1)
        self.assertEqual(_nodes('prov:Generation'), 1)

    def test_type(self):
        n = nodes.add(type='Term')
        self.assertEqual(_nodes('Term'), 1)

        # Normal UUID-based lookup still works (since it is unique)
        n_ = nodes.get(n.uuid)
        self.assertEqual(n, n_)

        # ID with type works
        n_ = nodes.get_by_id(n.id, type='Term')
        self.assertEqual(n, n_)

        self.assertRaises(DoesNotExist, nodes.get_by_id, n.id)
        self.assertEqual(n, n_)

    def test_set(self):
        n0 = nodes.add()
        n0_ = nodes.set(n0.uuid)

        # Nothing changed
        self.assertIsNone(n0_)

        n1 = nodes.set(n0.uuid, label='n0')

        self.assertEqual(n1.diff(n0), {'label': (None, 'n0')})
        self.assertEqual(n1.id, n0.id)
        self.assertNotEqual(n1.uuid, n0.uuid)

        self.assertEqual(_nodes('prov:Derivation'), 1)
        self.assertEqual(_nodes('prov:Invalidation'), 1)

    def test_remove(self):
        n0 = nodes.add()
        n1 = nodes.remove(n0.uuid)

        self.assertEqual(n1.uuid, n0.uuid)
        self.assertEqual(_nodes('prov:Invalidation'), 1)

    def test_match(self):
        n0 = nodes.add(type='Foo')
        m0 = nodes.add(type='Bar')

        # None of the default type
        self.assertEqual(nodes.match(), [])
        self.assertEqual(nodes.match(type='Foo'), [n0])
        self.assertEqual(nodes.match(type='Bar'), [m0])

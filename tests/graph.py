import unittest
from origins.graph import Node


class GraphTestCase(unittest.TestCase):
    def setUp(self):
        self.n0 = Node()
        self.n1 = Node()
        self.n2 = Node()
        self.n3 = Node()

    def test_props(self):
        self.n0['one'] = 1
        self.assertEqual(self.n0['one'], 1)
        del self.n0['one']
        self.assertFalse('one' in self.n0)

    def test_relate(self):
        r0 = self.n0.relate(self.n1, 'CONTAINS')
        r1 = self.n0.relate(self.n1, 'RELATED')
        r2 = self.n0.relate(self.n2, 'CONTAINS')
        r3 = self.n0.relate(self.n3, 'RELATED')

        self.assertTrue(self.n0.related(self.n1))
        self.assertTrue(self.n0.related(self.n2))
        self.assertTrue(self.n0.related(self.n3, 'RELATED'))

        self.assertEqual(sorted(self.n0.rels()), [r0, r1, r2, r3])
        self.assertEqual(self.n0.rels(node=self.n1), [r0, r1])
        self.assertEqual(self.n0.rels(type='CONTAINS'), [r0, r2])
        self.assertEqual(self.n0.rels(node=self.n1, type='RELATED'), [r1])
        self.assertEqual(self.n0.rels(node=self.n0, type='FOOBAR'), [])

    def test_relate_type_once(self):
        r0 = self.n0.relate(self.n1, 'CONTAINS')
        r0p = self.n0.relate(self.n1, 'CONTAINS', {'one': 1})
        self.assertEqual(r0p, r0)
        self.assertEqual(r0p.props, {'one': 1})

    def test_unrelate_node(self):
        self.n0.relate(self.n1, 'CONTAINS')
        self.n0.relate(self.n1, 'RELATED')
        self.n0.relate(self.n2, 'CONTAINS')
        self.n0.relate(self.n2, 'RELATED')
        self.n0.relate(self.n3, 'CONTAINS')

        # Node
        self.assertEqual(self.n0.unrelate(self.n1), 2)
        # Type
        self.assertEqual(self.n0.unrelate(type='RELATED'), 1)
        # Node and Type
        self.assertEqual(self.n0.unrelate(self.n3, type='CONTAINS'), 1)
        # All
        self.assertEqual(self.n0.unrelate(), 1)

        self.assertFalse(self.n0.related(self.n1))
        self.assertFalse(self.n0.related(self.n2))
        self.assertFalse(self.n0.related(self.n3))

        self.assertEqual(self.n0.rels(), [])

    def test_rels(self):
        r0 = self.n0.relate(self.n1, 'CONTAINS', {'container': 'table'})
        r1 = self.n0.relate(self.n2, 'CONTAINS', {'container': 'column'})
        r2 = self.n0.relate(self.n2, 'RELATED', {'container': 'table'})

        self.assertEqual(self.n0.rels().filter('container', 'table'), [r0, r2])

        rels = self.n0.rels(type='CONTAINS')

        self.assertEqual(rels.filter('container', 'table'), [r0])
        self.assertEqual(rels.filter('container', 'column'), [r1])

        # Arbitrary filter function
        self.assertEqual(rels.filter(lambda r: r.id == r0.id), [r0])

        # Only functions and strings
        self.assertRaises(TypeError, rels.filter, 10)

    def test_nodes(self):
        self.n0.relate(self.n1, 'CONTAINS')
        self.n0.relate(self.n1, 'RELATED')
        self.n0.relate(self.n2, 'CONTAINS')
        self.n0.relate(self.n2, 'RELATED')
        self.n0.relate(self.n3, 'CONTAINS')

        # Get all relationships from n0
        rels = self.n0.rels()
        nodes = rels.nodes()
        # Gets distinct set of end nodes of rels
        self.assertEqual(nodes, [self.n1, self.n2, self.n3])

        # Indexed by their bytes representation
        self.assertEqual(nodes[bytes(self.n1)], self.n1)

        regexp = r'|'.join([bytes(self.n1), bytes(self.n2)])
        self.assertEqual(nodes.match(regexp), [self.n1, self.n2])

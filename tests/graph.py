import unittest
from origins.graph import Node


class GraphTestCase(unittest.TestCase):
    def setUp(self):
        self.n0 = Node()
        self.n1 = Node()
        self.n2 = Node()
        self.n3 = Node()

    def test_relate(self):
        r0 = self.n0.relate(self.n1, 'CONTAINS')
        r1 = self.n0.relate(self.n1, 'RELATED')
        r2 = self.n0.relate(self.n2, 'CONTAINS')
        r3 = self.n0.relate(self.n3, 'RELATED')

        self.assertTrue(self.n0.related(self.n1))
        self.assertTrue(self.n0.related(self.n2))
        self.assertTrue(self.n0.related(self.n3))

        self.assertEqual(self.n0.rels(), [r0, r1, r2, r3])
        self.assertEqual(self.n0.rels(node=self.n1), [r0, r1])
        self.assertEqual(self.n0.rels(type='CONTAINS'), [r0, r2])
        self.assertEqual(self.n0.rels(node=self.n1, type='RELATED'), [r1])
        self.assertEqual(self.n0.rels(node=self.n0, type='FOOBAR'), [])

    def test_unrelate(self):
        self.n0.relate(self.n1, 'CONTAINS')
        self.n0.relate(self.n1, 'RELATED')
        self.n0.relate(self.n2, 'CONTAINS')
        self.n0.relate(self.n3, 'RELATED')

        self.assertEqual(self.n0.unrelate(self.n1, 'RELATED'), 1)
        self.assertEqual(self.n0.unrelate(self.n2, 'CONTAINS'), 1)
        self.assertEqual(self.n0.unrelate(self.n2), 0)
        self.assertEqual(self.n0.unrelate(), 2)

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
        self.assertEqual(rels.filter(lambda r: r.id % 2 == 0), [r0])

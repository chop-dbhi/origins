import unittest
from origins.graph import neo4j, nodes, edges


class NodeTestCase(unittest.TestCase):
    def setUp(self):
        neo4j.purge()

    def test_get_node(self):
        self.assertRaises(ValueError, nodes.get, 'abc123')

        n = nodes.add()
        _n = nodes.get(n)

        self.assertEqual(n['data'], _n['data'])

    def test_get_node_by_id(self):
        self.assertRaises(ValueError, nodes.get_by_id, 'abc123')

        n = nodes.add()
        _n = nodes.get_by_id(n)

        self.assertEqual(n['data'], _n['data'])

    def test_add_node(self):
        n = nodes.add()

        self.assertIn('data', n)
        self.assertIn('prov', n)
        self.assertIn('time', n)
        self.assertIn('perf', n)

        self.assertIn('uuid', n['data'])
        self.assertIn('id', n['data'])

    def test_change_node(self):
        n0 = nodes.add()
        n1 = nodes.change(n0)

        self.assertEqual(n1['data']['id'], n0['data']['id'])
        self.assertNotEqual(n1['data']['uuid'], n0['data']['uuid'])

    def test_remove_node(self):
        n0 = nodes.add()
        n1 = nodes.remove(n0)

        self.assertEqual(n1['data']['uuid'], n0['data']['uuid'])


class EdgeTestCase(unittest.TestCase):
    def setUp(self):
        neo4j.purge()

        self.a0 = nodes.add()
        self.b0 = nodes.add()

    def test_get_edge(self):
        self.assertRaises(ValueError, edges.get, 'abc123')

        e = edges.add(self.a0, self.b0)
        _e = edges.get(e)

        self.assertEqual(e['data'], _e['data'])

    def test_get_edge_by_id(self):
        self.assertRaises(ValueError, edges.get_by_id, 'abc123')

        n = edges.add(self.a0, self.b0)
        _n = edges.get_by_id(n)

        self.assertEqual(n['data'], _n['data'])

    def test_add_edge(self):
        e = edges.add(self.a0, self.b0)

        self.assertIn('data', e)
        self.assertIn('prov', e)
        self.assertIn('time', e)
        self.assertIn('perf', e)

        self.assertIn('uuid', e['data'])
        self.assertIn('id', e['data'])

    def test_change_edge(self):
        e0 = edges.add(self.a0, self.b0)
        e1 = edges.change(e0)

        self.assertEqual(e1['data']['id'], e0['data']['id'])
        self.assertNotEqual(e1['data']['uuid'], e0['data']['uuid'])

    def test_remove_edge(self):
        e0 = edges.add(self.a0, self.b0)
        e1 = edges.remove(e0)

        self.assertEqual(e1['data']['uuid'], e0['data']['uuid'])


class DependencyTestCase(unittest.TestCase):
    def setUp(self):
        neo4j.purge()

        self.a0 = nodes.add()
        self.b0 = nodes.add()

    def test_outbound_edges(self):
        e0 = edges.add(self.a0, self.b0)

        # Causes it's outbound nodes to be updated, i.e. e0
        nodes.change(self.a0)

        e1 = edges.get_by_id(e0)

        self.assertNotEqual(e1['data']['uuid'], e0['data']['uuid'])
        self.assertEqual(e1['data']['id'], e0['data']['id'])

    def test_inbound_edges(self):
        e0 = edges.add(self.a0, self.b0)

        b1 = nodes.change(self.b0)

        e1 = edges.get_by_id(e0)
        self.assertEqual(e1['data']['uuid'], e0['data']['uuid'])

        #
        o = edges.get_outdated()

        self.assertEqual(len(o), 1)
        self.assertEqual(o[0]['latest']['uuid'], b1['data']['uuid'])

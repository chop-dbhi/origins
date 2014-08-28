import unittest
from origins.graph import neo4j, core


class NodeTestCase(unittest.TestCase):
    def setUp(self):
        neo4j.purge()

    def test_get_node(self):
        self.assertRaises(ValueError, core.get_node, 1)

        n = core.add_node()
        _n = core.get_node(n)

        self.assertEqual(n['data'], _n['data'])

    def test_get_node_by_id(self):
        self.assertRaises(ValueError, core.get_node_by_id, 'abc123')

        n = core.add_node()
        _n = core.get_node_by_id(n)

        self.assertEqual(n['data'], _n['data'])

    def test_add_node(self):
        n = core.add_node()

        self.assertIn('data', n)
        self.assertIn('prov', n)
        self.assertIn('time', n)
        self.assertIn('perf', n)

        self.assertIn('uuid', n['data'])
        self.assertIn('id', n['data'])

    def test_change_node(self):
        n0 = core.add_node()
        n1 = core.change_node(n0)

        self.assertEqual(n1['data']['id'], n0['data']['id'])
        self.assertNotEqual(n1['data']['uuid'], n0['data']['uuid'])

    def test_remove_node(self):
        n0 = core.add_node()
        n1 = core.remove_node(n0)

        self.assertEqual(n1['data']['uuid'], n0['data']['uuid'])


class EdgeTestCase(unittest.TestCase):
    def setUp(self):
        neo4j.purge()

        self.a0 = core.add_node()
        self.b0 = core.add_node()

    def test_get_edge(self):
        self.assertRaises(ValueError, core.get_edge, 1)

        e = core.add_edge(self.a0, self.b0)
        _e = core.get_edge(e)

        self.assertEqual(e['data'], _e['data'])

    def test_get_edge_by_id(self):
        self.assertRaises(ValueError, core.get_edge_by_id, 'abc123')

        n = core.add_edge(self.a0, self.b0)
        _n = core.get_edge_by_id(n)

        self.assertEqual(n['data'], _n['data'])

    def test_add_edge(self):
        e = core.add_edge(self.a0, self.b0)

        self.assertIn('data', e)
        self.assertIn('prov', e)
        self.assertIn('time', e)
        self.assertIn('perf', e)

        self.assertIn('uuid', e['data'])
        self.assertIn('id', e['data'])

    def test_change_edge(self):
        e0 = core.add_edge(self.a0, self.b0)
        e1 = core.change_edge(e0)

        self.assertEqual(e1['data']['id'], e0['data']['id'])
        self.assertNotEqual(e1['data']['uuid'], e0['data']['uuid'])

    def test_remove_edge(self):
        e0 = core.add_edge(self.a0, self.b0)
        e1 = core.remove_edge(e0)

        self.assertEqual(e1['data']['uuid'], e0['data']['uuid'])


class EdgeUpdatesTestCase(unittest.TestCase):
    def setUp(self):
        neo4j.purge()

        self.a0 = core.add_node()
        self.b0 = core.add_node()

    def test_edge_update(self):
        e0 = core.add_edge(self.a0, self.b0)

        # Causes it's outbound nodes to be updated, i.e. e0
        core.change_node(self.a0)

        e1 = core.get_edge_by_id(e0)

        self.assertNotEqual(e1['data']['uuid'], e0['data']['uuid'])
        self.assertEqual(e1['data']['id'], e0['data']['id'])

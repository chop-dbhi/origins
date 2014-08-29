import unittest
from origins.graph import neo4j, nodes, edges


class NodeTestCase(unittest.TestCase):
    def setUp(self):
        neo4j.purge()

    def test_get(self):
        self.assertRaises(ValueError, nodes.get, 'abc123')

        n = nodes.add()
        _n = nodes.get(n)

        self.assertEqual(n['data'], _n['data'])

    def test_get_by_id(self):
        self.assertRaises(ValueError, nodes.get_by_id, 'abc123')

        n = nodes.add()
        _n = nodes.get_by_id(n)

        self.assertEqual(n['data'], _n['data'])

    def test_add(self):
        n = nodes.add()

        self.assertIn('data', n)
        self.assertIn('prov', n)
        self.assertIn('time', n)
        self.assertIn('perf', n)

        self.assertIn('uuid', n['data'])
        self.assertIn('id', n['data'])

    def test_set(self):
        n0 = nodes.add()
        n0_ = nodes.set(n0)

        # Nothing changed
        self.assertIsNone(n0_)

        n1 = nodes.set(n0, force=True)

        self.assertEqual(n1['diff'], {})
        self.assertEqual(n1['data']['id'], n0['data']['id'])
        self.assertNotEqual(n1['data']['uuid'], n0['data']['uuid'])

        # Something changed
        n2 = nodes.set(n0, {'label': 'a'})

        self.assertEqual(n2['diff'], {'label': (None, 'a')})
        self.assertEqual(n2['data']['id'], n0['data']['id'])
        self.assertNotEqual(n2['data']['uuid'], n0['data']['uuid'])

    def test_remove(self):
        n0 = nodes.add()
        n1 = nodes.remove(n0)

        self.assertEqual(n1['data']['uuid'], n0['data']['uuid'])


class EdgeTestCase(unittest.TestCase):
    def setUp(self):
        neo4j.purge()

        self.a0 = nodes.add()
        self.b0 = nodes.add()

    def test_get(self):
        self.assertRaises(ValueError, edges.get, 'abc123')

        e = edges.add(self.a0, self.b0)
        _e = edges.get(e)

        self.assertEqual(e['data'], _e['data'])

    def test_get_by_id(self):
        self.assertRaises(ValueError, edges.get_by_id, 'abc123')

        n = edges.add(self.a0, self.b0)
        _n = edges.get_by_id(n)

        self.assertEqual(n['data'], _n['data'])

    def test_add(self):
        e = edges.add(self.a0, self.b0)

        self.assertIn('data', e)
        self.assertIn('prov', e)
        self.assertIn('time', e)
        self.assertIn('perf', e)

        self.assertIn('uuid', e['data'])
        self.assertIn('id', e['data'])

    def test_real_edge(self):
        e = edges.add(self.a0, self.b0, {
            'type': 'LIKES',
            'properties': {'foo': 1}
        })

        query = {
            'statement': '''
                MATCH (:`origins:Node` {`origins:uuid`: { start }})-[r:LIKES]->(:`origins:Node` {`origins:uuid`: { end }})
                RETURN r
            ''',  # noqa
            'parameters': {
                'start': self.a0['data']['uuid'],
                'end': self.b0['data']['uuid'],
            }
        }

        # Created
        result = neo4j.tx.send(query)
        self.assertEqual(result[0][0], {'foo': 1})

        edges.remove(e)

        # Deleted
        result = neo4j.tx.send(query)
        self.assertEqual(result, [])

    def test_set(self):
        e0 = edges.add(self.a0, self.b0)
        e0_ = edges.set(e0)

        # Nothing changed
        self.assertIsNone(e0_)

        e1 = edges.set(e0, force=True)

        self.assertEqual(e1['diff'], {})
        self.assertEqual(e1['data']['id'], e0['data']['id'])
        self.assertNotEqual(e1['data']['uuid'], e0['data']['uuid'])

        # Something changed
        e2 = edges.set(e1, {'label': 'a'})

        self.assertEqual(e2['diff'], {'label': (None, 'a')})
        self.assertEqual(e2['data']['id'], e0['data']['id'])
        self.assertNotEqual(e2['data']['uuid'], e0['data']['uuid'])

    def test_remove(self):
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
        nodes.set(self.a0, force=True)

        e1 = edges.get_by_id(e0)

        self.assertNotEqual(e1['data']['uuid'], e0['data']['uuid'])
        self.assertEqual(e1['data']['id'], e0['data']['id'])

    def test_inbound_edges(self):
        e0 = edges.add(self.a0, self.b0)

        b1 = nodes.set(self.b0, force=True)

        e1 = edges.get_by_id(e0)
        self.assertEqual(e1['data']['uuid'], e0['data']['uuid'])

        #
        o = edges.get_outdated()

        self.assertEqual(len(o), 1)
        self.assertEqual(o[0]['latest']['uuid'], b1['data']['uuid'])

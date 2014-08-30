import unittest
from origins.graph import neo4j, nodes, edges
from origins.graph.cypher import labels_string


# Utilities for nodes and edges in the graph
def _nodes(labels=None, count=True):
    labels = labels_string(labels)
    result = neo4j.tx.send('MATCH (n{}) RETURN n'.format(labels))

    if count:
        return len(result)

    # Unpack nested lists
    return [r[0] for r in result]


def _edges(type, start=None, end=None, count=True):
    f = {'t': type, 's': '', 'e': ''}
    parameters = {}

    if start:
        f['s'] = ':`origins:Node` {`origins:uuid`: { start }}'
        f['e'] = ':`origins:Node` {`origins:uuid`: { end }}'

        parameters = {
            'start': start['data']['uuid'],
            'end': end['data']['uuid'],
        }

    query = {
        'statement': 'MATCH ({s})-[r:`{t}`]->({e}) RETURN r'.format(**f),
        'parameters': parameters,
    }

    result = neo4j.tx.send(query)

    if count:
        return len(result)

    # Unpack nested lists
    return [r[0] for r in result]


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

        # Adding one makes it visible
        n0 = nodes.add()
        n0_ = nodes.get_by_id(n0)

        self.assertEqual(n0['data'], n0_['data'])

        # Force set; get by id always returns the latest
        n1 = nodes.set(n0, force=True)
        n1_ = nodes.get_by_id(n0)

        self.assertEqual(n1['data'], n1_['data'])

        # Remove makes it no longer visible
        nodes.remove(n1)
        self.assertRaises(ValueError, nodes.get_by_id, n0)

    def test_add(self):
        n = nodes.add()

        self.assertIn('data', n)
        self.assertIn('prov', n)
        self.assertIn('time', n)
        self.assertIn('perf', n)

        self.assertIn('uuid', n['data'])
        self.assertIn('id', n['data'])

        self.assertEqual(_nodes('origins:Node'), 1)
        self.assertEqual(_nodes('prov:Generation'), 1)

    def test_labels(self):
        # Create node with custom labels
        n = nodes.add(labels=('Foo', 'Bar'))

        # Normall UUID-based lookup still works (since it is unique)
        n_ = nodes.get(n)
        self.assertEqual(n['data'], n_['data'])

        # Combination of labels work
        n_ = nodes.get_by_id(n, labels=('Foo', 'Bar'))
        self.assertEqual(n['data'], n_['data'])

        n_ = nodes.get_by_id(n, labels=('Foo',))
        self.assertEqual(n['data'], n_['data'])

        n_ = nodes.get_by_id(n, labels=('Bar',))
        self.assertEqual(n['data'], n_['data'])

        self.assertEqual(_nodes('Foo'), 1)
        self.assertEqual(_nodes('Bar'), 1)

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

        self.assertEqual(_nodes('prov:Derivation'), 2)
        self.assertEqual(_nodes('prov:Invalidation'), 2)

    def test_remove(self):
        n0 = nodes.add()
        n1 = nodes.remove(n0)

        self.assertEqual(n1['data']['uuid'], n0['data']['uuid'])

        self.assertEqual(_nodes('prov:Invalidation'), 1)


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
        # Adding one makes it visible
        n0 = edges.add(self.a0, self.b0)
        n0_ = edges.get_by_id(n0)

        self.assertEqual(n0['data'], n0_['data'])

        # Force set; get by id always returns the latest
        n1 = edges.set(n0, force=True)
        n1_ = edges.get_by_id(n0)

        self.assertEqual(n1['data'], n1_['data'])

        # Remove makes it no longer visible
        edges.remove(n1)
        self.assertRaises(ValueError, edges.get_by_id, n0)

    def test_add(self):
        e = edges.add(self.a0, self.b0)

        self.assertIn('data', e)
        self.assertIn('prov', e)
        self.assertIn('time', e)
        self.assertIn('perf', e)

        self.assertIn('uuid', e['data'])
        self.assertIn('id', e['data'])

        self.assertEqual(_nodes('origins:Edge'), 1)
        # 2 nodes, 1 edge
        self.assertEqual(_nodes('prov:Generation'), 3)

        # Forms a real edge between the two nodes, defaults to `null` type
        self.assertEqual(_edges('null'), 1)

    def test_real_edge(self):
        e = edges.add(self.a0, self.b0, {
            'type': 'LIKES',
            'properties': {'foo': 1}
        })

        # Created with properties
        self.assertEqual(_edges('LIKES', count=False), [{'foo': 1}])

        edges.remove(e)

        # Deleted
        self.assertEqual(_edges('LIKES'), 0)

    def test_labels(self):
        # Create edge with custom labels
        n = edges.add(self.a0, self.b0, labels=('Foo', 'Bar'))

        # Normall UUID-based lookup still works (since it is unique)
        n_ = edges.get(n)
        self.assertEqual(n['data'], n_['data'])

        # Combination of labels work
        n_ = edges.get_by_id(n, labels=('Foo', 'Bar'))
        self.assertEqual(n['data'], n_['data'])

        n_ = edges.get_by_id(n, labels=('Foo',))
        self.assertEqual(n['data'], n_['data'])

        n_ = edges.get_by_id(n, labels=('Bar',))
        self.assertEqual(n['data'], n_['data'])

        self.assertEqual(_nodes('Foo'), 1)
        self.assertEqual(_nodes('Bar'), 1)

    def test_set(self):
        e0 = edges.add(self.a0, self.b0)
        e0_ = edges.set(e0)

        # Nothing changed
        self.assertIsNone(e0_)

        e1 = edges.set(e0, force=True)

        self.assertEqual(_edges('null', self.a0, self.b0), 1)

        self.assertEqual(e1['diff'], {})
        self.assertEqual(e1['data']['id'], e0['data']['id'])
        self.assertNotEqual(e1['data']['uuid'], e0['data']['uuid'])

        # Something changed
        e2 = edges.set(e1, {'type': 'a'})

        self.assertEqual(e2['diff'], {'type': (None, 'a')})
        self.assertEqual(e2['data']['id'], e0['data']['id'])
        self.assertNotEqual(e2['data']['uuid'], e0['data']['uuid'])

        # Ensure the physical edge was replaced
        self.assertEqual(_edges('null', self.a0, self.b0), 0)
        self.assertEqual(_edges('a', self.a0, self.b0), 1)

        self.assertEqual(_nodes('prov:Derivation'), 2)
        self.assertEqual(_nodes('prov:Invalidation'), 2)

    def test_remove(self):
        e0 = edges.add(self.a0, self.b0)
        e1 = edges.remove(e0)

        self.assertEqual(e1['data']['uuid'], e0['data']['uuid'])

        self.assertEqual(_nodes('prov:Invalidation'), 1)


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

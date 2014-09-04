from origins.exceptions import DoesNotExist
from origins.graph import neo4j, edges, nodes
from origins.graph.model import Edge
from .nodes import _nodes
from .base import TestCase


def _edges(type, start=None, end=None, count=True):
    f = {'t': type, 's': '', 'e': ''}
    parameters = {}

    if start:
        f['s'] = ':`origins:Node` {`origins:uuid`: { start }}'
        f['e'] = ':`origins:Node` {`origins:uuid`: { end }}'

        parameters = {
            'start': start.uuid,
            'end': end.uuid,
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


class EdgeTestCase(TestCase):
    def setUp(self):
        super(EdgeTestCase, self).setUp()

        self.a0 = nodes.add()
        self.b0 = nodes.add()

    def test_get(self):
        self.assertRaises(DoesNotExist, edges.get, 'abc123')

        e = edges.add(self.a0, self.b0)
        _e = edges.get(e.uuid)

        self.assertEqual(e, _e)

    def test_get_by_id(self):
        # Adding one makes it visible
        n0 = edges.add(self.a0, self.b0)
        n0_ = edges.get_by_id(n0.id)

        self.assertEqual(n0, n0_)

        n1 = edges.set(n0.uuid, label='Notable')
        n1_ = edges.get_by_id(n0.id)

        self.assertEqual(n1, n1_)

        # Remove makes it no longer visible
        edges.remove(n1.uuid)
        self.assertRaises(DoesNotExist, edges.get_by_id, n0.id)

    def test_add(self):
        edges.add(self.a0, self.b0)

        self.assertEqual(_nodes('origins:Edge'), 1)
        # 2 nodes, 1 edge
        self.assertEqual(_nodes('prov:Generation'), 3)

        # Forms a real edge between the two nodes, defaults to `null` type
        self.assertEqual(_edges(Edge.DEFAULT_TYPE), 1)

    def test_real_edge(self):
        e = edges.add(self.a0, self.b0, type='likes', properties={'foo': 1})

        # Created with properties
        self.assertEqual(_edges('likes', count=False), [{'foo': 1}])

        edges.remove(e.uuid)

        # Deleted
        self.assertEqual(_edges('likes'), 0)

    def test_set(self):
        e0 = edges.add(self.a0, self.b0)
        e0_ = edges.set(e0.uuid)

        # Nothing changed
        self.assertIsNone(e0_)

        # Something changed
        e1 = edges.set(e0.uuid, type='likes')

        self.assertEqual(e1.diff(e0), {'type': ('related', 'likes')})
        self.assertEqual(e1.id, e0.id)
        self.assertNotEqual(e1.uuid, e0.uuid)

        # Ensure the physical edge was replaced
        self.assertEqual(_edges(Edge.DEFAULT_TYPE, self.a0, self.b0), 0)
        self.assertEqual(_edges('likes', self.a0, self.b0), 1)

        self.assertEqual(_nodes('prov:Derivation'), 1)
        self.assertEqual(_nodes('prov:Invalidation'), 1)

    def test_remove(self):
        e0 = edges.add(self.a0, self.b0)
        e1 = edges.remove(e0.uuid)

        self.assertEqual(e1.uuid, e0.uuid)

        self.assertEqual(_nodes('prov:Invalidation'), 1)

    def test_match(self):
        e0 = edges.add(self.a0, self.b0)
        f0 = edges.add(self.a0, self.b0, type='likes')

        self.assertEqual(edges.match(), [e0])
        self.assertEqual(edges.match(type='likes'), [f0])


class DependencyTestCase(TestCase):
    def setUp(self):
        super(DependencyTestCase, self).setUp()

        self.a0 = nodes.add()
        self.b0 = nodes.add()

    def test_auto_update(self):
        e0 = edges.add(self.a0, self.b0)

        # Causes it's outbound nodes to be updated, i.e. e0
        a1 = nodes.set(self.a0.uuid, label='v2')

        e1 = edges.get_by_id(e0.id)

        # Ensure the start and end nodes are correct
        self.assertEqual(e1.start, a1)
        self.assertEqual(e1.end, self.b0)

        self.assertNotEqual(e1.uuid, e0.uuid)
        self.assertEqual(e1.id, e0.id)

        self.assertEqual(edges.match(), [e1])

    def test_outdated(self):
        e0 = edges.add(self.a0, self.b0)
        b1 = nodes.set(self.b0.uuid, label='v2')

        e1 = edges.get_by_id(e0.id)
        self.assertEqual(e1.uuid, e0.uuid)

        # All outdated edges
        o = edges.outdated()

        self.assertEqual(len(o), 1)

        edge, latest = o[0]
        self.assertEqual(latest.uuid, b1.uuid)

    def test_update(self):
        edges.add(self.a0, self.b0)
        nodes.set(self.b0.uuid, label='v2')

        edges.update(self.a0.uuid)

        o = edges.outdated()
        self.assertEqual(len(o), 0)

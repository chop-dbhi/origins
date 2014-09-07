from origins.exceptions import DoesNotExist, InvalidState, ValidationError
from origins.graph import neo4j
from origins.graph.core import edges, nodes
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

    def test_match(self):
        e0 = edges.add(self.a0, self.b0)
        f0 = edges.add(self.a0, self.b0, type='likes')

        self.assertEqual(edges.match(), [e0, f0])
        self.assertEqual(edges.match(type='related'), [e0])
        self.assertEqual(edges.match(type='likes'), [f0])

    def test_search(self):
        n0 = edges.add(self.a0, self.b0, label='Hello world')
        m0 = edges.add(self.a0, self.b0, label='Hello moto')

        # Metadata
        predicate = {'label': 'Hello.*'}
        self.assertEqual(edges.search(predicate), [n0, m0])

        p0 = edges.add(self.a0, self.b0, properties={
            'color': 'blue',
            'size': 10,
        })

        # Properties
        predicate = {'properties': {'color': 'bl.*', 'size': 10}}
        self.assertEqual(edges.search(predicate), [p0])

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

    def test_add_bad_node(self):
        self.assertRaises(ValidationError, edges.add, 'abc123', self.b0)

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

    def test_set_not_exists(self):
        self.assertRaises(DoesNotExist, edges.set, 'abc123')

    def test_set_invalid(self):
        e0 = edges.add(self.a0, self.b0)
        edges.remove(e0.uuid)

        self.assertRaises(InvalidState, edges.set, e0.uuid)

    def test_remove(self):
        e0 = edges.add(self.a0, self.b0)
        e1 = edges.remove(e0.uuid)

        self.assertEqual(e1.uuid, e0.uuid)

        self.assertEqual(_nodes('prov:Invalidation'), 1)

    def test_remove_not_exists(self):
        self.assertRaises(DoesNotExist, edges.remove, 'abc123')

    def test_remove_invalid(self):
        e0 = edges.add(self.a0, self.b0)
        edges.remove(e0.uuid)

        self.assertRaises(InvalidState, edges.remove, e0.uuid)


class EdgeDirectionTestCase(TestCase):
    def setUp(self):
        super(EdgeDirectionTestCase, self).setUp()

        self.a0 = nodes.add()
        self.b0 = nodes.add()

    def assertUpdate(self, old, start, end):
        new = edges.get_by_id(old.id)

        # Ensure the revisions are different
        self.assertNotEqual(new.uuid, old.uuid)
        self.assertEqual(new.id, old.id)

        # Edge still visible
        self.assertEqual(edges.match(), [new])

        # Ensure the start and end nodes are correct
        self.assertEqual(new.start, start)
        self.assertEqual(new.end, end)

    def test_bidirected(self):
        e0 = edges.add(self.a0, self.b0, direction='bidirected')

        # a -> b; forward
        a1 = nodes.set(self.a0.uuid, label='v2')

        self.assertUpdate(e0, a1, self.b0)

        # a <- b; reverse
        b1 = nodes.set(self.b0.uuid, label='v2')

        self.assertUpdate(e0, a1, b1)

    def test_directed(self):
        e0 = edges.add(self.a0, self.b0, direction='directed')

        # a -> b; forward
        a1 = nodes.set(self.a0.uuid, label='v2')

        self.assertUpdate(e0, a1, self.b0)

        # a <- b; reverse
        nodes.set(self.b0.uuid, label='v2')

        # No new edge is formed
        self.assertRaises(DoesNotExist, edges.get_by_id, e0.id)

    def test_undirected(self):
        e0 = edges.add(self.a0, self.b0, direction='undirected')

        # a -> b; forward
        nodes.set(self.a0.uuid, label='v2')

        self.assertRaises(DoesNotExist, edges.get_by_id, e0.id)

        # Add a new edge since the previous is no longer visible
        e0 = edges.add(self.a0, self.b0, direction='undirected')

        # a <- b; reverse
        nodes.set(self.b0.uuid, label='v2')

        # No new edge is formed
        self.assertRaises(DoesNotExist, edges.get_by_id, e0.id)

    def test_optimistic(self):
        e0 = edges.add(self.a0, self.b0, optimistic=True, direction='directed')

        # a -> b; forward (works as normal)
        a1 = nodes.set(self.a0.uuid, label='v2')

        self.assertUpdate(e0, a1, self.b0)

        # a <- b; reverse
        b1 = nodes.set(self.b0.uuid, label='v2')

        # New edge formed due to optimistic update
        self.assertUpdate(e0, a1, b1)


class EdgeDependenceTestCase(TestCase):
    def setUp(self):
        super(EdgeDependenceTestCase, self).setUp()

        self.a0 = nodes.add()
        self.b0 = nodes.add()

    def test_mutual_s(self):
        e0 = edges.add(self.a0, self.b0, dependence='mutual')

        nodes.remove(self.a0.uuid)

        self.assertRaises(DoesNotExist, edges.get_by_id, e0.id)
        self.assertRaises(DoesNotExist, nodes.get_by_id, self.b0.id)

    def test_mutual_e(self):
        e0 = edges.add(self.a0, self.b0, dependence='mutual')

        nodes.remove(self.b0.uuid)

        self.assertRaises(DoesNotExist, edges.get_by_id, e0.id)
        self.assertRaises(DoesNotExist, nodes.get_by_id, self.a0.id)

    def test_forward_s(self):
        e0 = edges.add(self.a0, self.b0, dependence='forward')

        nodes.remove(self.a0.uuid)

        self.assertRaises(DoesNotExist, edges.get_by_id, e0.id)

        # b still exists
        b0 = nodes.get_by_id(self.b0.id)
        self.assertEqual(b0, self.b0)

    def test_forward_e(self):
        e0 = edges.add(self.a0, self.b0, dependence='forward')

        nodes.remove(self.b0.uuid)

        self.assertRaises(DoesNotExist, edges.get_by_id, e0.id)

        # a removed because of b
        self.assertRaises(DoesNotExist, nodes.get_by_id, self.a0.id)

    def test_inverse_s(self):
        e0 = edges.add(self.a0, self.b0, dependence='inverse')

        nodes.remove(self.a0.uuid)

        self.assertRaises(DoesNotExist, edges.get_by_id, e0.id)

        # b removed because inverse dependence on a
        self.assertRaises(DoesNotExist, nodes.get_by_id, self.b0.id)

    def test_inverse_e(self):
        e0 = edges.add(self.a0, self.b0, dependence='inverse')

        nodes.remove(self.b0.uuid)

        self.assertRaises(DoesNotExist, edges.get_by_id, e0.id)

        # a still exists because the dependence is inverse to the direction
        a0 = nodes.get_by_id(self.a0.id)
        self.assertEqual(a0, self.a0)

    def test_none_s(self):
        e0 = edges.add(self.a0, self.b0, dependence='none')

        nodes.remove(self.a0.uuid)

        self.assertRaises(DoesNotExist, edges.get_by_id, e0.id)

        # a still exists because the dependence is inverse to the direction
        b0 = nodes.get_by_id(self.b0.id)
        self.assertEqual(b0, self.b0)

    def test_none_e(self):
        e0 = edges.add(self.a0, self.b0, dependence='none')

        nodes.remove(self.b0.uuid)

        self.assertRaises(DoesNotExist, edges.get_by_id, e0.id)

        # a still exists because the dependence is inverse to the direction
        a0 = nodes.get_by_id(self.a0.id)
        self.assertEqual(a0, self.a0)


class DependencyTreeTestCase(TestCase):
    def setUp(self):
        super(DependencyTreeTestCase, self).setUp()

        self.a0 = nodes.add()
        self.b0 = nodes.add()

    def test_circle(self):
        f = s = self.a0
        e = self.b0

        # Create initial edge
        edges.add(s, e, dependence='forward')

        for _ in range(10):
            s = e
            e = nodes.add()
            edges.add(s, e, dependence='forward')

        # Connect the loop
        edges.add(e, f, dependence='forward')

        # Trigger delete
        nodes.remove(f.uuid)

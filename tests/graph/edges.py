from origins.exceptions import DoesNotExist, ValidationError
from origins.graph import neo4j
from origins.graph.edges import Edge
from origins.graph.nodes import Node
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

        self.a0 = Node.add()
        self.b0 = Node.add()

    def test_match(self):
        e0 = Edge.add(start=self.a0, end=self.b0)
        f0 = Edge.add(start=self.a0, end=self.b0, type='likes')

        self.assertEqual(Edge.match(), [e0, f0])
        self.assertEqual(Edge.match(type='related'), [e0])
        self.assertEqual(Edge.match(type='likes'), [f0])

    def test_search(self):
        n0 = Edge.add(self.a0, self.b0, label='Hello world')
        m0 = Edge.add(self.a0, self.b0, label='Hello moto')

        # Metadata
        predicate = {'label': 'Hello.*'}
        self.assertEqual(Edge.search(predicate), [n0, m0])

        p0 = Edge.add(self.a0, self.b0, properties={
            'color': 'blue',
            'size': 10,
        })

        # Properties
        predicate = {'properties': {'color': 'bl.*', 'size': 10}}
        self.assertEqual(Edge.search(predicate), [p0])

    def test_get(self):
        self.assertRaises(DoesNotExist, Edge.get, 'abc123')

        e = Edge.add(self.a0, self.b0)
        _e = Edge.get(e.uuid)

        self.assertEqual(e, _e)

    def test_get_by_id(self):
        # Adding one makes it visible
        n0 = Edge.add(self.a0, self.b0)

        n0_ = Edge.get_by_id(n0.id)

        self.assertEqual(n0, n0_)

        n1 = Edge.set(n0.uuid, label='Notable')
        n1_ = Edge.get_by_id(n0.id)

        self.assertEqual(n1, n1_)

        # Remove makes it no longer visible
        Edge.remove(n1.uuid)
        self.assertRaises(DoesNotExist, Edge.get_by_id, n0.id)

    def test_add(self):
        Edge.add(self.a0, self.b0)

        self.assertEqual(_nodes('origins:Edge'), 1)

        # 2 Node, 1 edge
        self.assertEqual(_nodes('origins:Continuant'), 3)
        self.assertEqual(_nodes('prov:Generation'), 3)

        # Forms a real edge between the two Node
        self.assertEqual(_edges(Edge.model_type), 1)

    def test_add_bad_node(self):
        # Missing node
        self.assertRaises(ValidationError, Edge.add, Node(), self.b0)

        e = Edge.add(Node.add(), self.b0)
        # Invalid node
        Node.remove(e.start.uuid)

        self.assertRaises(ValidationError, Edge.add, e)

    def test_add_graph_edge(self):
        e = Edge.add(self.a0, self.b0, type='likes', properties={'foo': 1})

        # Created with properties
        self.assertEqual(_edges('likes', count=False), [{'foo': 1}])

        Edge.remove(e.uuid)

        # Deleted
        self.assertEqual(_edges('likes'), 0)

    def test_set(self):
        e0 = Edge.add(self.a0, self.b0)
        e0_ = Edge.set(e0.uuid)

        # Nothing changed
        self.assertIsNone(e0_)

        # Something changed
        e1 = Edge.set(e0.uuid, type='likes')

        self.assertEqual(e1.diff(e0), {'type': ('related', 'likes')})
        self.assertEqual(e1.id, e0.id)
        self.assertNotEqual(e1.uuid, e0.uuid)

        # Ensure the physical edge was replaced
        self.assertEqual(_edges(Edge.model_type, self.a0, self.b0), 0)
        self.assertEqual(_edges('likes', self.a0, self.b0), 1)

        self.assertEqual(_nodes('prov:Derivation'), 1)
        self.assertEqual(_nodes('prov:Invalidation'), 1)

    def test_set_not_exists(self):
        self.assertRaises(DoesNotExist, Edge.set, 'abc123')

    def test_set_invalid(self):
        e0 = Edge.add(self.a0, self.b0)
        Edge.remove(e0.uuid)

        self.assertRaises(ValidationError, Edge.set, e0.uuid)

    def test_remove(self):
        e0 = Edge.add(self.a0, self.b0)

        e1 = Edge.remove(e0.uuid)

        self.assertEqual(e1.uuid, e0.uuid)

        self.assertEqual(_nodes('prov:Invalidation'), 1)

    def test_remove_not_exists(self):
        self.assertRaises(DoesNotExist, Edge.remove, 'abc123')

    def test_remove_invalid(self):
        e0 = Edge.add(self.a0, self.b0)
        Edge.remove(e0.uuid)

        self.assertRaises(ValidationError, Edge.remove, e0.uuid)


class EdgeDirectionTestCase(TestCase):
    def setUp(self):
        super(EdgeDirectionTestCase, self).setUp()

        self.a0 = Node.add()
        self.b0 = Node.add()

    def assertUpdate(self, old, start, end):
        new = Edge.get_by_id(old.id)

        # Ensure the revisions are different
        self.assertNotEqual(new.uuid, old.uuid)
        self.assertEqual(new.id, old.id)

        # Edge still visible
        self.assertEqual(Edge.match(), [new])

        # Ensure the start and end Node are correct
        self.assertEqual(new.start, start)
        self.assertEqual(new.end, end)

    def test_bidirected(self):
        e0 = Edge.add(self.a0, self.b0, direction='bidirected')

        # a -> b; forward
        a1 = Node.set(self.a0.uuid, label='v2')

        self.assertUpdate(e0, a1, self.b0)

        # a <- b; reverse
        b1 = Node.set(self.b0.uuid, label='v2')

        self.assertUpdate(e0, a1, b1)

    def test_directed(self):
        e0 = Edge.add(self.a0, self.b0, direction='directed')

        # a -> b; forward
        a1 = Node.set(self.a0.uuid, label='v2')

        self.assertUpdate(e0, a1, self.b0)

        # a <- b; reverse
        Node.set(self.b0.uuid, label='v2')

        # No new edge is formed
        self.assertRaises(DoesNotExist, Edge.get_by_id, e0.id)

    def test_undirected(self):
        e0 = Edge.add(self.a0, self.b0, direction='undirected')

        # a -> b; forward
        a1 = Node.set(self.a0.uuid, label='v2')

        self.assertRaises(DoesNotExist, Edge.get_by_id, e0.id)

        # Add a new edge since the previous is no longer visible
        e0 = Edge.add(a1, self.b0, direction='undirected')

        # a <- b; reverse
        Node.set(self.b0.uuid, label='v2')

        # No new edge is formed
        self.assertRaises(DoesNotExist, Edge.get_by_id, e0.id)

    def test_reverse(self):
        # A will watch B
        e0 = Edge.add(self.a0, self.b0, direction='reverse')

        # a <- b; reverse
        b1 = Node.set(self.b0.uuid, label='v2')

        # New edge formed due to the reverse
        self.assertUpdate(e0, self.a0, b1)


class EdgeDependenceTestCase(TestCase):
    def setUp(self):
        super(EdgeDependenceTestCase, self).setUp()

        self.a0 = Node.add()
        self.b0 = Node.add()

    def test_mutual_s(self):
        e0 = Edge.add(self.a0, self.b0, dependence='mutual')

        Node.remove(self.a0.uuid)

        self.assertRaises(DoesNotExist, Edge.get_by_id, e0.id)
        self.assertRaises(DoesNotExist, Node.get_by_id, self.b0.id)

    def test_mutual_e(self):
        e0 = Edge.add(self.a0, self.b0, dependence='mutual')

        Node.remove(self.b0.uuid)

        self.assertRaises(DoesNotExist, Edge.get_by_id, e0.id)
        self.assertRaises(DoesNotExist, Node.get_by_id, self.a0.id)

    def test_forward_s(self):
        e0 = Edge.add(self.a0, self.b0, dependence='forward')

        Node.remove(self.a0.uuid)

        self.assertRaises(DoesNotExist, Edge.get_by_id, e0.id)

        # b still exists
        b0 = Node.get_by_id(self.b0.id)
        self.assertEqual(b0, self.b0)

    def test_forward_e(self):
        e0 = Edge.add(self.a0, self.b0, dependence='forward')

        Node.remove(self.b0.uuid)

        self.assertRaises(DoesNotExist, Edge.get_by_id, e0.id)

        # a removed because of b
        self.assertRaises(DoesNotExist, Node.get_by_id, self.a0.id)

    def test_inverse_s(self):
        e0 = Edge.add(self.a0, self.b0, dependence='inverse')

        Node.remove(self.a0.uuid)

        self.assertRaises(DoesNotExist, Edge.get_by_id, e0.id)

        # b removed because inverse dependence on a
        self.assertRaises(DoesNotExist, Node.get_by_id, self.b0.id)

    def test_inverse_e(self):
        e0 = Edge.add(self.a0, self.b0, dependence='inverse')

        Node.remove(self.b0.uuid)

        self.assertRaises(DoesNotExist, Edge.get_by_id, e0.id)

        # a still exists because the dependence is inverse to the direction
        a0 = Node.get_by_id(self.a0.id)
        self.assertEqual(a0, self.a0)

    def test_none_s(self):
        e0 = Edge.add(self.a0, self.b0, dependence='none')

        Node.remove(self.a0.uuid)

        self.assertRaises(DoesNotExist, Edge.get_by_id, e0.id)

        # a still exists because the dependence is inverse to the direction
        b0 = Node.get_by_id(self.b0.id)
        self.assertEqual(b0, self.b0)

    def test_none_e(self):
        e0 = Edge.add(self.a0, self.b0, dependence='none')

        Node.remove(self.b0.uuid)

        self.assertRaises(DoesNotExist, Edge.get_by_id, e0.id)

        # a still exists because the dependence is inverse to the direction
        a0 = Node.get_by_id(self.a0.id)
        self.assertEqual(a0, self.a0)

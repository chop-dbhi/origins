from origins.graph import neo4j
from origins.graph.edges import Edge
from origins.graph.nodes import Node
from .base import TestCase


def _build_tree(depth=3, tx=neo4j.tx):
    with tx as tx:
        root = Node.add('0', validate=False)

        # Start with root
        queue = [(root, 0)]

        # add two leaves to each parent
        while queue:
            r, d = queue.pop()

            d += 1
            a = Node.add('{}a'.format(d), validate=False)
            b = Node.add('{}b'.format(d), validate=False)

            # Attach children for convenience
            r.children = [a, b]

            Edge.add(a, r, dependence='forward', validate=False)
            Edge.add(b, r, dependence='forward', validate=False)

            if d < depth:
                queue.append((a, d))
                queue.append((b, d))

        return root


def _build_circle(size=10, tx=neo4j.tx):
    with tx as tx:
        f = s = Node.add(validate=False, tx=tx)
        e = Node.add(validate=False, tx=tx)

        # Create initial edge
        Edge.add(s, e, dependence='forward', validate=False, tx=tx)

        for _ in range(size):
            s = e
            e = Node.add(validate=False, tx=tx)
            Edge.add(s, e, dependence='forward', validate=False, tx=tx)

        # Connect the loop
        Edge.add(e, f, dependence='forward', validate=False, tx=tx)

    return f


def _descendents(n):
    d = []
    s = [n]

    while s:
        r = s.pop()

        if hasattr(r, 'children'):
            d.extend(r.children)
            s.extend(r.children)

    return d


class DependencyChainTestCase(TestCase):
    def test_circle(self):
        f = _build_circle()

        # Trigger delete
        Node.remove(f.uuid)

        # All were removed
        self.assertEqual(Node.match(), [])

    def test_tree(self):
        root = _build_tree()

        n = root.children[0]
        d = _descendents(n)
        removed = set([n] + d)

        Node.remove(n.uuid)

        remaining = set(Node.match())

        # None of the removed Node should match
        self.assertFalse(remaining & removed)

        # No Edge remain for removed Node
        for e in Edge.match():
            self.assertFalse(e.start in removed)
            self.assertFalse(e.end in removed)

        # Remove root; everything else is gone
        Node.remove(root.uuid)

        self.assertEqual(Node.match(), [])
        self.assertEqual(Edge.match(), [])

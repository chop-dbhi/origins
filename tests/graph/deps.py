from origins.graph import neo4j
from origins.graph.core import edges, nodes
from .base import TestCase


def _build_tree(max_depth=3, tx=neo4j.tx):
    with tx as tx:
        root = nodes.add('0')

        # Start with root
        queue = [(root, 0)]

        # add two leaves to each parent
        while queue:
            r, d = queue.pop()

            d += 1
            a = nodes.add('{}a'.format(d), publish_event=False)
            b = nodes.add('{}b'.format(d), publish_event=False)

            # Attach children for convenience
            r.children = [a, b]

            edges.add(a, r, dependence='forward', publish_event=False,
                      check_nodes=False)
            edges.add(b, r, dependence='forward', publish_event=False,
                      check_nodes=False)

            if d < max_depth:
                queue.append((a, d))
                queue.append((b, d))

        return root


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
        f = s = nodes.add()
        e = nodes.add()

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

        # All were removed
        self.assertEqual(nodes.match(), [])

    def test_tree(self):
        root = _build_tree()

        n = root.children[0]
        d = _descendents(n)
        removed = set([n] + d)

        nodes.remove(n.uuid)

        remaining = set(nodes.match())

        # None of the removed nodes should match
        self.assertFalse(remaining & removed)

        # No edges remain for removed nodes
        for e in edges.match():
            self.assertFalse(e.start in removed)
            self.assertFalse(e.end in removed)

        # Remove root; everything else is gone
        nodes.remove(root.uuid)

        self.assertEqual(nodes.match(), [])
        self.assertEqual(edges.match(), [])

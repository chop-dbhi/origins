import unittest
from origins.exceptions import OriginsError
from origins.backends.base import LAZY_ATTR


class TestBase(unittest.TestCase):
    def setUp(self):
        from origins.backends import base
        self.module = base

    def test_node_default_state(self):
        node = self.module.Node()

        self.assertEqual(node.attrs, {})
        self.assertEqual(node.source, None)
        self.assertEqual(node._client, None)

        self.assertEqual(node.id, None)
        self.assertEqual(node.label, None)
        self.assertRaises(OriginsError, lambda: node.client)

        # No-op by default
        node.synchronize()
        self.assertEqual(node.attrs, {})

        # No id exists nor source
        self.assertEqual(node.serialize(), {})

        self.assertEqual(node.branches(), [])
        self.assertEqual(node.elements(), [])

    def test_lazy_attributes(self):
        class Lazy(self.module.Node):
            lazy_attributes = ('foo', 'bar', 'baz')

            def foo(self):
                return 1

        node = Lazy()

        # before
        self.assertEqual(node.attrs['foo'], LAZY_ATTR)
        self.assertEqual(node.attrs['bar'], LAZY_ATTR)

        # access
        self.assertEqual(node['foo'], 1)
        self.assertEqual(node['bar'], None)

        # after
        self.assertEqual(node.attrs['foo'], 1)
        self.assertEqual(node.attrs['bar'], None)

        self.assertEqual(node.serialize(), {'foo': 1, 'bar': None})

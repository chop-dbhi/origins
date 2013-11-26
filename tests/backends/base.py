import os
import unittest
from importlib import import_module
from origins.exceptions import OriginsError
from origins.backends.base import LAZY_ATTR

TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), '../data')


class BackendTestCase(unittest.TestCase):
    backend_path = ''

    def load_backend(self):
        if not self.backend_path:
            raise ValueError('backend_path must be defined')
        try:
            self.backend = import_module(self.backend_path)
        except ImportError as e:
            self.skipTest(unicode(e))

    def setUp(self):
        self.load_backend()


class BaseTestBase(BackendTestCase):
    backend_path = 'origins.backends.base'

    def test_node_default_state(self):
        node = self.backend.Node()

        self.assertEqual(node.attrs, {})
        self.assertEqual(node.source, None)
        self.assertEqual(node._client, None)

        self.assertEqual(node.id, '')
        self.assertEqual(node.label, '')
        self.assertRaises(OriginsError, lambda: node.client)

        # No-op by default
        node.synchronize()
        self.assertEqual(node.attrs, {})

        # No id exists nor source
        self.assertEqual(node.serialize(), {'id': ''})

        self.assertEqual(node.branches(), [])
        self.assertEqual(node.elements(), [])

    def test_node_dict_behavior(self):
        node = self.backend.Node()
        node['a'] = 1
        self.assertEqual(node.attrs['a'], 1)
        self.assertEqual(node['a'], 1)
        del node['a']
        self.assertFalse('a' in node.attrs)
        self.assertFalse('a' in node)

    def test_node_lazy_attributes(self):
        class Lazy(self.backend.Node):
            lazy_attributes = ('foo', 'bar')

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

        self.assertEqual(node.serialize(), {'id': '', 'foo': 1, 'bar': None})

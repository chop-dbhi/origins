import unittest
from origins.exceptions import OriginsError


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

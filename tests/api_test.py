from __future__ import unicode_literals
import os
import unittest

TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')


class ApiTestCase(unittest.TestCase):
    def setUp(self):
        import origins
        path = os.path.join(TEST_DATA_DIR, 'chinook.sqlite')
        self.root = origins.connect('sqlite', path=path)

    # Backend is always passed as an attribute to the root
    def test_backend_attr(self):
        self.assertEqual(self.root['backend'], 'sqlite')

    # Ensure the client is set on the underlying node
    def test_client_exists(self):
        self.assertTrue(self.root._node.client)

    def test_branches(self):
        self.assertEqual(self.root.branches.labels(), [
            'Album',
            'Artist',
            'Customer',
            'Employee',
            'Genre',
            'Invoice',
            'InvoiceLine',
            'MediaType',
            'Playlist',
            'PlaylistTrack',
            'Track',
            ])

    def test_branches_alias(self):
        self.assertTrue(hasattr(self.root, 'tables'))
        self.assertEqual(self.root.tables, self.root.branches)

    # Collects all elements across all tables
    def test_root_elements(self):
        self.assertEqual(len(self.root.elements), 64)

        # Test container by key (key relative to root)
        self.assertEqual(self.root.elements['Album/AlbumId'].id,
                         'chinook.sqlite/Album/AlbumId')

        # Test container by index
        self.assertEqual(self.root.elements[0].id,
                         'chinook.sqlite/Album/AlbumId')

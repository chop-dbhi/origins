import os
import origins
from .base import BackendTestCase, TEST_DATA_DIR


class DelimitedBackendClientTestCase(BackendTestCase):
    backend_path = 'origins.backends.delimited'

    def setUp(self):
        self.load_backend()
        path = os.path.join(TEST_DATA_DIR, 'chinook_tracks.csv')
        self.client = self.backend.Client(path)

    def test_setup(self):
        self.assertTrue(self.client.has_header)
        self.assertEqual(self.client.delimiter, ',')
        self.assertEqual(len(self.client._header_index), 9)

    def test_columns(self):
        columns = self.client.columns()
        self.assertEqual(len(columns), 9)
        self.assertTrue('name' in columns[0])


class DelimitedBackendTestCase(BackendTestCase):
    backend_path = 'origins.backends.delimited'

    def setUp(self):
        self.load_backend()
        path = os.path.join(TEST_DATA_DIR, 'chinook_tracks.csv')
        self.origin = origins.connect('delimited', path=path)

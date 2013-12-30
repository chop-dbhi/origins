import os
import origins
from .base import BackendTestCase, TEST_DATA_DIR


class DelimitedClientTestCase(BackendTestCase):
    backend_path = 'origins.backends.delimited'

    def setUp(self):
        self.load_backend()
        path = os.path.join(TEST_DATA_DIR, 'chinook_tracks.csv')
        self.client = self.backend.Client(path)

    def test_reader(self):
        row = next(self.client.reader)
        # Ensure the header is skipped
        self.assertNotEqual(row[0], 'TrackId')

    def test_setup(self):
        self.assertTrue(self.client.has_header)
        self.assertEqual(self.client.delimiter, ',')
        self.assertEqual(len(self.client._header_index), 9)

    def test_properties(self):
        self.assertGreater(self.client.file_line_count(), 0)

    def test_file(self):
        self.assertTrue(self.client.file())

    def test_columns(self):
        columns = self.client.columns()
        self.assertEqual(len(columns), 9)
        self.assertTrue('name' in columns[0])


class DelimitedApiTestCase(BackendTestCase):
    backend_path = 'origins.backends.delimited'

    def setUp(self):
        self.path = os.path.join(TEST_DATA_DIR, 'chinook_tracks.csv')
        self.f = origins.connect('delimited', path=self.path)

    def test_file(self):
        self.assertEqual(self.f.label, 'chinook_tracks.csv')
        self.assertEqual(self.f.name, 'chinook_tracks.csv')
        self.assertEqual(self.f.path, 'chinook_tracks.csv')
        self.assertEqual(self.f.uri, 'delimited:///chinook_tracks.csv')
        self.assertEqual(self.f.relpath, [])
        self.assertTrue(self.f.isroot)
        self.assertFalse(self.f.isleaf)
        self.assertTrue('uri' in self.f.serialize())

    def test_column(self):
        column = self.f.columns[0]

        self.assertEqual(column.label, 'TrackId')
        self.assertEqual(column.name, 'TrackId')
        self.assertEqual(column.path, 'chinook_tracks.csv/TrackId')
        self.assertEqual(len(column.relpath), 1)
        self.assertFalse(column.isroot)
        self.assertTrue(column.isleaf)

from __future__ import unicode_literals, absolute_import

import os
import origins
from .base import BackendTestCase, TEST_DATA_DIR


class DataDictClientTestCase(BackendTestCase):
    backend_path = 'origins.backends.datadict'

    def setUp(self):
        self.load_backend()
        path = os.path.join(TEST_DATA_DIR, 'chinook_tracks_datadict.csv')
        self.client = self.backend.Client(path)

    def test_reader(self):
        row = next(self.client.reader)
        # Ensure the header is skipped
        self.assertEqual(row[0], 'TrackId')

    def test_setup(self):
        self.assertTrue(self.client.has_header)
        self.assertEqual(self.client.delimiter, ',')

    def test_properties(self):
        self.assertGreater(self.client.file_line_count(), 0)

    def test_file(self):
        self.assertTrue(self.client.file())

    def test_fields(self):
        fields = self.client.fields()
        self.assertEqual(len(fields), 9)
        self.assertTrue('name' in fields[0])


class DataDictApiTestCase(BackendTestCase):
    backend_path = 'origins.backends.datadict'

    def setUp(self):
        self.path = os.path.join(TEST_DATA_DIR, 'chinook_tracks_datadict.csv')
        self.f = origins.connect('datadict', path=self.path)

    def test_file(self):
        self.assertEqual(self.f.label, 'chinook_tracks_datadict.csv')
        self.assertEqual(self.f.name, 'chinook_tracks_datadict.csv')
        self.assertEqual(self.f.path, 'chinook_tracks_datadict.csv')
        self.assertEqual(self.f.uri, 'datadict:///chinook_tracks_datadict.csv')
        self.assertEqual(self.f.relpath, [])
        self.assertTrue(self.f.isroot)
        self.assertFalse(self.f.isleaf)
        self.assertTrue('uri' in self.f.serialize())

    def test_field(self):
        field = self.f.fields['TrackId']

        self.assertEqual(field.label, 'TrackId')
        self.assertEqual(field.name, 'TrackId')
        self.assertEqual(field.path, 'chinook_tracks_datadict.csv/TrackId')
        self.assertEqual(len(field.relpath), 1)
        self.assertFalse(field.isroot)
        self.assertTrue(field.isleaf)

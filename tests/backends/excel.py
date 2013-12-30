import os
import origins
from .base import BackendTestCase, TEST_DATA_DIR


class ExcelClientTestCase(BackendTestCase):
    backend_path = 'origins.backends.excel'

    def setUp(self):
        self.load_backend()
        self.path = os.path.join(TEST_DATA_DIR, 'chinook.xlsx')
        self.client = self.backend.Client(self.path)

    def test_setup(self):
        self.assertTrue(self.client.has_headers)
        self.assertFalse(self.client._sheet_columns)

    def test_file(self):
        self.assertTrue(self.client.file())

    def test_sheets(self):
        sheets = self.client.sheets()
        self.assertTrue(sheets)
        self.assertTrue('name' in sheets[0])

    def test_single_headers(self):
        headers = ['one', 'two', 'three']
        client = self.backend.Client(self.path, headers=headers)
        self.assertEqual([c['name'] for c in client.columns('Albums')],
                         headers)

    def test_no_headers(self):
        client = self.backend.Client(self.path, headers=False)
        self.assertEqual([c['name'] for c in client.columns('Albums')],
                         [0, 1, 2])

    def test_multiple_headers(self):
        client = self.backend.Client(self.path, headers={
            'Albums': ['one', 'two', 'three'],
            'PlaylistTracks': ['one', 'two'],
        })
        self.assertEqual([c['name'] for c in client.columns('PlaylistTracks')],
                         ['one', 'two'])

    def test_invalid_headers(self):
        self.assertRaises(TypeError, self.backend.Client,
                          self.path, headers=10)


class ExcelApiTestCase(BackendTestCase):
    backend_path = 'origins.backends.excel'

    def setUp(self):
        self.path = os.path.join(TEST_DATA_DIR, 'chinook.xlsx')
        self.wb = origins.connect('excel', path=self.path)

    def test_wb(self):
        self.assertEqual(self.wb.label, 'chinook.xlsx')
        self.assertEqual(self.wb.name, 'chinook.xlsx')
        self.assertEqual(self.wb.path, 'chinook.xlsx')
        self.assertEqual(self.wb.uri, 'excel:///chinook.xlsx')
        self.assertEqual(self.wb.relpath, [])
        self.assertTrue(self.wb.isroot)
        self.assertFalse(self.wb.isleaf)
        self.assertTrue('uri' in self.wb.serialize())

    def test_sheet(self):
        sheet = self.wb.sheets['Albums']

        self.assertEqual(sheet.label, 'Albums')
        self.assertEqual(sheet.name, 'Albums')
        self.assertEqual(sheet.path, 'chinook.xlsx/Albums')
        self.assertEqual(len(sheet.relpath), 1)
        self.assertFalse(sheet.isroot)
        self.assertFalse(sheet.isleaf)

    def test_column(self):
        column = self.wb.sheets['Albums'].columns['AlbumId']

        self.assertEqual(column.label, 'AlbumId')
        self.assertEqual(column.name, 'AlbumId')
        self.assertEqual(column.path, 'chinook.xlsx/Albums/AlbumId')
        self.assertEqual(len(column.relpath), 2)
        self.assertFalse(column.isroot)
        self.assertTrue(column.isleaf)

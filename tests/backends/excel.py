import os
import origins
from .base import BackendTestCase, TEST_DATA_DIR


class ExcelBackendClientTestCase(BackendTestCase):
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


class ExcelBackendTestCase(BackendTestCase):
    backend_path = 'origins.backends.excel'

    def setUp(self):
        self.path = os.path.join(TEST_DATA_DIR, 'chinook.xlsx')

    def test_default(self):
        node = origins.connect('excel', path=self.path)
        self.assertTrue(node.props)
        self.assertTrue(node.sheets)
        self.assertTrue(node.sheets[0].columns)

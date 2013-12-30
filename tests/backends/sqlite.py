import os
import origins
from .base import BackendTestCase, TEST_DATA_DIR


class SqliteClientTestCase(BackendTestCase):
    backend_path = 'origins.backends.sqlite'

    def setUp(self):
        self.load_backend()
        path = os.path.join(TEST_DATA_DIR, 'chinook.sqlite')
        self.client = self.backend.Client(path)

    def test_properties(self):
        self.assertFalse(self.client.foreign_keys_supported())

    def test_database(self):
        props = self.client.database()
        self.assertGreater(props['size'], 0)
        self.assertEqual(props['version'], 32)

    def test_tables(self):
        tables = self.client.tables()
        self.assertEqual(len(tables), 11)
        self.assertTrue('name' in tables[0])
        self.assertEqual(sorted(tables), tables)

    def test_columns(self):
        columns = self.client.columns('Album')
        self.assertEqual(len(columns), 3)
        self.assertTrue('name' in columns[0])

        sorted_columns = sorted(columns, key=lambda x: x['index'])
        self.assertEqual(sorted_columns, columns)

    def test_foreign_keys(self):
        fks = self.client.foreign_keys('Album', 'ArtistId')
        self.assertEqual(len(fks), 1)


class SqliteTestCase(BackendTestCase):
    backend_path = 'origins.backends.sqlite'

    def setUp(self):
        self.load_backend()
        path = os.path.join(TEST_DATA_DIR, 'chinook.sqlite')
        self.origin = origins.connect('sqlite', path=path)

    def test_sync(self):
        self.assertTrue(self.origin.props)
        self.assertTrue(self.origin.tables)
        self.assertTrue(self.origin.tables[0].columns)

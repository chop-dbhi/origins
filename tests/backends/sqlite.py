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


class SqliteApiTestCase(BackendTestCase):
    backend_path = 'origins.backends.sqlite'

    def setUp(self):
        self.path = os.path.join(TEST_DATA_DIR, 'chinook.sqlite')
        self.db = origins.connect('sqlite', path=self.path)

    def test_db(self):
        self.assertEqual(self.db.label, 'chinook.sqlite')
        self.assertEqual(self.db.name, 'chinook.sqlite')
        self.assertEqual(self.db.path, 'chinook.sqlite')
        self.assertEqual(self.db.uri, 'sqlite:///chinook.sqlite')
        self.assertEqual(self.db.relpath, [])
        self.assertTrue(self.db.isroot)
        self.assertFalse(self.db.isleaf)
        self.assertTrue('uri' in self.db.serialize())

    def test_table(self):
        table = self.db.tables['Album']

        self.assertEqual(table.label, 'Album')
        self.assertEqual(table.name, 'Album')
        self.assertEqual(table.path, 'chinook.sqlite/Album')
        self.assertEqual(len(table.relpath), 1)
        self.assertFalse(table.isroot)
        self.assertFalse(table.isleaf)

    def test_column(self):
        column = self.db.tables['Album'].columns['AlbumId']

        self.assertEqual(column.label, 'AlbumId')
        self.assertEqual(column.name, 'AlbumId')
        self.assertEqual(column.path, 'chinook.sqlite/Album/AlbumId')
        self.assertEqual(len(column.relpath), 2)
        self.assertFalse(column.isroot)
        self.assertTrue(column.isleaf)

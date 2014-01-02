import os
import origins
from .base import BackendTestCase, TEST_DATA_DIR

import sqlite3


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

    def test_foreign_keys(self):
        fks = self.db.tables['Album'].columns['ArtistId'].foreign_keys
        self.assertEqual(len(fks), 1)


class SqliteDalTestCase(BackendTestCase):
    backend_path = 'origins.backends.sqlite'

    def setUp(self):
        self.load_backend()
        path = os.path.join(TEST_DATA_DIR, 'chinook.sqlite')
        self.db = origins.connect('sqlite', path=path)

    def test_table_count(self):
        table = self.db.tables['Employee']
        self.assertEqual(table.count(), 8)

    def test_table_select(self):
        table = self.db.tables['Employee']
        records = table.select(['FirstName', 'LastName'], iterator=False)
        self.assertTrue(records)
        self.assertEqual(records[0], ('Andrew', 'Adams'))

    def test_column_count(self):
        column = self.db.tables['Employee'].columns['Title']
        self.assertEqual(column.count(), 5)
        self.assertEqual(column.count(distinct=False), 8)

    def test_column_select(self):
        column = self.db.tables['Employee'].columns['Title']
        records = column.select(iterator=False)
        self.assertTrue(records)

    def test_column_sum(self):
        column = self.db.tables['track'].columns['milliseconds']
        self.assertGreater(column.sum(), 0)

    def test_column_avg(self):
        column = self.db.tables['track'].columns['milliseconds']
        self.assertGreater(column.avg(), 0)

    def test_column_min(self):
        column = self.db.tables['track'].columns['milliseconds']
        self.assertGreater(column.min(), 0)

    def test_column_max(self):
        column = self.db.tables['track'].columns['milliseconds']
        self.assertGreater(column.max(), 0)

    # SQLite does not have built-in functions for STDDEV nor VARIANCE, however
    # they are available in a C-extension: http://www.sqlite.org/contrib
    # therefore it _may_ work.
    def test_column_stddev(self):
        column = self.db.tables['track'].columns['milliseconds']
        self.assertRaises(sqlite3.OperationalError, column.stddev)

    def test_column_variance(self):
        column = self.db.tables['track'].columns['milliseconds']
        self.assertRaises(sqlite3.OperationalError, column.variance)

    def test_column_longest(self):
        column = self.db.tables['track'].columns['name']
        self.assertGreater(len(column.longest()), 0)

    def test_column_shortest(self):
        column = self.db.tables['track'].columns['name']
        self.assertGreater(len(column.shortest()), 0)

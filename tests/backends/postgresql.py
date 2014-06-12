from __future__ import unicode_literals, absolute_import

import os
import origins
from .base import BackendTestCase

HOST = os.environ.get('POSTGRESQL_HOST', 'localhost')
USER = os.environ.get('POSTGRESQL_USER')
PASSWORD = os.environ.get('POSTGRESQL_PASSWORD')


class PostgresqlClientTestCase(BackendTestCase):
    backend_path = 'origins.backends.postgresql'

    def setUp(self):
        self.load_backend()
        self.client = self.backend.Client('chinook', host=HOST, user=USER,
                                          password=PASSWORD)

    def test_database(self):
        props = self.client.database()
        self.assertTrue('version' in props)

    def test_schemas(self):
        schemas = self.client.schemas()
        self.assertEqual(len(schemas), 1)
        self.assertTrue('name' in schemas[0])

    def test_tables(self):
        tables = self.client.tables('public')
        self.assertEqual(len(tables), 11)
        self.assertTrue('name' in tables[0])

    def test_columns(self):
        columns = self.client.columns('public', 'Album')
        self.assertEqual(len(columns), 3)
        self.assertTrue('name' in columns[0])

        sorted_columns = sorted(columns, key=lambda x: x['index'])
        self.assertEqual(sorted_columns, columns)

    def test_foreign_keys(self):
        fks = self.client.foreign_keys('public', 'Album', 'ArtistId')
        self.assertEqual(len(fks), 1)


class PostgresqlApiTestCase(BackendTestCase):
    backend_path = 'origins.backends.postgresql'

    def setUp(self):
        self.db = origins.connect('postgresql', database='chinook', host=HOST,
                                  user=USER, password=PASSWORD)

    def test_db(self):
        self.assertEqual(self.db.label, 'chinook')
        self.assertEqual(self.db.name, 'chinook')
        self.assertEqual(self.db.path, 'chinook')
        self.assertEqual(self.db.uri, 'postgresql://' + HOST + ':5432/chinook')
        self.assertEqual(self.db.relpath, [])
        self.assertTrue(self.db.isroot)
        self.assertFalse(self.db.isleaf)
        self.assertTrue('uri' in self.db.serialize())

    def test_schema(self):
        table = self.db.schemas['public']

        self.assertEqual(table.label, 'public')
        self.assertEqual(table.name, 'public')
        self.assertEqual(table.path, 'chinook/public')
        self.assertEqual(len(table.relpath), 1)
        self.assertFalse(table.isroot)
        self.assertFalse(table.isleaf)

    def test_table(self):
        table = self.db.tables['Album']

        self.assertEqual(table.label, 'Album')
        self.assertEqual(table.name, 'Album')
        self.assertEqual(table.path, 'chinook/public/Album')
        self.assertEqual(len(table.relpath), 2)
        self.assertFalse(table.isroot)
        self.assertFalse(table.isleaf)

    def test_column(self):
        column = self.db.tables['Album'].columns['AlbumId']

        self.assertEqual(column.label, 'AlbumId')
        self.assertEqual(column.name, 'AlbumId')
        self.assertEqual(column.path, 'chinook/public/Album/AlbumId')
        self.assertEqual(len(column.relpath), 3)
        self.assertFalse(column.isroot)
        self.assertTrue(column.isleaf)

    def test_foreign_keys(self):
        fks = self.db.tables['Album'].columns['ArtistId'].foreign_keys
        self.assertEqual(len(fks), 1)


class PostgresqlDalTestCase(BackendTestCase):
    backend_path = 'origins.backends.postgresql'

    def setUp(self):
        self.load_backend()
        self.db = origins.connect('postgresql', database='chinook', host=HOST,
                                  user=USER, password=PASSWORD)

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

    def test_column_stddev(self):
        column = self.db.tables['track'].columns['milliseconds']
        self.assertGreater(column.stddev(), 0)

    def test_column_variance(self):
        column = self.db.tables['track'].columns['milliseconds']
        self.assertGreater(column.variance(), 0)

    def test_column_longest(self):
        column = self.db.tables['track'].columns['name']
        self.assertGreater(len(column.longest()), 0)

    def test_column_shortest(self):
        column = self.db.tables['track'].columns['name']
        self.assertGreater(len(column.shortest()), 0)

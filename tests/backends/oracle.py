from __future__ import unicode_literals, absolute_import

import origins
from .base import BackendTestCase


class OracleClientTestCase(BackendTestCase):
    backend_path = 'origins.backends.oracle'

    def setUp(self):
        self.load_backend()
        self.client = self.backend.Client(database='xe', user='chinook',
                                          password='p4ssw0rd')

    def test_database(self):
        self.assertTrue(self.client.database())

    def test_tables(self):
        tables = self.client.tables()
        self.assertEqual(len(tables), 11)
        self.assertTrue('name' in tables[0])

    def test_views(self):
        views = self.client.views()
        self.assertEqual(len(views), 0)

    def test_columns(self):
        columns = self.client.table_columns('album')
        self.assertEqual(len(columns), 3)
        self.assertTrue('name' in columns[0])

    def test_foreign_keys(self):
        fks = self.client.foreign_keys('album', 'ArtistId')
        self.assertEqual(len(fks), 1)


class OracleApiTestCase(BackendTestCase):
    backend_path = 'origins.backends.oracle'

    def setUp(self):
        self.load_backend()
        self.db = origins.connect('oracle', database='xe', user='chinook',
                                  password='p4ssw0rd')

    def test_db(self):
        self.assertEqual(self.db.label, 'xe')
        self.assertEqual(self.db.name, 'xe')
        self.assertEqual(self.db.path, 'xe')
        self.assertEqual(self.db.uri, 'oracle://localhost:1521/xe')
        self.assertEqual(self.db.relpath, [])
        self.assertTrue(self.db.isroot)
        self.assertFalse(self.db.isleaf)
        self.assertTrue('uri' in self.db.serialize())

    def test_table(self):
        table = self.db.tables['album']

        self.assertEqual(table.label, 'ALBUM')
        self.assertEqual(table.name, 'ALBUM')
        self.assertEqual(table.path, 'xe/ALBUM')
        self.assertEqual(len(table.relpath), 1)
        self.assertFalse(table.isroot)
        self.assertFalse(table.isleaf)

    def test_column(self):
        column = self.db.tables['album'].columns['albumid']

        self.assertEqual(column.label, 'ALBUMID')
        self.assertEqual(column.name, 'ALBUMID')
        self.assertEqual(column.path, 'xe/ALBUM/ALBUMID')
        self.assertEqual(len(column.relpath), 2)
        self.assertFalse(column.isroot)
        self.assertTrue(column.isleaf)

    def test_foreign_keys(self):
        fks = self.db.tables['album'].columns['artistid'].foreign_keys
        self.assertEqual(len(fks), 1)

    def test_db_schema(self):
        db = origins.connect('oracle', database='xe', user='chinook',
                             password='p4ssw0rd', schema='chinook')
        self.assertTrue(db.tables)

import os
from .base import BackendTestCase, TEST_DATA_DIR


class SqliteTestBase(BackendTestCase):
    backend_path = 'origins.backends.sqlite'

    def setUp(self):
        self.load_backend()
        path = os.path.join(TEST_DATA_DIR, 'chinook.sqlite')
        self.client = self.backend.Client(path)

    def test_client_methods(self):
        self.assertEqual(self.client.version(), 32)
        self.assertEqual(self.client.database_size(), 1067008)

    def test_tables(self):
        tables = self.client.tables()
        self.assertEqual(len(tables), 11)
        self.assertTrue('table_name' in tables[0])
        self.assertEqual(sorted(tables), tables)

        self.assertEqual(self.client.table_count('Album'), 347)

    def test_columns(self):
        columns = self.client.columns('Album')
        self.assertEqual(len(columns), 3)
        self.assertTrue('column_name' in columns[0])

        sorted_columns = sorted(columns, key=lambda x: x['column_index'])
        self.assertEqual(sorted_columns, columns)

        count = self.client.column_unique_count('Album', 'ArtistId')
        self.assertEqual(count, 204)

        values = list(self.client.column_unique_values('Album', 'ArtistId'))
        self.assertEqual(len(values), 204)

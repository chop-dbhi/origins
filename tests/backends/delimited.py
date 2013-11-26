import os
from .base import BackendTestCase, TEST_DATA_DIR


class DelimitedBackendTestCase(BackendTestCase):
    backend_path = 'origins.backends.delimited'

    def setUp(self):
        self.load_backend()
        path = os.path.join(TEST_DATA_DIR, 'chinook_tracks.csv')
        self.client = self.backend.Client(path)

    def test_client(self):
        self.assertEqual(self.client.header, [
            'TrackId',
            'Name',
            'AlbumId',
            'MediaTypeId',
            'GenreId',
            'Composer',
            'Milliseconds',
            'Bytes',
            'UnitPrice'])
        self.assertTrue(self.client.has_header)
        self.assertEqual(self.client.delimiter, ',')

    def test_client_reader(self):
        row = next(self.client.reader)
        self.assertEqual(row, [
            '1',
            'For Those About To Rock (We Salute You)',
            '1',
            '1',
            '1',
            'Angus Young, Malcolm Young, Brian Johnson',
            '343719',
            '11170334',
            '0.99'])

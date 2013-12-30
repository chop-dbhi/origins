import origins
from .base import BackendTestCase


class MongodbClientTestCase(BackendTestCase):
    backend_path = 'origins.backends.mongodb'

    def setUp(self):
        self.load_backend()
        self.client = self.backend.Client('chinook')

    def test_database(self):
        props = self.client.database()
        self.assertTrue(props['version'])
        self.assertTrue(self.client.database_stats())

    def test_collections(self):
        collections = self.client.collections()
        self.assertEqual(len(collections), 11)
        self.assertTrue('name' in collections[0])

    def test_fields(self):
        fields = self.client.fields('album')
        self.assertEqual(len(fields), 4)
        self.assertTrue('name' in fields[0])


class MongodbApiTestCase(BackendTestCase):
    backend_path = 'origins.backends.mongodb'

    def setUp(self):
        self.db = origins.connect('mongodb', database='chinook')

    def test_db(self):
        self.assertEqual(self.db.label, 'chinook')
        self.assertEqual(self.db.name, 'chinook')
        self.assertEqual(self.db.path, 'chinook')
        self.assertEqual(self.db.uri, 'mongodb://localhost:27017/chinook')
        self.assertEqual(self.db.relpath, [])
        self.assertTrue(self.db.isroot)
        self.assertFalse(self.db.isleaf)
        self.assertTrue('uri' in self.db.serialize())

    def test_collection(self):
        collection = self.db.collections[0]

        self.assertEqual(collection.label, 'album')
        self.assertEqual(collection.name, 'album')
        self.assertEqual(collection.path, 'chinook/album')
        self.assertEqual(len(collection.relpath), 1)
        self.assertFalse(collection.isroot)
        self.assertFalse(collection.isleaf)

    def test_field(self):
        field = self.db.collections[0].fields[0]

        self.assertEqual(field.label, 'AlbumId')
        self.assertEqual(field.name, 'AlbumId')
        self.assertEqual(field.path, 'chinook/album/AlbumId')
        self.assertEqual(len(field.relpath), 2)
        self.assertFalse(field.isroot)
        self.assertTrue(field.isleaf)

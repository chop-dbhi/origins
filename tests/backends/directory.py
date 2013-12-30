import origins
from .base import BackendTestCase, TEST_DATA_DIR


class DirectoryClientTestCase(BackendTestCase):
    backend_path = 'origins.backends.directory'

    def setUp(self):
        self.load_backend()
        self.client = self.backend.Client(TEST_DATA_DIR)

    def test_setup(self):
        self.assertTrue(self.client.recurse)
        self.assertIsNone(self.client.depth)

    def test_directory(self):
        self.assertTrue(self.client.directory())

    def test_files(self):
        files = self.client.files()
        self.assertGreater(files, 0)


class DirectoryApiTestCase(BackendTestCase):
    backend_path = 'origins.backends.directory'

    def setUp(self):
        self.directory = origins.connect('directory', path=TEST_DATA_DIR)

    def test_directory(self):
        self.assertEqual(self.directory.label, 'data')
        self.assertEqual(self.directory.name, 'data')
        self.assertEqual(self.directory.path, 'data')
        self.assertEqual(self.directory.uri, 'directory:///data')
        self.assertEqual(self.directory.relpath, [])
        self.assertTrue(self.directory.isroot)
        self.assertFalse(self.directory.isleaf)
        self.assertTrue('uri' in self.directory.serialize())

    def test_fields(self):
        column = self.directory.files['chinook.sqlite']

        self.assertEqual(column.label, 'chinook.sqlite')
        self.assertEqual(column.name, 'chinook.sqlite')
        self.assertEqual(column.path, 'data/chinook.sqlite')
        self.assertEqual(len(column.relpath), 1)
        self.assertFalse(column.isroot)
        self.assertTrue(column.isleaf)

    def test_no_recurse(self):
        d = origins.connect('directory', path=TEST_DATA_DIR, recurse=False)
        # Dirty way of ensuring the walk did not recurse
        self.assertFalse([f for f in d.files if '/' in f['name']])

    def test_explicit_depth(self):
        d = origins.connect('directory', path=TEST_DATA_DIR, depth=1)
        self.assertTrue([f for f in d.files if '/' in f['name']])

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


class DirectoryNodeTestCase(BackendTestCase):
    backend_path = 'origins.backends.directory'

    def setUp(self):
        self.load_backend()
        self.origin = origins.connect('directory', path=TEST_DATA_DIR)

    def test_sync(self):
        self.assertTrue(self.origin.props)
        self.assertTrue(self.origin.files)

    def test_no_recurse(self):
        node = origins.connect('directory', path=TEST_DATA_DIR, recurse=False)
        # Dirty way of ensuring the walk did not recurse
        self.assertFalse([f for f in node.files if '/' in f['name']])

    def test_explicit_depth(self):
        node = origins.connect('directory', path=TEST_DATA_DIR, depth=1)
        self.assertTrue([f for f in node.files if '/' in f['name']])

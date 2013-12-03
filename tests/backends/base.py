import os
import unittest
from importlib import import_module

TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), '../data')


class BackendTestCase(unittest.TestCase):
    backend_path = ''

    def load_backend(self):
        if not self.backend_path:
            raise ValueError('backend_path must be defined')
        try:
            self.backend = import_module(self.backend_path)
        except ImportError as e:
            self.skipTest(unicode(e))

    def setUp(self):
        self.load_backend()

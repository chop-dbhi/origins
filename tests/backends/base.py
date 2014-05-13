from __future__ import unicode_literals, absolute_import

import unittest
from importlib import import_module
from tests import TEST_DATA_DIR  # noqa


class BackendTestCase(unittest.TestCase):
    backend_path = ''

    def load_backend(self):
        if not self.backend_path:
            raise ValueError('backend_path must be defined')
        try:
            self.backend = import_module(self.backend_path)
        except ImportError as e:
            self.skipTest(str(e))

    def setUp(self):
        self.load_backend()

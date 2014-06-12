from __future__ import unicode_literals, absolute_import

from .base import BackendTestCase


class NoopClientTestCase(BackendTestCase):
    backend_path = 'origins.backends.noop'

    def setUp(self):
        self.load_backend()
        self.client = self.backend.Client()

    def test(self):
        self.assertTrue(self.client)

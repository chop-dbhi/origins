import os
import unittest
import origins
from importlib import import_module

TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), '../data')


class ExporterTestCase(unittest.TestCase):
    exporter_path = ''

    def load_exporter(self):
        if not self.exporter_path:
            raise ValueError('exporter_path must be defined')
        try:
            self.exporter = import_module(self.exporter_path)
        except ImportError as e:
            self.skipTest(unicode(e))

    def load_test_node(self):
        node = origins.connect('sqlite', path=os.path.join(TEST_DATA_DIR,
                               'chinook.sqlite'))

        # Pre-load foreign key relatexporternships.
        for table in node.tables:
            for column in table.columns:
                column.foreign_keys

        return node

    def setUp(self):
        self.load_exporter()

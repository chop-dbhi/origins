from __future__ import unicode_literals

import os
import json
import unittest
from origins import graph
from .. import TEST_DATA_DIR

export_data_path = os.path.join(TEST_DATA_DIR, 'chinook.sqlite.json')


class GraphTestCase(unittest.TestCase):
    def setUp(self):
        with open(export_data_path) as f:
            self.data = json.load(f)

        graph.delete_resource(self.data)

    def test_create_resource(self):
        out = graph.create_resource(self.data)

        self.assertEqual(out['nodes'], 76)
        self.assertEqual(out['relationships'], 86)

    def test_sync_resource(self):
        out = graph.sync_resource(self.data)

        self.assertEqual(out['nodes']['added'], 76)
        self.assertEqual(out['relationships']['added'], 86)

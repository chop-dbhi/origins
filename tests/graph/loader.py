from __future__ import unicode_literals, absolute_import

import os
import json
import unittest
from origins import graph
from origins.graph import neo4j
from tests import TEST_DATA_DIR


export_data_path = os.path.join(TEST_DATA_DIR, 'chinook.sqlite.json')


class GraphTestCase(unittest.TestCase):
    def setUp(self):
        neo4j.purge()

        with open(export_data_path) as f:
            self.data = json.load(f)

    def tearDown(self):
        neo4j.purge()

    def test_create_resource(self):
        out = graph.create_resource(self.data)

        self.assertEqual(out['nodes'], 76)
        self.assertEqual(out['relationships'], 86)

    def test_sync_resource(self):
        out = graph.sync_resource(self.data)

        self.assertEqual(out['nodes'], {
            'added': 76,
            'updated': 0,
            'removed': 0,
        })

        self.assertEqual(out['relationships'], {
            'added': 86,
            'updated': 0,
            'removed': 0,
        })

        out = graph.sync_resource(self.data)

        self.assertEqual(out['nodes'], {
            'added': 0,
            'updated': 0,
            'removed': 0,
        })

        self.assertEqual(out['relationships'], {
            'added': 0,
            'updated': 0,
            'removed': 0,
        })

        self.data['nodes']['chinook.sqlite/Album/Title']['properties']['type'] = 'NVARCHAR(200)'  # noqa

        out = graph.sync_resource(self.data)

        self.assertEqual(out['nodes'], {
            'added': 0,
            'updated': 1,
            'removed': 0,
        })

    def test_delete_resource(self):
        graph.create_resource(self.data)
        out = graph.delete_resource(self.data)
        self.assertEqual(out['nodes'], 76)
        self.assertEqual(out['relationships'], 86)

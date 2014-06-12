from __future__ import unicode_literals

import os
import json
import unittest
from origins.provenance import graph
from origins.graph import neo4j
from .. import TEST_DATA_DIR


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

        self.assertEqual(out['prov:entity'], 324)
        self.assertEqual(out['prov:specializationOf'], 162)
        self.assertEqual(out['prov:wasGeneratedBy'], 162)
        self.assertEqual(out['origins:wasDependentOn'], 86)

    def test_sync_resource(self):
        out = graph.sync_resource(self.data)

        self.assertEqual(out['prov:entity'], 324)
        self.assertEqual(out['prov:specializationOf'], 162)
        self.assertEqual(out['prov:wasGeneratedBy'], 162)
        self.assertEqual(out['origins:wasDependentOn'], 86)

        out = graph.sync_resource(self.data)

        self.assertEqual(out['prov:specializationOf'], 0)
        self.assertEqual(out['prov:wasGeneratedBy'], 0)
        self.assertEqual(out['prov:wasInvalidatedBy'], 0)
        self.assertEqual(out['origins:wasDependentOn'], 0)

        self.data['nodes']['chinook.sqlite/Album/Title']['properties']['type'] = 'NVARCHAR(200)'  # noqa

        out = graph.sync_resource(self.data)

        self.assertEqual(out['prov:specializationOf'], 1)
        self.assertEqual(out['prov:wasGeneratedBy'], 1)
        self.assertEqual(out['prov:wasInvalidatedBy'], 1)
        self.assertEqual(out['origins:wasDependentOn'], 0)

    def test_delete_resource(self):
        graph.create_resource(self.data)
        out = graph.delete_resource(self.data)

        self.assertEqual(out['nodes'], 734)
        self.assertEqual(out['relationships'], 658)

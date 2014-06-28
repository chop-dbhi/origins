from __future__ import unicode_literals, absolute_import

import os
import json
import unittest
from copy import deepcopy
from origins.graph import resources, neo4j
from origins.graph.sync import sync
from tests import TEST_DATA_DIR

export_data_path = os.path.join(TEST_DATA_DIR, 'chinook.sqlite-export.json')


class SyncTestCase(unittest.TestCase):
    def setUp(self):
        neo4j.purge()

        with open(export_data_path) as f:
            self.data = json.load(f)
            self.r = self.data['resource']

    def test_immutable_data(self):
        copy = deepcopy(self.data)
        sync(self.data)
        self.assertEqual(copy, self.data)

    def test_sync(self):
        out = sync(self.data)

        self.assertEqual(out['components'], {
            'added': 76,
            'updated': 0,
            'removed': 0,
        })

        self.assertEqual(out['relationships'], {
            'added': 86,
            'updated': 0,
            'removed': 0,
        })

        # Confirm in the graph itself
        self.assertEqual(len(resources.components(self.r)), 76)
        self.assertEqual(len(resources.relationships(self.r)), 86)

    def test_idempotent(self):
        sync(self.data)

        out = sync(self.data)

        self.assertEqual(out['components'], {
            'added': 0,
            'updated': 0,
            'removed': 0,
        })

        self.assertEqual(out['relationships'], {
            'added': 0,
            'updated': 0,
            'removed': 0,
        })

        # Confirm in the graph itself
        self.assertEqual(len(resources.components(self.r)), 76)
        self.assertEqual(len(resources.relationships(self.r)), 86)

    def test_update(self):
        out = sync(self.data)

        self.data['components']['chinook.sqlite/Album/Title']['properties']['type'] = 'NVARCHAR(200)'  # noqa

        out = sync(self.data)

        self.assertEqual(out['components'], {
            'added': 0,
            'updated': 1,
            'removed': 0,
        })

        components = filter(lambda x: x['id'] == 'chinook.sqlite/Album/Title',
                            resources.components(self.r))

        self.assertEqual(list(components)[0]['properties']['type'],
                         'NVARCHAR(200)')

    def test_remove(self):
        out = sync(self.data)

        del self.data['components']['chinook.sqlite/Album/Title']

        out = sync(self.data)

        self.assertEqual(out['components'], {
            'added': 0,
            'updated': 0,
            'removed': 1,
        })

        components = filter(lambda x: x['id'] == 'chinook.sqlite/Album/Title',
                            resources.components(self.r))

        # Invalid component
        self.assertFalse(list(components)[0]['valid'])

    def test_no_add(self):
        out = sync(self.data, add=False)

        self.assertEqual(out['components'], {
            'added': 0,
            'updated': 0,
            'removed': 0,
        })

        self.assertEqual(out['relationships'], {
            'added': 0,
            'updated': 0,
            'removed': 0,
        })

    def test_no_update(self):
        out = sync(self.data)

        self.data['components']['chinook.sqlite/Album/Title']['properties']['type'] = 'NVARCHAR(200)'  # noqa

        out = sync(self.data, update=False)

        self.assertEqual(out['components'], {
            'added': 0,
            'updated': 0,
            'removed': 0,
        })

    def test_no_remove(self):
        out = sync(self.data)

        del self.data['components']['chinook.sqlite/Album/Title']

        out = sync(self.data, remove=False)

        self.assertEqual(out['components'], {
            'added': 0,
            'updated': 0,
            'removed': 0,
        })

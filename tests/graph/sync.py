from __future__ import unicode_literals, absolute_import

import os
import json
import unittest
from copy import deepcopy
from origins.graph import sync, resources, neo4j
from tests import TEST_DATA_DIR


export_data_path = os.path.join(TEST_DATA_DIR, 'chinook.sqlite.json')


class SyncTestCase(unittest.TestCase):
    def setUp(self):
        neo4j.purge()

        with open(export_data_path) as f:
            self.data = json.load(f)

    def tearDown(self):
        neo4j.purge()

    def test_immutable_input(self):
        copy = deepcopy(self.data)
        sync(self.data)
        self.assertEqual(copy, self.data)

    def test_idempotent(self):
        out = sync(self.data)
        uuid = out['resource']['uuid']

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
        self.assertEqual(len(resources.components(uuid)), 76)
        self.assertEqual(len(resources.relationships(uuid)), 86)

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

    def test_update(self):
        out = sync(self.data)
        uuid = out['resource']['uuid']

        self.data['components']['chinook.sqlite/Album/Title']['properties']['type'] = 'NVARCHAR(200)'  # noqa

        out = sync(self.data)

        self.assertEqual(out['components'], {
            'added': 0,
            'updated': 1,
            'removed': 0,
        })

        components = filter(lambda x: x['id'] == 'chinook.sqlite/Album/Title',
                            resources.components(uuid))

        self.assertEqual(components[0]['properties']['type'], 'NVARCHAR(200)')

    def test_remove(self):
        out = sync(self.data)
        uuid = out['resource']['uuid']

        del self.data['components']['chinook.sqlite/Album/Title']

        out = sync(self.data)

        self.assertEqual(out['components'], {
            'added': 0,
            'updated': 0,
            'removed': 1,
        })

        components = filter(lambda x: x['id'] == 'chinook.sqlite/Album/Title',
                            resources.components(uuid))

        # None match
        self.assertFalse(components)

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

from __future__ import unicode_literals, absolute_import

import unittest
from origins.graph import resources, neo4j


class ResourcesTestCase(unittest.TestCase):
    def setUp(self):
        neo4j.purge()

    def tearDown(self):
        neo4j.purge()

    def test_create_by_id(self):
        resource = resources.create(1)

        self.assertEqual(resources.get(resource['uuid']), resource)

    def test_create_with_properties(self):
        resource = resources.create(1, {
            'other': 'foo',
        })

        self.assertEqual(resources.get(resource['uuid']), resource)

    def test_create_packed(self):
        resource = resources.create({
            'origins:id': 1,
            'other': 'foo',
        })

        self.assertEqual(resources.get(resource['uuid']), resource)

    def test_delete(self):
        resource = resources.create(1)
        out = resources.delete(resource['uuid'])

        self.assertEqual(out['components'], 0)
        self.assertEqual(out['relationships'], 0)

        self.assertFalse(resources.get())

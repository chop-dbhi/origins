from __future__ import unicode_literals, absolute_import

import unittest
from origins.graph import resources, components, neo4j


class ResourcesTestCase(unittest.TestCase):
    def setUp(self):
        neo4j.purge()

    def tearDown(self):
        neo4j.purge()

    def test_create_by_id(self):
        r = resources.create(1)
        c = components.create(1, resource=r)

        self.assertEqual(components.get(c['uuid']), c)

    def test_create_with_properties(self):
        r = resources.create(1)
        c = components.create(1, {
            'other': 'foo',
        }, resource=r)

        self.assertEqual(components.get(c['uuid']), c)

    def test_create_packed(self):
        r = resources.create(1)

        c = components.create({
            'origins:id': 1,
            'origins:resource': r['uuid'],
            'other': 'foo',
        })

        self.assertEqual(components.get(c['uuid']), c)

    def test_delete(self):
        r = resources.create(1)
        c = components.create(1, resource=r)

        out = components.delete(c['uuid'])

        self.assertEqual(out['components'], 1)
        self.assertEqual(out['relationships'], 2)

        self.assertFalse(components.get())

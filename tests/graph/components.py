from __future__ import unicode_literals, absolute_import

import unittest
from origins.graph import resources, relationships, components, neo4j


class ComponentsTestCase(unittest.TestCase):
    def setUp(self):
        neo4j.purge()

        self.r = resources.create(1)

        self.c1 = components.create(1, resource=self.r)

        self.c2 = components.create(2, {
            'other': 'foo',
        }, resource=self.r['id'])

        self.c3 = components.create({
            'origins:id': 3,
            'other': 'foo',
        }, resource={'origins:id': self.r['id']})

        self.c4 = components.create({
            'origins:id': 4,
            'other': 'food',
            'origins:resource': self.r,
        })

    def test_create(self):
        comps = (self.c1, self.c2, self.c3, self.c4)

        for c in comps:
            self.assertTrue('id' in c)
            self.assertTrue('uuid' in c)
            self.assertTrue('timestamp' in c)
            self.assertTrue('properties' in c)

            self.assertTrue('resource' not in c)

    def test_get_by_id(self):
        result = self.c1

        self.assertEqual(components.get(self.c1['id']), result)

    def test_get_by_uuid(self):
        result = self.c1

        self.assertEqual(components.get(self.c1['uuid']), result)

    def test_match(self):
        result = [self.c1, self.c2, self.c3, self.c4]

        self.assertEqual(components.match(), result)

    def test_match_skip_limit(self):
        result = [self.c2]

        self.assertEqual(components.match(limit=1, skip=1), result)

    def test_match_with_predicate(self):
        result = [self.c1]

        self.assertEqual(components.match({'origins:id': 1}), result)

    def test_search(self):
        result = [self.c2, self.c3, self.c4]

        self.assertEqual(components.search({'other': 'fo.*'}), result)

    def test_update(self):
        c = self.c1

        c1 = components.update(c['id'], {
            'foo': 'bar',
        })

        # Same ID
        self.assertEqual(c['id'], c1['id'])

        # New UUID
        self.assertNotEqual(c['uuid'], c1['uuid'])

    def test_delete(self):
        c = components.delete(self.c1)
        self.assertFalse(c['valid'])

    def test_resource(self):
        r = components.resource(self.c1['id'])

        self.assertEqual(r, self.r)

    def test_relationships(self):
        rel = relationships.create(1, self.c1, 'related', self.c2,
                                   resource=self.r)

        result = components.relationships(self.c1)
        self.assertEqual([rel], result)

        result = components.relationships(self.c2)
        self.assertEqual([rel], result)

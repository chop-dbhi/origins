import unittest
from origins.graph import collections, resources, neo4j


class CollectionsTestCase(unittest.TestCase):
    def setUp(self):
        neo4j.purge()

    def test_create_by_id(self):
        c = collections.create(1)
        self.assertEqual(c['id'], 1)

        self.assertEqual(collections.get(1), c)

    def test_create_with_properties(self):
        c = collections.create(1, {'other': 'foo'})
        self.assertEqual(c['id'], 1)

        self.assertEqual(collections.get(1), c)

    def test_create_packed(self):
        c = collections.create({'origins:id': 1, 'other': 'foo'})
        self.assertEqual(c['id'], 1)

        self.assertEqual(collections.get(1), c)

    def test_create_error(self):
        self.assertRaises(KeyError, collections.create, {'foo': 1})

    def test_get(self):
        c = collections.create(1)

        self.assertEqual(collections.get({'origins:id': 1}), c)

    def test_update(self):
        c = collections.create(1)
        c = collections.update(c, {'origins:label': 'test'})

        self.assertEqual(c['label'], 'test')

    def test_get_by_id(self):
        c = collections.create(1)

        self.assertEqual(collections.get(1), c)

    def test_get_by_uuid(self):
        c = collections.create(1)

        self.assertEqual(collections.get(c['uuid']), c)

    def test_match(self):
        c = collections.create(1)

        self.assertEqual(collections.match(), [c])

    def test_match_skip_limit(self):
        collections.create(1)
        r2 = collections.create(2)
        collections.create(3)

        self.assertEqual(collections.match(limit=1, skip=1), [r2])

    def test_match_with_predicate(self):
        c = collections.create(1)

        self.assertEqual(collections.match({'origins:id': 1}), [c])

    def test_delete(self):
        c = collections.create(1)
        collections.delete(c)

        self.assertFalse(collections.get(1))

    def test_add(self):
        c = collections.create(1)
        r1 = resources.create(1)
        r2 = resources.create(2)

        collections.add(c, r1)
        collections.add(c, r2)

        self.assertEqual(len(collections.resources(c)), 2)

    def test_remove(self):
        c = collections.create(1)
        r1 = resources.create(1)
        r2 = resources.create(2)

        collections.add(c, r1)
        collections.add(c, r2)

        collections.remove(c, r1)

        self.assertEqual(len(collections.resources(c)), 1)

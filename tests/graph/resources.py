import unittest
from origins.graph import resources, components, relationships, neo4j


class ResourcesTestCase(unittest.TestCase):
    def setUp(self):
        neo4j.purge()

        # ID-only
        self.s1 = resources.create(1)

        # ID with properties
        self.s2 = resources.create(2, {
            'other': 'foo',
        })

        # Properties only
        self.s3 = resources.create({
            'origins:id': 3,
            'other': 'foo',
        })

        self.c1 = components.create(1, resource=self.s1)
        self.c2 = components.create(2, resource=self.s1)

        self.r1 = relationships.create(1, self.c1, 'references', self.c2,
                                       resource=self.s1)

        self.c3 = components.create(3, resource=self.s2)

        resources.include(self.s1, self.c3)

    def test_create_by_id(self):
        self.assertEqual(self.s1['id'], 1)
        self.assertEqual(self.s1['properties'], {})
        self.assertTrue('uuid' in self.s1)
        self.assertEqual(resources.get(1), self.s1)

    def test_create_id_with_properties(self):
        self.assertEqual(self.s2['id'], 2)
        self.assertEqual(self.s2['properties'], {
            'other': 'foo',
        })
        self.assertTrue('uuid' in self.s2)
        self.assertEqual(resources.get(2), self.s2)

    def test_create_by_properties(self):
        self.assertEqual(self.s3['id'], 3)
        self.assertEqual(self.s3['properties'], {
            'other': 'foo',
        })
        self.assertTrue('uuid' in self.s3)
        self.assertEqual(resources.get(3), self.s3)

    def test_create_error(self):
        self.assertRaises(KeyError, resources.create, {'foo': 1})

    def test_update(self):
        self.s1 = resources.update(self.s1, {
            'origins:label': 'test',
        })

        self.assertEqual(self.s1['label'], 'test')

    def test_get_by_id(self):
        s = resources.get(1)

        self.assertEqual(s, self.s1)

    def test_get_by_uuid(self):
        s = resources.get(self.s1['uuid'])

        self.assertEqual(s, self.s1)

    def test_get_by_predicate(self):
        s = resources.get({'origins:id': 1})

        self.assertEqual(s, self.s1)

    def test_match(self):
        s = resources.match()

        self.assertEqual(s, [self.s1, self.s2, self.s3])

    def test_match_skip_limit(self):
        s = resources.match(limit=1, skip=1)

        self.assertEqual(s, [self.s2])

    def test_match_by_predicate(self):
        s = resources.match({'other': 'foo'})

        self.assertEqual(s, [self.s2, self.s3])

    def test_delete(self):
        resources.delete(self.s1)

        self.assertFalse(resources.get(self.s1))
        self.assertFalse(resources.components(self.r1))
        self.assertFalse(resources.relationships(self.r1))

    def test_components(self):
        r = resources.components(self.s1)

        self.assertEqual(r, [self.c1, self.c2, self.c3])

    def test_managed_components(self):
        r = resources.components(self.s1, managed=True)

        self.assertEqual(r, [self.c1, self.c2])

    def test_relationships(self):
        r = resources.relationships(self.s1)

        self.assertEqual(r, [self.r1])

    def test_managed_relationships(self):
        r = resources.relationships(self.s1, managed=True)

        self.assertEqual(r, [self.r1])

    def test_timeline(self):
        r = resources.timeline(self.s1)

        self.assertEqual(len(r), 12)

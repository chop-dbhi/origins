import unittest
from origins.graph import resources, components, relationships, neo4j, utils


class RelationshipsTestCase(unittest.TestCase):
    def setUp(self):
        neo4j.purge()

        self.s = resources.create(1)

        self.c1 = components.create(1, resource=self.s)
        self.c2 = components.create(2, resource=self.s)

        self.r = relationships.create(1, self.c1, 'references', self.c2,
                                      resource=self.s)

    def test_create_by_id(self):
        self.assertEqual(self.r['id'], 1)

        self.assertEqual(relationships.get(self.r['uuid']), self.r)

    def test_create_with_properties(self):
        r = relationships.create(1, properties={
            'origins:start': self.c1,
            'origins:end': self.c2,
            'origins:type': 'references',
            'other': 'foo',
        }, resource=utils.pack(self.s))

        self.assertEqual(r['id'], 1)

        self.assertEqual(relationships.get(r['uuid']), r)

    def test_create_packed(self):
        r = relationships.create({
            'origins:id': 1,
            'origins:start': self.c1,
            'origins:end': self.c2,
            'origins:type': 'references',
            'other': 'foo',
        }, resource=self.s)

        self.assertEqual(r['id'], 1)

        self.assertEqual(relationships.get(r['uuid']), r)

    def test_create_packed_resource(self):
        r = relationships.create({
            'origins:id': 1,
            'origins:start': self.c1,
            'origins:end': self.c2,
            'origins:type': 'references',
            'origins:resource': self.s,
            'other': 'foo',
        })

        # Select keys are removed from properties
        self.assertFalse('resource' in r)

        # Embedded
        self.assertTrue(isinstance(r['start'], dict))
        self.assertTrue(isinstance(r['end'], dict))

        self.assertEqual(relationships.get(r['uuid']), r)

    def test_get_by_id(self):
        self.assertEqual(relationships.get(self.r['id']), self.r)

    def test_get_by_uuid(self):
        self.assertEqual(relationships.get(self.r['uuid']), self.r)

    def test_match(self):
        self.assertEqual(relationships.match(), [self.r])

    def test_match_skip_limit(self):
        r2 = relationships.create(2, self.c1, 'foo', self.c2, resource=self.s)
        relationships.create(3, self.c1, 'bar', self.c2, resource=self.s)

        self.assertEqual(relationships.match(limit=1, skip=1), [r2])

    def test_match_with_predicate(self):
        self.assertEqual(relationships.match({'origins:id': 1}), [self.r])

    def test_revisions(self):
        r = relationships.revisions(self.r)
        self.assertEqual(r, [self.r])

    def test_update(self):
        r1 = relationships.update(self.r, {'foo': 'bar'})

        self.assertEqual(self.r['id'], r1['id'])
        self.assertNotEqual(self.r['uuid'], r1['uuid'])

        self.assertEqual(len(relationships.revisions(self.r)), 2)

    def test_delete(self):
        r = relationships.delete(self.r)
        self.assertFalse(r['valid'])

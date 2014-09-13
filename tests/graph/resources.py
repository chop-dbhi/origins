import unittest
from origins.exceptions import ValidationError, DoesNotExist
from origins.graph import neo4j
from origins.graph import Resource, Component, Relationship


class ResourceTestCase(unittest.TestCase):
    def setUp(self):
        neo4j.purge()

    def test_match(self):
        r0 = Resource.add()
        p0 = Resource.add(type='Database')

        self.assertEqual(Resource.match(), [r0, p0])

    def test_add(self):
        r0 = Resource.add(id='my-resource')

        # Cannot add with same ID
        self.assertRaises(ValidationError, Resource.add, id=r0.id)

    def test_set(self):
        r0 = Resource.add()
        r1 = Resource.set(r0.uuid, label='My Resource')

        self.assertEqual(r0.id, r1.id)
        self.assertNotEqual(r0.uuid, r1.uuid)
        self.assertEqual(r1.label, 'My Resource')

    def test_remove(self):
        r = Resource.add()
        Resource.remove(r.uuid)

        self.assertRaises(DoesNotExist, Resource.get_by_id, r.id)

    def test_components(self):
        r = Resource.add()
        p = Resource.add()

        c = Component.add(resource=r)
        d = Component.add(resource=p)

        Resource.include_component(r.uuid, component=d)

        self.assertEqual(Resource.components(r.uuid), [c, d])
        self.assertEqual(Resource.components(p.uuid), [d])
        self.assertEqual(Resource.managed_components(r.uuid), [c])
        self.assertEqual(Resource.managed_components(p.uuid), [d])

    def test_relationships(self):
        r = Resource.add()
        p = Resource.add()

        x = Component.add(resource=r)
        y = Component.add(resource=p)

        c = Relationship.add(x, y, resource=r)
        d = Relationship.add(x, y, type='other', resource=p)

        Resource.include_relationship(r.uuid, relationship=d)

        self.assertEqual(Resource.relationships(r.uuid), [c, d])
        self.assertEqual(Resource.relationships(p.uuid), [d])
        self.assertEqual(Resource.managed_relationships(r.uuid), [c])
        self.assertEqual(Resource.managed_relationships(p.uuid), [d])

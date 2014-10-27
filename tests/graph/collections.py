import unittest
from origins.exceptions import ValidationError, DoesNotExist
from origins.graph import neo4j
from origins.graph.collections import Collection
from origins.graph.resources import Resource


class CollectionTestCase(unittest.TestCase):
    def setUp(self):
        neo4j.client.purge()

    def test_match(self):
        r0 = Collection.add()
        p0 = Collection.add(label='Databases')

        self.assertEqual(Collection.match(), [r0, p0])

    def test_add(self):
        r0 = Collection.add(id='collection')

        # Cannot add with same ID
        self.assertRaises(ValidationError, Collection.add, id=r0.id)

    def test_set(self):
        r0 = Collection.add()
        r1 = Collection.set(r0.uuid, label='My Collection')

        self.assertEqual(r0.id, r1.id)
        self.assertNotEqual(r0.uuid, r1.uuid)
        self.assertEqual(r1.label, 'My Collection')

    def test_remove(self):
        r = Collection.add()
        Collection.remove(r.uuid)

        self.assertRaises(DoesNotExist, Collection.get_by_id, r.id)

    def test_resources(self):
        r = Collection.add()
        p = Collection.add()

        c = Resource.add()
        d = Resource.add()

        Collection.add_resource(r.uuid, resource=c)
        Collection.add_resource(r.uuid, resource=d)

        Collection.add_resource(p.uuid, resource=d)

        self.assertCountEqual(Collection.resources(r.uuid), [d, c])
        self.assertEqual(Collection.resources(p.uuid), [d])

        r1 = Collection.set(r.uuid, label='Special')
        self.assertCountEqual(Collection.resources(r1.uuid), [c, d])

        self.assertEqual(Collection.resource_count(r.uuid), 0)
        self.assertEqual(Collection.resource_count(r1.uuid), 2)
        self.assertEqual(Collection.resource_count(p.uuid), 1)

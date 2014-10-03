import unittest
from origins.exceptions import ValidationError, DoesNotExist
from origins.graph import neo4j
from origins.graph.relationships import Relationship
from origins.graph.components import Component
from origins.graph.resources import Resource


class RelationshipTestCase(unittest.TestCase):
    def setUp(self):
        neo4j.client.purge()

        self.r = Resource.add()

        self.a0 = Component.add(type='Column', label='artist_id',
                                resource=self.r)
        self.b0 = Component.add(type='Column', label='id', resource=self.r)

    def test_match(self):
        r0 = Relationship.add(self.a0, self.b0, resource=self.r,
                              type='foreignkey')

        self.assertEqual(Relationship.match(), [r0])

    def test_add(self):
        Relationship.add(self.a0, self.b0, id='relationship',
                         resource=self.r)

        # Cannot add with same ID
        self.assertRaises(ValidationError, Relationship.add,
                          self.a0, self.b0, id='relationship',
                          resource=self.r)

    def test_set(self):
        r0 = Relationship.add(self.a0, self.b0, resource=self.r)
        r1 = Relationship.set(r0.uuid, label='Relationship')

        self.assertEqual(r0.id, r1.id)
        self.assertNotEqual(r0.uuid, r1.uuid)

    def test_remove(self):
        c = Relationship.add(self.a0, self.b0, resource=self.r)
        Relationship.remove(c.uuid)

        self.assertRaises(DoesNotExist, Relationship.get_by_id, id=c.id,
                          resource=self.r)

    def test_resource(self):
        c = Relationship.add(self.a0, self.b0, resource=self.r)
        r = Relationship.resource(c.uuid)

        self.assertEqual(r, self.r)

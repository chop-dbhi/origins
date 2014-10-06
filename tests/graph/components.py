import unittest
from origins.exceptions import ValidationError, DoesNotExist
from origins.graph import neo4j
from origins.graph.resources import Resource
from origins.graph.components import Component


class ComponentTestCase(unittest.TestCase):
    def setUp(self):
        neo4j.purge()

        self.r = Resource.add()

    def test_match(self):
        r0 = Component.add(resource=self.r)
        p0 = Component.add(type='Database', resource=self.r)

        self.assertEqual(Component.match(), [r0, p0])

    def test_add(self):
        c = Component.add(id='component', resource=self.r)

        # Cannot add with same ID
        self.assertRaises(ValidationError, Component.add, id=c.id,
                          resource=self.r)

    def test_set(self):
        c0 = Component.add(resource=self.r)
        c1 = Component.set(c0.uuid, label='My Component')

        self.assertEqual(c0.id, c1.id)
        self.assertNotEqual(c0.uuid, c1.uuid)

        self.assertEqual(Component.resource(c1.uuid), self.r)

    def test_remove(self):
        c = Component.add(resource=self.r)
        Component.remove(c.uuid)

        self.assertRaises(DoesNotExist, Component.get_by_id, c.id,
                          resource=self.r)

    def test_resource(self):
        c = Component.add(resource=self.r)
        r = Component.resource(c.uuid)

        self.assertEqual(r, self.r)

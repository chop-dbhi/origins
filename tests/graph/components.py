import unittest
from origins.exceptions import ValidationError, DoesNotExist
from origins.graph import neo4j, components, resources


class ComponentTestCase(unittest.TestCase):
    def setUp(self):
        neo4j.purge()

        self.r = resources.add()

    def test_match(self):
        r0 = components.add(self.r.uuid)
        p0 = components.add(self.r.uuid, type='Database')
        self.assertEqual(components.match(), [r0, p0])

    def test_add(self):
        # Without ID
        r = components.add(self.r.uuid)
        self.assertTrue(r.type, components.COMPONENT_TYPE)

        # With ID
        r = components.add(self.r.uuid, id='component')
        self.assertTrue(r.type, components.COMPONENT_TYPE)

        # Cannot add with same ID
        self.assertRaises(ValidationError, components.add, self.r.uuid,
                          id='component')

    def test_set(self):
        r0 = components.add(self.r.uuid)
        r1 = components.set(r0.uuid, label='My Component')

        self.assertEqual(r0.id, r1.id)
        self.assertNotEqual(r0.uuid, r1.uuid)

    def test_remove(self):
        r = components.add(self.r.uuid)
        components.remove(r.uuid)

        self.assertRaises(DoesNotExist, components.get_by_id, self.r.uuid,
                          id=r.id)

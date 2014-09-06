import unittest
from origins.exceptions import ValidationError, DoesNotExist
from origins.graph import neo4j, resources, components


class ResourceTestCase(unittest.TestCase):
    def setUp(self):
        neo4j.purge()

    def test_match(self):
        r0 = resources.add()
        p0 = resources.add(type='Database')
        self.assertEqual(resources.match(), [r0, p0])

    def test_add(self):
        # Without ID
        r = resources.add()
        self.assertTrue(r.type, resources.RESOURCE_TYPE)

        # With ID
        r = resources.add('my-resource')
        self.assertTrue(r.type, resources.RESOURCE_TYPE)

        # Cannot add with same ID
        self.assertRaises(ValidationError, resources.add, 'my-resource')

    def test_set(self):
        r0 = resources.add()
        r1 = resources.set(r0.uuid, label='My Resource')

        self.assertEqual(r0.id, r1.id)
        self.assertNotEqual(r0.uuid, r1.uuid)

    def test_remove(self):
        r = resources.add()
        resources.remove(r.uuid)

        self.assertRaises(DoesNotExist, resources.get_by_id, r.id)

    def test_components(self):
        r = resources.add()
        p = resources.add()

        c = components.add(r.uuid)
        d = components.add(p.uuid)

        self.assertEqual(resources.components(r.uuid), [c])
        self.assertEqual(resources.components(p.uuid), [d])

from origins.graph import trends, Resource, Component, Relationship
from .base import TestCase


class TrendsTestCase(TestCase):
    def setUp(self):
        super(TrendsTestCase, self).setUp()

        self.r1 = Resource.add('one')
        self.r2 = Resource.add('two')

        self.r1_cmps = [Component.add(resource=self.r1) for _ in range(5)]
        self.r2_cmps = [Component.add(resource=self.r2) for _ in range(5)]

        for i in range(5):
            Relationship.add(self.r1_cmps[i], self.r2_cmps[i],
                             resource=self.r1)
            Resource.include_component(self.r2.uuid, self.r1_cmps[i])

    def test_connected_components(self):
        result = trends.connected_components()

        self.assertEqual(max([r['count'] for r in result]), 1)
        self.assertEqual(len(result), 10)

    def test_used_components(self):
        result = trends.used_components()

        self.assertCountEqual([r['component'] for r in result], self.r1_cmps)

    def test_connected_resources(self):
        result = trends.connected_resources()

        self.assertEqual(result[0], {
            'resource': self.r1,
            'count': 5,
        })

    def test_used_resources(self):
        result = trends.used_resources()

        self.assertEqual(result[0], {
            'resource': self.r1,
            'count': 5,
        })

    def test_component_sources(self):
        result = trends.component_sources()

        self.assertEqual(result, [])

    def test_relationship_types(self):
        result = trends.relationship_types()

        self.assertEqual(result, [{'count': 5, 'type': 'related'}])

    def test_component_types(self):
        result = trends.component_types()

        self.assertEqual(result, [{'count': 10, 'type': 'Component'}])

    def test_resource_types(self):
        result = trends.resource_types()

        self.assertEqual(result, [{'count': 2, 'type': 'Resource'}])

import os
from origins.graph.sync import sync
from origins import encoding
from tests import TEST_DATA_DIR
from .base import TestCase


data_path = os.path.join(TEST_DATA_DIR, 'chinook-resource.json')


class SyncTestCase(TestCase):
    def test_minimum(self):
        data = {
            'resource': {
                'id': 'foo',
            }
        }

        r = sync(data)

        self.assertTrue(r['resource'])

    def test_full(self):
        with open(data_path) as f:
            data = encoding.json.load(f)

        r = sync(data)

        self.assertEqual(r['components']['added'], 76)
        self.assertEqual(r['relationships']['added'], 86)

import os
import unittest
from origins import encoding
from tests import TEST_DATA_DIR


class CsvLoaderTestCase(unittest.TestCase):
    def test(self):
        file_path = os.path.join(TEST_DATA_DIR, 'resource-example.csv')

        with open(file_path) as f:
            data = encoding.csv.load(f, {
                'id': 'test',
            })

            self.assertEqual(len(data['components']), 7)
            self.assertEqual(len(data['relationships']), 3)


class JsonLoaderTestCase(unittest.TestCase):
    def test(self):
        file_path = os.path.join(TEST_DATA_DIR, 'chinook-resource.json')

        with open(file_path) as f:
            data = encoding.json.load(f)
            self.assertEqual(len(data['components']), 76)
            self.assertEqual(len(data['relationships']), 11)


class YamlLoaderTestCase(unittest.TestCase):
    def test(self):
        file_path = os.path.join(TEST_DATA_DIR, 'chinook-resource.yaml')

        with open(file_path) as f:
            data = encoding.yaml.load(f)
            self.assertEqual(len(data['components']), 76)
            self.assertEqual(len(data['relationships']), 11)

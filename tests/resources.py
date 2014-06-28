from __future__ import unicode_literals, absolute_import

import os
import unittest
import origins
from origins.resources import ResourceExporter, export
from tests import TEST_DATA_DIR


DB_PATH = os.path.join(TEST_DATA_DIR, 'chinook.sqlite')


class Exporter(ResourceExporter):
    def relationship_data(self, rel):
        data = super(Exporter, self).relationship_data(rel)
        data['label'] = repr(rel)
        return data


class ResourceExporterTestCase(unittest.TestCase):
    def setUp(self):
        self.db = origins.connect('sqlite', path=DB_PATH)

    def test_function(self):
        data = export('test', self.db)

        self.assertEqual(data['resource'], 'test')
        self.assertEqual(len(data['components']), 76)
        self.assertEqual(len(data['relationships']), 86)

    def test_subclass(self):
        e = Exporter('test')
        data = e.export(self.db)

        self.assertEqual(len(data['components']), 76)
        self.assertEqual(len(data['relationships']), 86)

        self.assertTrue('label' in list(data['relationships'].values())[0])

    def test_node(self):
        data = self.db.export()

        self.assertEqual(data['resource'], {
            'origins:id': self.db.uri,
            'origins:label': self.db.label,
        })

        self.assertEqual(len(data['components']), 76)
        self.assertEqual(len(data['relationships']), 86)

        # Export one table
        artist = self.db.tables['Artist']
        data = artist.export()

        self.assertEqual(data['resource'], {
            'origins:id': artist.uri,
            'origins:label': artist.label,
        })

        self.assertEqual(len(data['components']), 3)
        self.assertEqual(len(data['relationships']), 2)

    def test_partial(self):
        e = ResourceExporter('test')

        e.export([
            self.db.tables['Artist'],
            self.db.tables['Album'],
        ], incoming=False)

        # No arguments return data in current state
        data = e.export()

        self.assertEqual(len(data['components']), 7)
        self.assertEqual(len(data['relationships']), 6)

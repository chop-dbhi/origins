from __future__ import unicode_literals, absolute_import

import os
import unittest
from origins.provenance import load_document
from origins.graph import neo4j
from tests import TEST_DATA_DIR


class LoaderTestCase(unittest.TestCase):
    def setUp(self):
        neo4j.purge()

    tearDown = setUp

    def test(self):
        doc = os.path.join(TEST_DATA_DIR, 'prov-primer.json')
        load_document(doc)
        # Distinct combinations of relationships to node types
        self.assertEqual(len(neo4j.summary()), 17)

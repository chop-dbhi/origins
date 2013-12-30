import os
import origins
from .base import ExporterTestCase

NEO4J_ENDPOINT = os.environ.get('NEO4J_ENDPOINT')


class Neo4jTestCase(ExporterTestCase):
    exporter_path = 'origins.io.neo4j'

    def test_export_node(self):
        node = self.load_test_node()
        output = origins.export('neo4j', node, uri=NEO4J_ENDPOINT)
        self.assertGreater(output['nodes'], 0)
        self.assertGreater(output['rels'], 0)

    def test_export_rel(self):
        node = self.load_test_node()
        rel = node.rels()[0]
        output = origins.export('neo4j', rel, uri=NEO4J_ENDPOINT)
        self.assertGreater(output['nodes'], 0)
        self.assertGreater(output['rels'], 0)

    def test_export_multi(self):
        node = self.load_test_node()
        rels = node.rels()
        output = origins.export('neo4j', rels, uri=NEO4J_ENDPOINT)
        self.assertGreater(output['nodes'], 0)
        self.assertGreater(output['rels'], 0)

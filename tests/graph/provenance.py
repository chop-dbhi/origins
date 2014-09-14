from origins.graph import neo4j, provenance
from .base import TestCase


class LoadTestCase(TestCase):
    def test(self):
        provenance.load({
            'agent': {
                'joe': {},
            },
            'activity': {
                'writing': {},
            },
            'wasAssociatedWith': {
                'waw': {
                    'prov:agent': 'joe',
                    'prov:activity': 'writing',
                }
            },
        })

        # Ensure graph exists
        r = neo4j.tx.send('''
            MATCH (:`prov:Agent`)<-[:`prov:agent`]-(:`prov:Association`)-[:`prov:activity`]->(:`prov:Activity`) RETURN true
        ''')  # noqa

        self.assertTrue(r[0][0])

from __future__ import unicode_literals

import unittest
from origins.graph import neo4j


class Neo4jTestCase(unittest.TestCase):
    def setUp(self):
        neo4j.purge()

    def test_client(self):
        client = neo4j.Client()

        with client.transaction() as tx:
            result = tx.send('CREATE (n {foo: 1}) RETURN n')[0][0]
            self.assertEqual(result, {'foo': 1})

    def test_batch(self):
        with neo4j.Transaction(batch_size=2) as tx:
            tx.send([{
                'statement': 'CREATE ({ props })',
                'parameters': {'props': {'foo': 1}},
            }, {
                'statement': 'CREATE ({ props })',
                'parameters': {'props': {'foo': 2}},
            }, {
                'statement': 'CREATE ({ props })',
                'parameters': {'props': {'foo': 3}},
            }])

            self.assertEqual(tx.batches, 2)

            self.assertEqual(tx.send('MATCH (n) RETURN count(n)')[0][0], 3)

    def test_rollback(self):
        tx = neo4j.Transaction()

        # Create node
        tx.send('CREATE ({})')

        # Visibility in transaction
        self.assertEqual(len(tx.send('MATCH (n) RETURN n')), 1)

        tx.rollback()

        # Change not committed
        self.assertEqual(len(neo4j.send('MATCH (n) RETURN n')), 0)

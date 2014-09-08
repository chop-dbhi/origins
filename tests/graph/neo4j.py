import unittest
import logging
from origins.graph import neo4j


class MockHandler(logging.Handler):
    def __init__(self, *args, **kwargs):
        self.reset()
        logging.Handler.__init__(self, *args, **kwargs)

    def emit(self, record):
        self.messages[record.levelname.lower()].append(record.getMessage())

    def reset(self):
        self.messages = {
            'debug': [],
            'info': [],
            'warning': [],
            'error': [],
            'critical': [],
        }


class Neo4jTestCase(unittest.TestCase):
    def setUp(self):
        neo4j.purge()

    def test_batch(self):
        with neo4j.client.transaction(batch_size=2) as tx:
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

            self.assertEqual(tx._batches, 2)
            self.assertEqual(tx.send('MATCH (n) RETURN count(n)')[0][0], 3)

    def test_rollback(self):
        tx = neo4j.client.transaction()

        # Create node
        tx.send('CREATE ({})')

        # Visibility in transaction
        self.assertEqual(len(tx.send('MATCH (n) RETURN n')), 1)

        tx.rollback()

        # Change not committed
        self.assertEqual(len(neo4j.tx.send('MATCH (n) RETURN n')), 0)

    def test_nesting(self):
        with neo4j.tx as tx1:
            self.assertEqual(tx1._depth, 1)

            with tx1 as tx2:
                self.assertEqual(tx2._depth, 2)

                with tx2 as tx3:
                    self.assertEqual(tx3._depth, 3)

                    tx3.send('CREATE (n)')

                self.assertEqual(tx2._depth, 2)
                self.assertFalse(tx2._closed)

            self.assertEqual(tx1._depth, 1)
            self.assertFalse(tx1._closed)

        self.assertIsNotNone(neo4j.tx.send('MATCH (n) RETURN id(n)')[0][0])

    def test_autocommit(self):
        with neo4j.tx as tx1:
            tx1.send('CREATE (n)')

            with tx1 as tx2:
                tx2.send('CREATE (n)')

            tx1.rollback()

        neo4j.tx.send('CREATE (n)')
        r = neo4j.tx.send('MATCH (n) RETURN n')

        self.assertEqual(len(r), 1)

    def test_uncommitted(self):
        handler = MockHandler(logging.ERROR)
        neo4j.logger.addHandler(handler)

        tx = neo4j.client.transaction()

        # Uncommitted
        tx.send('CREATE (n)')
        # Unsent
        tx.send('CREATE (n)', wait=True)

        # Mimic call on exit
        neo4j._transaction_exit(tx)

        self.assertEqual(len(handler.messages['error']), 2)

        # Remove handler for subsequent tests
        neo4j.logger.removeHandler(handler)

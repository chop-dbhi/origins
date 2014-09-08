import unittest
from origins import events
from origins.graph import neo4j


class TestCase(unittest.TestCase):
    def setUp(self):
        neo4j.purge()
        events.purge()

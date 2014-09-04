import unittest
from origins.events import utils
from origins.graph import neo4j


class TestCase(unittest.TestCase):
    def setUp(self):
        neo4j.purge()
        utils.reset()

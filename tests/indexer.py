from __future__ import unicode_literals, absolute_import

import unittest
from origins import indexer


class IndexerTestCase(unittest.TestCase):
    def setUp(self):
        # a -> b -> c
        self.rels1 = (
            ('a', 'b'),
            ('b', 'c'),
        )

        # a -> b, c
        self.rels2 = (
            ('a', 'b'),
            ('a', 'c'),
        )

        # a -> b with extra args
        self.rels3 = (
            ('a', 'b', {}),
        )


class BuildIndex(IndexerTestCase):
    def test_none(self):
        self.assertEqual(indexer.build(()), {})

    def test_build_1(self):
        self.assertEqual(indexer.build(self.rels1), {
            'a': {
                'b': ('a', 'b'),
            },
            'b': {
                'a': 'a',
                'c': ('b', 'c'),
            },
            'c': {
                'b': 'b',
            }
        })

    def test_build_2(self):
        self.assertEqual(indexer.build(self.rels2), {
            'a': {
                'b': ('a', 'b'),
                'c': ('a', 'c'),
            },
            'b': {
                'a': 'a',
            },
            'c': {
                'a': 'a',
            }
        })

    def test_build_3(self):
        self.assertEqual(indexer.build(self.rels3), {
            'a': {
                'b': ('a', 'b', {}),
            },
            'b': {
                'a': 'a',
            }
        })


class CrawlIndex(IndexerTestCase):
    def test_none(self):
        self.assertEqual(indexer.crawl({}), {})

    def test_path_to_target(self):
        index = indexer.build(self.rels1)

        self.assertEqual(indexer._path_to_target(index, 'a', 'a'), None)
        self.assertEqual(indexer._path_to_target(index, 'a', 'b'), (1, 'b'))
        self.assertEqual(indexer._path_to_target(index, 'a', 'c'), (2, 'b'))

        self.assertEqual(indexer._path_to_target(index, 'b', 'b'), None)
        self.assertEqual(indexer._path_to_target(index, 'b', 'a'), (1, 'a'))
        self.assertEqual(indexer._path_to_target(index, 'b', 'c'), (1, 'c'))

        self.assertEqual(indexer._path_to_target(index, 'c', 'c'), None)
        self.assertEqual(indexer._path_to_target(index, 'c', 'b'), (1, 'b'))
        self.assertEqual(indexer._path_to_target(index, 'c', 'a'), (2, 'b'))

    def test_crawl_1(self):
        index = indexer.build(self.rels1)

        self.assertEqual(indexer.crawl(index), {
            'a': {
                'b': ('a', 'b'),
                'c': 'b',
            },
            'b': {
                'a': 'a',
                'c': ('b', 'c'),
            },
            'c': {
                'b': 'b',
                'a': 'b',
            },
        })

    def test_crawl_2(self):
        index = indexer.build(self.rels2)

        self.assertEqual(indexer.crawl(index), {
            'a': {
                'b': ('a', 'b'),
                'c': ('a', 'c'),
            },
            'b': {
                'a': 'a',
                'c': 'a',
            },
            'c': {
                'a': 'a',
                'b': 'a',
            },
        })

    def test_crawl_3(self):
        index = indexer.build(self.rels3)

        self.assertEqual(indexer.crawl(index), {
            'a': {
                'b': ('a', 'b', {}),
            },
            'b': {
                'a': 'a',
            }
        })


class ResolveIndex(IndexerTestCase):
    def test_none(self):
        self.assertEqual(indexer.resolve({}), {})

    def test_resolve_path(self):
        index = indexer.crawl(indexer.build(self.rels1))

        self.assertEqual(indexer._resolve_path(index, 'a', 'a'), None)
        self.assertEqual(indexer._resolve_path(index, 'a', 'b'), [
            ('a', 'b'),
        ])
        self.assertEqual(indexer._resolve_path(index, 'a', 'c'), [
            ('a', 'b'),
            ('b', 'c'),
        ])

        self.assertEqual(indexer._resolve_path(index, 'b', 'b'), None)
        self.assertEqual(indexer._resolve_path(index, 'b', 'a'), [
            ('b', 'a'),
        ])
        self.assertEqual(indexer._resolve_path(index, 'b', 'c'), [
            ('b', 'c'),
        ])

        self.assertEqual(indexer._resolve_path(index, 'c', 'c'), None)
        self.assertEqual(indexer._resolve_path(index, 'c', 'b'), [
            ('c', 'b'),
        ])
        self.assertEqual(indexer._resolve_path(index, 'c', 'a'), [
            ('c', 'b'),
            ('b', 'a'),
        ])

    def test_resolve_1(self):
        index = indexer.crawl(indexer.build(self.rels1))

        self.assertEqual(indexer.resolve(index), {
            'a': {
                'b': (
                    ('a', 'b'),
                ),
                'c': (
                    ('a', 'b'),
                    ('b', 'c')
                ),
            },
            'b': {
                'a': (
                    ('b', 'a'),
                ),
                'c': (
                    ('b', 'c'),
                ),
            },
            'c': {
                'b': (
                    ('c', 'b'),
                ),
                'a': (
                    ('c', 'b'),
                    ('b', 'a'),
                ),
            },
        })

    def test_resolve_2(self):
        index = indexer.crawl(indexer.build(self.rels2))

        self.assertEqual(indexer.resolve(index), {
            'a': {
                'b': (
                    ('a', 'b'),
                ),
                'c': (
                    ('a', 'c'),
                ),
            },
            'b': {
                'a': (
                    ('b', 'a'),
                ),
                'c': (
                    ('b', 'a'),
                    ('a', 'c'),
                ),
            },
            'c': {
                'a': (
                    ('c', 'a'),
                ),
                'b': (
                    ('c', 'a'),
                    ('a', 'b'),
                )
            },
        })

    def test_resolve_3(self):
        index = indexer.crawl(indexer.build(self.rels3))

        self.assertEqual(indexer.resolve(index), {
            'a': {
                'b': (
                    ('a', 'b', {}),
                ),
            },
            'b': {
                'a': (
                    ('b', 'a', {}),
                ),
            }
        })

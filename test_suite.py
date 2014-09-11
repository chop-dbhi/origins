#!/usr/bin/env python

if __name__ == '__main__':
    import sys
    import unittest

    loader = unittest.TestLoader()
    runner = unittest.runner.TextTestRunner()

    args = sys.argv[1:]

    if '--debug' in args:
        args.remove('--debug')

        import logging
        from origins.graph import neo4j

        neo4j.debug()
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        neo4j.logger.addHandler(handler)

    tests = None

    if args:
        if 'TestCase' in args[0]:
            tests = loader.loadTestsFromNames(args)
        else:
            start_dir = args[0]
    else:
        start_dir = 'tests'

    if tests is None:
        tests = loader.discover(start_dir, '*.py', top_level_dir='tests')

    result = runner.run(tests)

    if result.errors or result.failures:
        sys.exit(1)

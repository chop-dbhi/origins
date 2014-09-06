#!/usr/bin/env python

if __name__ == '__main__':
    import sys
    import unittest

    loader = unittest.TestLoader()
    runner = unittest.runner.TextTestRunner()

    tests = None

    if sys.argv[1:]:
        if 'TestCase' in sys.argv[1]:
            tests = loader.loadTestsFromNames(sys.argv[1:])
        else:
            start_dir = sys.argv[1]
    else:
        start_dir = 'tests'

    if tests is None:
        tests = loader.discover(start_dir, '*.py', top_level_dir='tests')

    result = runner.run(tests)

    if result.errors or result.failures:
        sys.exit(1)

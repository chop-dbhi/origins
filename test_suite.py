#!/usr/bin/env python

if __name__ == '__main__':
    import sys
    import unittest

    loader = unittest.TestLoader()
    runner = unittest.runner.TextTestRunner()

    args = sys.argv[1:]

    from origins import config

    if '--debug' in args:
        args.remove('--debug')
        config.set_loglevel('DEBUG')
    else:
        # Set above critical bound to prevent any messages from
        # being emitted
        config.set_loglevel(100)

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

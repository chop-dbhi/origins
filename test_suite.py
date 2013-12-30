#!/usr/bin/env python

if __name__ == '__main__':
    import sys
    import unittest
    import logging
    from origins import logger

    # Increase log level to prevent polluting test suite output
    logger.setLevel(logging.ERROR)

    loader = unittest.TestLoader()
    runner = unittest.runner.TextTestRunner()
    if sys.argv[1:]:
        tests = loader.loadTestsFromNames(sys.argv[1:])
    else:
        tests = loader.discover('tests', '*.py')
    runner.run(tests)

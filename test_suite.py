#!/usr/bin/env python

if __name__ == '__main__':
    import unittest
    loader = unittest.TestLoader()
    runner = unittest.runner.TextTestRunner()
    tests = loader.discover('tests', '*_test.py')
    runner.run(tests)

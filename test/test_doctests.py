import doctest
import unittest

import heimdall


def load_tests(loader, tests, ignore):
    tests.addTests(doctest.DocTestSuite(heimdall))
    return tests

import unittest
from . import node, link


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    tl = unittest.TestLoader()
    suites = []
    suites.append( tl.loadTestsFromModule(node) )
    suites.append( tl.loadTestsFromModule(link) )
    s = unittest.TestSuite(suites)
    unittest.TextTestRunner().run(s)
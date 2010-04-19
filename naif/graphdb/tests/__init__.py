import unittest
import node, link, nodefeed

def suite():
    tl = unittest.TestLoader()
    suites = []
    suites.append( tl.loadTestsFromModule(node) )
    suites.append( tl.loadTestsFromModule(link) )
    suites.append( tl.loadTestsFromModule(nodefeed) )
    return unittest.TestSuite(suites)
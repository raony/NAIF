import unittest
import node, link, nodefeed, linkfeed

def suite():
    tl = unittest.TestLoader()
    suites = []
    suites.append( tl.loadTestsFromModule(node) )
    suites.append( tl.loadTestsFromModule(link) )
    suites.append( tl.loadTestsFromModule(nodefeed) )
    suites.append( tl.loadTestsFromModule(linkfeed) )
    return unittest.TestSuite(suites)
'''
Created on Apr 19, 2010

@author: raony
'''
import unittest
from graphdb.graphdatabase import GraphDatabase


class GraphDatabaseTest(unittest.TestCase):


    def setUp(self):
        self.graphdb = GraphDatabase('neo_test/', True)
        self.transaction= self.graphdb.transaction()

    def tearDown(self):
        self.transaction.failure()
        self.transaction.finish()
        self.graphdb.shutdown()

    def testName(self):
        pass


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
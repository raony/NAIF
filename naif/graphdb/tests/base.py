'''
Created on Apr 19, 2010

@author: raony
'''
import unittest
from graphdb.graphdatabase import GraphDatabase


class GraphDatabaseTest(unittest.TestCase):


    def setUp(self):
        self.graphdb = GraphDatabase('neo_test2/', True)
        self.transaction= self.graphdb.transaction()

    def tearDown(self):
        self.transaction.failure()
        self.transaction.finish()
        self.graphdb.shutdown()

    def testName(self):
        pass

class TransactionMock(object):
    def __init__(self):
        self.success_called = 0
        self.failure_called = 0
        self.finish_called = 0
        
    def success(self):
        self.success_called += 1
    def failure(self):
        self.failure_called += 1
    def finish(self):
        self.finish_called += 1

class FeedTest(GraphDatabaseTest):
    
    def setUp(self):
        super(FeedTest, self).setUp()
        self.transactMock = TransactionMock()

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
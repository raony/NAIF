'''
Created on Apr 15, 2010

@author: raony
'''
from graphdatabase import GraphDatabase
import unittest

class GraphDatabaseTest(unittest.TestCase):


    def setUp(self):
        self.graphdb = GraphDatabase('neo_test/', True)
        self.transaction= self.graphdb.transaction()


    def tearDown(self):
        self.transaction.failure()
        self.transaction.finish()
        self.graphdb.shutdown()

    def testNewNode(self):
        node = self.graphdb.new_node(id=25)
        self.assertEquals(25, node.id)
        self.assertEquals('node', node.type)
        
    def testNewNodeRefRelation(self):
        node = self.graphdb.new_node(5)
        self.assertEquals(self.graphdb.__neo__.ref, node.__node__.NODE.single.start)
    
    def testGetNodeByID(self):
        node = self.graphdb.new_node(16)
        self.assertEquals(node, self.graphdb.node[16])
    
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testFirstTimeNodeIndexInit']
    unittest.main()
'''
Created on Apr 15, 2010

@author: raony
'''
from graphdatabase import GraphDatabase, Node
import unittest

class GraphDatabaseTest(unittest.TestCase):


    def setUp(self):
        self.graphdb = GraphDatabase('neo_test/', True)
        self.transaction= self.graphdb.transaction()


    def tearDown(self):
        self.transaction.failure()
        self.transaction.finish()
        self.graphdb.shutdown()

    def test_new_node(self):
        node = self.graphdb.node(id=25)
        self.assertEquals(25, node.id)
        self.assertEquals('node', node.type)
        
    def test_new_node_ref_relation(self):
        node = self.graphdb.node(5)
        self.assertEquals(self.graphdb.__neo__.ref, node.__node__.NODE.single.start)
    
    def test_get_node_by_id(self):
        node = self.graphdb.node(16)
        self.assertEquals(node, self.graphdb.node[16])
    
    def test_contains_node_by_id(self):
        self.graphdb.node(16)
        self.assertTrue(16 in self.graphdb.node)
        
    def test_not_contains_node_id(self):
        self.assertTrue(27 not in self.graphdb.node)
    
    def test_duplicate_node_exception(self):
        try:
            self.graphdb.node(16)
            self.graphdb.node(16)
            self.fail('should raise a Node.AlreadyExist exception')
        except Node.AlreadyExist:
            pass
    
    def test_node_type_property(self):
        node = self.graphdb.node(32, type='event')
        self.assertEquals('event', node.type)
    
    def test_list_types(self):
        self.graphdb.node(32, type='event')
        self.graphdb.node(11, type='member')
        self.assertEquals(2, len(self.graphdb.node.type))
        self.assertTrue('event' in self.graphdb.node.type)
        self.assertTrue('member' in self.graphdb.node.type)
    
    def test_list_nodes_by_type(self):
        e1 = self.graphdb.node(32, type='event')
        m1 = self.graphdb.node(11, type='member')
        e2 = self.graphdb.node(15, type='event')
        nodes = [n for n in self.graphdb.node.type('event')]
        self.assertTrue(2, len(nodes))
        self.assertTrue(e1 in nodes)
        self.assertTrue(e2 in nodes)
        for n in self.graphdb.node.type('member'):
            self.assertEquals(m1, n)
    
    
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testFirstTimeNodeIndexInit']
    unittest.main()
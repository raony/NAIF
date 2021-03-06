'''
Created on Apr 19, 2010

@author: raony
'''
import unittest
from graphdb.nodes import Node
from base import GraphDatabaseTest


class NodeTest(GraphDatabaseTest):

    def test_new_node(self):
        node = self.graphdb.node(id=25)
        self.assertEquals(25, node.id)
        self.assertEquals('node', node.type)
        
    def test_new_node_ref_relation(self):
        node = self.graphdb.node(5)
        self.assertEquals(self.graphdb.__neo__.ref, node.__node__.node.single.start)
    
    def test_get_node_by_id(self):
        node = self.graphdb.node(16)
        self.assertEquals(node, self.graphdb.node[16])
        
    def test_list_all_nodes(self):
        e1 = self.graphdb.node(32, type='event')
        e2 = self.graphdb.node(11, type='member')
        e3 = self.graphdb.node(5, type='event')
        e4= self.graphdb.node(6)
        result = [n for n in self.graphdb.node]
        self.assertEquals(4, len(result))
        self.assertTrue(e1 in result)
        self.assertTrue(e2 in result)
        self.assertTrue(e3 in result)
        self.assertTrue(e4 in result)
        
        
    def test_node_not_found(self):
        self.assertTrue(not self.graphdb.node[16])
    
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
    
    def test_node_generic_properties(self):
        name = 'teste nome'
        age = 16
        node = self.graphdb.node(32, name=name, age=age)
        self.assertEquals(name, node['name'])
        self.assertEquals(age, node['age'])
    
    def test_set_node_generic_properties(self):
        name = 'teste nome'
        node = self.graphdb.node(47)
        node['name'] = name
        self.assertEquals(name, node['name'])
    
    def test_set_node_type_exception(self):
        node = self.graphdb.node(47)
        try:
            node['type'] = 'arroz'
            self.fail('should raise a KeyError exception.')
        except KeyError:
            self.assertEquals('node', node.type)
    
    def test_contains_node_property(self):
        node = self.graphdb.node(47, name='edgar')
        self.assertTrue('name' in node)
        self.assertTrue('age' not in node)

    def test_del_node_property(self):
        node = self.graphdb.node(47, name='edgar')
        del node['name']
        self.assertTrue('name' not in node)
    
    def test_del_node_type_property(self):
        node = self.graphdb.node(47, name='edgar')
        try:
            del node['type']
            self.fail('Should raise KeyError.')
        except KeyError:
            pass

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
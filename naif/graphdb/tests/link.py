'''
Created on Apr 19, 2010

@author: raony
'''
import unittest
from graphdb.tests.base import GraphDatabaseTest
from graphdb.graphdatabase import LinkType, Link


class LinkTest(GraphDatabaseTest):

    def test_new_link(self):
        start = self.graphdb.node(1)
        end = self.graphdb.node(2)
        link = self.graphdb.link(8, start, end)
        self.assertEquals(start, link.start)
        self.assertEquals(end, link.end)
        self.assertEquals(8, link.id)
        self.assertEquals('LINK', link.type)
        
    def test_get_link_by_id(self):
        start = self.graphdb.node(1)
        end = self.graphdb.node(2)
        link = self.graphdb.link(12, start, end)
        self.assertEquals(link.__link__, self.graphdb.link[12].__link__)
    
    def test_contains_link_by_id(self):
        start = self.graphdb.node(1)
        end = self.graphdb.node(2)
        self.graphdb.link(16, start, end)
        self.assertTrue(16 in self.graphdb.link)
        
    def test_not_contains_link_id(self):
        start = self.graphdb.node(1)
        end = self.graphdb.node(2)
        self.graphdb.link(16, start, end)
        self.assertTrue(27 not in self.graphdb.link)
        
    def test_duplicate_link_exception(self):
        try:
            start = self.graphdb.node(1)
            end = self.graphdb.node(2)
            self.graphdb.link(16, start, end)
            self.graphdb.link(16, end, start)
            self.fail('should raise a Node.AlreadyExist exception')
        except Link.AlreadyExist:
            pass
    
    def test_link_type_property(self):
        start = self.graphdb.node(1)
        end = self.graphdb.node(2)
        link = self.graphdb.link(16, start, end, type='KNOW')
        self.assertEquals('KNOW', link.type)
    
    def test_link_binary_property(self):
        start = self.graphdb.node(1)
        end = self.graphdb.node(2)
        link = self.graphdb.link(16, start, end, type='KNOW')
        self.assertTrue(link.binary)
        self.assertEquals(1.0, link.strength)
        
    def test_link_valued_property(self):
        start = self.graphdb.node(1)
        end = self.graphdb.node(2)
        link = self.graphdb.link(16, start, end, type='KNOW', strength=7.5)
        self.assertTrue(link.binary == False)
        self.assertEquals(7.5, link.strength)
        
    def test_link_generic_properties(self):
        source = 'e-mail'
        age = 16
        start = self.graphdb.node(1)
        end = self.graphdb.node(2)
        link = self.graphdb.link(16, start, end, source=source, age=age)
        self.assertEquals(source, link['source'])
        self.assertEquals(age, link['age'])
    
    def test_set_link_generic_properties(self):
        age = 16
        start = self.graphdb.node(1)
        end = self.graphdb.node(2)
        link = self.graphdb.link(16, start, end)
        link['age'] = age
        self.assertEquals(age, link['age'])

    def test_set_link_binary_exception(self):
        start = self.graphdb.node(1)
        end = self.graphdb.node(2)
        link = self.graphdb.link(16, start, end)
        try:
            link['binary'] = False
            self.fail('should raise a KeyError exception.')
        except KeyError:
            self.assertEquals(True, link.binary)
    
    def test_set_link_strength_exception(self):
        start = self.graphdb.node(1)
        end = self.graphdb.node(2)
        link = self.graphdb.link(16, start, end, strength=6.8)
        try:
            link['strength'] = 5.5
            self.fail('should raise a KeyError exception.')
        except KeyError:
            self.assertEquals(6.8, link.strength)
    
    def test_contains_link_property(self):
        start = self.graphdb.node(1)
        end = self.graphdb.node(2)
        link = self.graphdb.link(16, start, end, familiar=True)
        self.assertTrue('familiar' in link)
        self.assertTrue('age' not in link)
    
    def test_mutually_exclusive_strength_or_binary(self):
        start = self.graphdb.node(1)
        end = self.graphdb.node(2)
        try:
            self.graphdb.link(16, start, end, binary=True, strength=2.3)
            self.fail("should raise ValueError.")
        except ValueError:
            pass
    
    def test_list_link_types(self):
        start = self.graphdb.node(1)
        end = self.graphdb.node(2)
        self.graphdb.link(16, start, end, type="CHILD")
        self.graphdb.link(17, start, end, type="FRIEND", strength=3.4)
        self.assertEquals(2, len(self.graphdb.link.type))
        self.assertTrue('CHILD' in self.graphdb.link.type)
        self.assertTrue('FRIEND' in self.graphdb.link.type)
        self.assertEquals(LinkType.BINARY, filter(lambda x: x == 'CHILD', self.graphdb.link.type)[0].binary)
        self.assertEquals(LinkType.VALUED, filter(lambda x: x == 'FRIEND', self.graphdb.link.type)[0].binary)
        self.graphdb.link(18, end, start, type="FRIEND")
        self.assertEquals(LinkType.MIXED, filter(lambda x: x == 'FRIEND', self.graphdb.link.type)[0].binary)


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
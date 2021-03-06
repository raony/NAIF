'''
Created on Apr 19, 2010

@author: raony
'''
import unittest
from base import GraphDatabaseTest
from graphdb.links import LinkType, Link
from datetime import datetime


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
        self.assertEquals(link, self.graphdb.link[12])
    
    def test_link_not_found(self):
        self.assertTrue(not self.graphdb.link[12])
    
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
    
    def test_del_link_property(self):
        start = self.graphdb.node(1)
        end = self.graphdb.node(2)
        link = self.graphdb.link(16, start, end, familiar=True)
        del link['familiar']
        self.assertTrue('familiar' not in link)
        
    def test_del_binary_exception(self):
        start = self.graphdb.node(1)
        end = self.graphdb.node(2)
        link = self.graphdb.link(16, start, end, familiar=True)
        try:
            del link['binary']
            self.fail('Should rise a KeyError exception.')
        except KeyError:
            pass
    
    def test_del_strength_exception(self):
        start = self.graphdb.node(1)
        end = self.graphdb.node(2)
        link = self.graphdb.link(16, start, end, familiar=True)
        try:
            del link['strength']
            self.fail('Should rise a KeyError exception.')
        except KeyError:
            pass
    
    def test_remove_and_list(self):
        
        links = []
        for i in range(10):
            links.append(self.graphdb.link(i, self.graphdb.node(i*2), self.graphdb.node(i*2+1)))
        
        import random
        for j in range(2):
            k = random.randint(0,9-j)
            del self.graphdb.link[ links[k].id ]
            del links[k]

#        FIXME: currently i do not know how to easily make a independent tests with successful transactions
#        FIXME: without the self.transaction.success the relationship lookup of neo4j python still returns deleted links
#        self.transaction.success()
#        self.transaction.finish()
#        self.transaction = self.graphdb.transaction()
        
        for link in links:
            self.assertEquals(link, self.graphdb.link[link.id])
            
        for link in self.graphdb.link:
            try:
                self.assertTrue(link in links)
            except:
                print link
                raise
        
        for link in links:
            try:
                self.assertTrue(link.id in self.graphdb.link)
            except:
                print link
                raise
    
    def test_time_measured(self):
        start = self.graphdb.node(1)
        end = self.graphdb.node(2)
        self.graphdb.link(16, start, end, familiar=True)
        self.assertTrue((datetime.today() - self.graphdb.link[16].measured_time).seconds < 1.0)

    def test_time_measured_as_parameter(self):
        start = self.graphdb.node(1)
        end = self.graphdb.node(2)
        self.graphdb.link(16, start, end, familiar=True, measured_time=datetime.today())
        self.assertTrue((datetime.today() - self.graphdb.link[16].measured_time).seconds < 1.0)
        
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
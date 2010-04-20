'''
Created on Apr 19, 2010

@author: raony
'''
import unittest
from graphdb.graphdatabase import FeedResult, LinkFeed, NodeReadPolicy
from base import FeedTest

class LinkFeedTest(FeedTest):

    def test_read_links_ok(self):
        links = [{'id': 1,
                  'start': {'id': 1, 'type': 'typeA'}, 
                  'end': {'id': 2, 'type': 'typeB'},
                  'type': 'KNOWS'},
                  {'id': 2,
                   'start': {'id': 2, 'type': 'typeC'},
                  'end': {'id': 3, 'type': 'typeC'},
                  'type': 'WORKSFOR'}]
        result = self.graphdb.read_links(LinkFeed(links, node_policy=NodeReadPolicy.UPDATE), self.transactMock)
        self.assertEquals(FeedResult.OK, result.status)
        self.assertEquals(2, result.inserted)
#        self.assertEquals(3, result.nodes.inserted)
        print result.nodes
        
         
        


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.test_read_links_ok']
    unittest.main()
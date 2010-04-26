'''
Created on Apr 19, 2010

@author: raony
'''
import unittest
from graphdb.graphdatabase import FeedResult, LinkFeed, FeedPolicy
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
        result = self.graphdb.read_links(LinkFeed(links, link_policy= FeedPolicy(FeedPolicy.CREATE),
                                                   node_policy=FeedPolicy(FeedPolicy.CREATE_AND_UPDATE)), self.transactMock)
        self.assertEquals(FeedResult.OK, result.status)
        self.assertEquals(2, result.inserted)
        self.assertEquals(3, result.nodes.inserted)
#        print result.nodes

    def test_missing_nodes(self):
        links = [{'id': 1,
                  'start': {'id': 1, 'type': 'typeA'}, 
                  'end': {'id': 2, 'type': 'typeB'},
                  'type': 'KNOWS'},
                  {'id': 2,
                   'start': {'id': 2, 'type': 'typeC'},
                  'end': {'id': 3, 'type': 'typeC'},
                  'type': 'WORKSFOR'}]
        result = self.graphdb.read_links(LinkFeed(links, link_policy=FeedPolicy(FeedPolicy.CREATE),
                                                  node_policy=FeedPolicy(FeedPolicy.UPDATE)), self.transactMock)
        self.assertEquals(FeedResult.OK, result.status)
        self.assertEquals(0, result.inserted)
        self.assertEquals(links, result.missing)
        self.assertEquals(3, len(result.nodes.missing))
        
    def test_read_link_conflict(self):
        self.graphdb.read_links(LinkFeed([{'id':1, 'start': {'id': 1}, 'end': {'id': 3}}],
                                         link_policy=FeedPolicy(FeedPolicy.CREATE),
                                         node_policy=FeedPolicy(FeedPolicy.CREATE)), self.transactMock)
        
        links = [{'id': 1,
                  'start': {'id': 1, 'type': 'typeA'}, 
                  'end': {'id': 2, 'type': 'typeB'},
                  'type': 'KNOWS'},
                  {'id': 2,
                   'start': {'id': 2, 'type': 'typeC'},
                  'end': {'id': 3, 'type': 'typeC'},
                  'type': 'WORKSFOR'}]
        
        called = []
        
        def conflict(new, old):
            self.assertEquals(1, old.id)
            self.assertEquals(1, new['id'])
            called.append(True)
            return False
            
        result = self.graphdb.read_links(LinkFeed(links, link_policy=FeedPolicy(FeedPolicy.CREATE, conflict),
                                                  node_policy=FeedPolicy(FeedPolicy.CREATE_AND_UPDATE)), self.transactMock)
        self.assertEquals(FeedResult.OK, result.status)
        self.assertEquals(1, result.inserted)
        self.assertEquals(1, result.conflicted)
        self.assertEquals(1, len(called))
        
         
        


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.test_read_links_ok']
    unittest.main()
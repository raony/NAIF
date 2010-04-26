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
    
    def test_feed_update_ok(self):
        self.graphdb.read_links(LinkFeed([{'id':1, 'start': {'id': 1}, 'end': {'id': 3}}],
                                         link_policy=FeedPolicy(FeedPolicy.CREATE),
                                         node_policy=FeedPolicy(FeedPolicy.CREATE)), self.transactMock)
        result = self.graphdb.read_links(LinkFeed([{'id':1, 'start': {'id': 1}, 'end': {'id': 4}, 'novaprop': 'testando 123'}],
                                         link_policy=FeedPolicy(FeedPolicy.UPDATE, lambda x,y: True),
                                         node_policy=FeedPolicy(FeedPolicy.CREATE_AND_UPDATE)), self.transactMock)
        self.assertEquals(FeedResult.OK, result.status)
        self.assertEquals(1, result.updated)
        link = self.graphdb.link[1]
        self.assertEquals(4, link.end.id)
        self.assertEquals('testando 123', link['novaprop'])
    
    def test_feed_failure(self):
        result = self.graphdb.read_links(LinkFeed([{'start': {'id': 1}, 'end': {'id': 3}}],
                                         link_policy=FeedPolicy(FeedPolicy.CREATE),
                                         node_policy=FeedPolicy(FeedPolicy.CREATE)), self.transactMock)
        self.assertEquals(FeedResult.FAILURE, result.status)
        self.assertEquals(0, self.transactMock.success_called)
        self.assertEquals(1, self.transactMock.failure_called)
        self.assertEquals(1, self.transactMock.finish_called)
        
    def test_history(self):
        def compare_n(rx, ry):
            self.assertEquals(rx.inserted, ry.inserted)
            self.assertEquals(rx.updated, ry.updated)
            self.assertEquals(rx.conflicted, ry.conflicted)
            self.assertEquals(rx.datetime, ry.datetime)
            self.assertEquals(rx.status, ry.status)
            self.assertEquals(rx.missing, ry.missing)
        
        def compare(rx, ry):
            self.assertEquals(rx.inserted, ry.inserted)
            self.assertEquals(rx.updated, ry.updated)
            self.assertEquals(rx.conflicted, ry.conflicted)
            self.assertEquals(rx.datetime, ry.datetime)
            self.assertEquals(rx.status, ry.status)
            self.assertEquals(rx.missing, ry.missing)
            compare_n(rx.nodes, ry.nodes)
                
        r1 = self.graphdb.read_links(LinkFeed([{'start': {'id': 1}, 'end': {'id': 3}}],
                                         link_policy=FeedPolicy(FeedPolicy.CREATE),
                                         node_policy=FeedPolicy(FeedPolicy.CREATE)), self.transactMock)
        r2 = self.graphdb.read_links(LinkFeed([{'id': 1, 'start': {'id': 1}, 'end': {'id': 3}}],
                                         link_policy=FeedPolicy(FeedPolicy.CREATE),
                                         node_policy=FeedPolicy(FeedPolicy.CREATE)), self.transactMock)
        r3 = self.graphdb.read_links(LinkFeed([{'id': 1, 'start': {'id': 1}, 'end': {'id': 3}}],
                                         link_policy=FeedPolicy(FeedPolicy.CREATE),
                                         node_policy=FeedPolicy(FeedPolicy.CREATE_AND_UPDATE)), self.transactMock)
        r4 = self.graphdb.read_links(LinkFeed([{'id': 1, 'start': {'id': 4}, 'end': {'id': 3}}],
                                         link_policy=FeedPolicy(FeedPolicy.CREATE),
                                         node_policy=FeedPolicy(FeedPolicy.UPDATE)), self.transactMock)
        r5 = self.graphdb.read_links(LinkFeed([{'id': 1, 'start': {'id': 1}, 'end': {'id': 3}}],
                                         link_policy=FeedPolicy(FeedPolicy.UPDATE),
                                         node_policy=FeedPolicy(FeedPolicy.UPDATE)), self.transactMock)
        
        results = self.graphdb.feed_history(feed_id = 1)
        self.assertEquals(5, len(results))
        compare(r5, results[0])
        compare(r4, results[1])        
        compare(r3, results[2]) 
        compare(r2, results[3])        
        compare(r1, results[4]) 
    
    def test_link_missing(self):
        r = self.graphdb.read_links(LinkFeed([{'id': 1, 'start': {'id': 1}, 'end': {'id': 3}},
                                               {'id': 2, 'start': {'id': 2}, 'end': {'id': 3}}, ],
                                         link_policy=FeedPolicy(FeedPolicy.UPDATE),
                                         node_policy=FeedPolicy(FeedPolicy.CREATE)), self.transactMock)
        self.assertEquals(FeedResult.OK, r.status)
        self.assertEquals(2, len(r.missing))
        self.assertEquals(0, r.inserted)
        self.assertEquals(0, r.updated)
        self.assertEquals(0, r.conflicted)


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.test_read_links_ok']
    unittest.main()
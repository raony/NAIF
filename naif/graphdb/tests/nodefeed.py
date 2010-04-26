'''
Created on Apr 19, 2010

@author: raony
'''
import unittest
from datetime import datetime
from graphdb.graphdatabase import FeedResult, FeedPolicy
from base import FeedTest

class NodeFeedMock(object):
    def __init__(self, data, policy=FeedPolicy.CREATE, id=0):
        self.id = id
        self.data = data
        self.conf_comparisons = []
        self.node_policy = FeedPolicy(policy, self.conflict)
        
    def __getitem__(self, index):
        return self.data[index]
    def conflict(self, new, old):
        return self.conf_comparisons.pop(0)(new, old)

def read_node(args):
    result = {}
    attrs = ['id', 'type', 'attr1']
    for i, arg in enumerate(args):
        result[attrs[i]] = arg
    return result

class NodeFeedTest(FeedTest):
        
    def test_node_feed_read(self):
        raw = [[1, 'tipoum'], [2, 'tipodois'], [3, 'tipoum'], [18, 'tiposeis', 'testando']]
        nodeFeed = map(read_node, raw)
        self.assertTrue(self.graphdb.read_nodes(NodeFeedMock(nodeFeed), self.transactMock))
        self.assertEquals('tipoum', self.graphdb.node[1].type) 
        self.assertEquals('tipodois', self.graphdb.node[2].type)
        self.assertEquals('tipoum', self.graphdb.node[3].type)
        self.assertEquals('tiposeis', self.graphdb.node[18].type)
        self.assertEquals('testando', self.graphdb.node[18]['attr1'])
        
        self.assertEquals(1, self.transactMock.success_called)
        self.assertEquals(0, self.transactMock.failure_called)
        self.assertEquals(1, self.transactMock.finish_called)
    
    def test_node_feed_read_result(self):
        raw = [[1, 'tipoum'], [2, 'tipodois'], [3, 'tipoum'], [18, 'tiposeis', 'testando']]
        nodeFeed = map(read_node, raw)
        result = self.graphdb.read_nodes(NodeFeedMock(nodeFeed), self.transactMock)
        self.assertEquals(FeedResult.OK, result.status)
        self.assertTrue((datetime.today() - result.datetime).seconds < 1.0)
        self.assertEquals(4, result.inserted)
        self.assertEquals(4, result.total)
        self.assertEquals(1, self.transactMock.success_called)
        self.assertEquals(0, self.transactMock.failure_called)
        self.assertEquals(1, self.transactMock.finish_called)
    
    def test_node_feed_update_conflict(self):
        nf = NodeFeedMock([{'type': 'vermelho', 'id' : 1, 'age': 16}])
        called = []
        def conf_eq(n, o):
            self.assertEquals(n['id'], o.id)
            self.assertEquals(16, n['age'])
            self.assertEquals(15, o['age'])
            called.append(1)
            return False
        nf.conf_comparisons.append(conf_eq)
        
        self.graphdb.node(1, 'vermelho', age=15)
        result = self.graphdb.read_nodes(nf, self.transactMock)
        self.assertEquals(FeedResult.OK, result.status)
        self.assertEquals(0, result.inserted)
        self.assertEquals(1, result.conflicted)
        self.assertEquals(1, result.total)
        self.assertEquals(1, len(called))
        self.assertEquals(1, self.transactMock.success_called)
        self.assertEquals(0, self.transactMock.failure_called)
        self.assertEquals(1, self.transactMock.finish_called)
    
    def test_node_feed_update_ok(self):
        nf = NodeFeedMock([{'type': 'vermelho', 'id' : 1, 'age': 16, 'newp': 'ahoy'}])
        called = []
        def conf_eq(n, o):
            called.append(1)
            return True
        nf.conf_comparisons.append(conf_eq)
        
        self.graphdb.node(1, 'vermelho', age=15)
        result = self.graphdb.read_nodes(nf, self.transactMock)
        self.assertEquals(FeedResult.OK, result.status)
        self.assertEquals(0, result.inserted)
        self.assertEquals(1, result.updated)
        self.assertEquals(0, result.conflicted)
        self.assertEquals(1, result.total)
        self.assertEquals(1, len(called))
        self.assertEquals(16, self.graphdb.node[1]['age'])
        self.assertEquals('ahoy', self.graphdb.node[1]['newp'])
        self.assertEquals(1, self.transactMock.success_called)
        self.assertEquals(0, self.transactMock.failure_called)
        self.assertEquals(1, self.transactMock.finish_called)
        
    def test_node_feed_update_always(self):
        nf = NodeFeedMock([{'type': 'vermelho', 'id' : 1, 'age': 16, 'newp': 'ahoy'}], policy=FeedPolicy.UPDATE)
        
        self.graphdb.node(1, 'vermelho', age=15)
        result = self.graphdb.read_nodes(nf, self.transactMock)
        self.assertEquals(FeedResult.OK, result.status)
        self.assertEquals(0, result.inserted)
        self.assertEquals(1, result.updated)
        self.assertEquals(0, result.conflicted)
        self.assertEquals(1, result.total)
        self.assertEquals(16, self.graphdb.node[1]['age'])
        self.assertEquals('ahoy', self.graphdb.node[1]['newp'])
        self.assertEquals(1, self.transactMock.success_called)
        self.assertEquals(0, self.transactMock.failure_called)
        self.assertEquals(1, self.transactMock.finish_called)
        
    def test_node_feed_failure(self):
        nf = NodeFeedMock([{'type': 'vermelho', 'age': 16, 'newp': 'ahoy'}])
        self.graphdb.read_nodes(nf, self.transactMock)
        self.assertEquals(0, self.transactMock.success_called)
        self.assertEquals(1, self.transactMock.failure_called)
        self.assertEquals(1, self.transactMock.finish_called)
    
    def test_node_history(self):
        def compare(rx, ry):
            self.assertEquals(rx.inserted, ry.inserted)
            self.assertEquals(rx.updated, ry.updated)
            self.assertEquals(rx.conflicted, ry.conflicted)
            self.assertEquals(rx.datetime, ry.datetime)
        r1 = self.graphdb.read_nodes(NodeFeedMock(id=1, data=[{'id': 1, 'name': 'john'}]), self.transactMock)
        r2 = self.graphdb.read_nodes(NodeFeedMock(id=1, data=[{'name': 'john'}]), self.transactMock)
        nf = NodeFeedMock(id=1, data=[{'id': 1, 'age': 12}, {'id': 2, 'name': 'john'}], policy=FeedPolicy.CREATE_AND_UPDATE)
        r3 = self.graphdb.read_nodes(nf, self.transactMock)
        
        results = self.graphdb.feed_history(feed_id = 1)
        compare(r3, results[0])
        compare(r2, results[1])        
        compare(r1, results[2]) 
        self.assertEquals(3, len(results))
    
    def test_node_history_empty(self):
        self.assertTrue(not self.graphdb.feed_history(1))
    
    def test_node_missing(self):
        nf = NodeFeedMock(id=1, data=[{'id': 1, 'age': 12}, {'id': 2, 'name': 'john'}], policy=FeedPolicy.UPDATE)
        r = self.graphdb.read_nodes(nf, self.transactMock)
        self.assertEquals(FeedResult.OK, r.status)
        self.assertEquals(2, len(r.missing))
        self.assertEquals(0, r.inserted)
        self.assertEquals(0, r.updated)
        self.assertEquals(0, r.conflicted)

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
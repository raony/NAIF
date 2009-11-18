import unittest
from mock import Mock

from networkx.graph import Graph
from networkx.digraph import DiGraph

class LinkDict(dict):
    def __init__(self):
        self.links = {}
        
    def __getitem__(self, key):
        try:
            return super(LinkDict, self).__getitem__(key)
        except KeyError:
            return super(LinkDict, self).__getitem__(self.links[key])

    def __setitem__(self, key, value):
        self.links[key.name] = key
        return super(LinkDict, self).__setitem__(key, value)

class Link(object):
    def __init__(self, name, is_directed = False):
        self.is_directed = is_directed
        self.name = name

class Analysis(object):
    
    class ConfigError(StandardError):
        pass
    
    def __init__(self, db):
        self.linker = LinkDict()
        self.db = db
        self.db.fill_links(self.linker)
        if ('friend' not in self.linker.links) or (not self.linker['friend']): raise Analysis.ConfigError
        self.nodes = self.db.get_nodes()
        self.metric = {}
    
    def networks(self):
        result = LinkDict()
        for link in self.linker:
            result[link] = self.network(link)
        return result

    def network(self, link):
        link = self.linker[link]
        if link.is_directed:
            nw = DiGraph(name=link.name)
        else:
            nw = Graph(name=link.name)
        for node in self.nodes:
            nw.add_node(node)
            for neighbor in link.get_list(node): 
                nw.add_edge(node, neighbor)
        return nw
    
    def setup_metrics(self, metrics):
        for metric in metrics.list_metrics():
            self.metric[metric] = metrics
    
    def run_metric(self, metric, network):
        metrics = self.metric[metric]
        metrics.run_metrics(network)
        for node in network.nodes():
            node.metric[metric] = metrics.get_metric(metric, node)
        return network
            
class node():
    def __init__(self, id=-1):
        self.id = id
        self.metric = {}

    def __eq__(self, obj):
        return (type(self) == type(obj)) and (self.id == obj.id)
    
    def __cmp__(self, obj):
        return self.id.__cmp__(obj.id)
    
    def __hash__(self):
        return self.id.__hash__()
                    
class AnalysisTest(unittest.TestCase):
    def setUp(self):
        class filter:
            def __init__(self):
                self.is_directed = True
                self.name = 'friend'
                
            def get_list(self, thenode):
                self.matriz = [[node(1),node(3)],
                               [node(0),node(3)],[],
                               [node(0),node(1)]]
                return self.matriz[thenode.id]
        
        class amigos:
            def get_nodes(self):
                return self.lista
            def fill_links(self, linker):
                linker[Link('friend', True)] =filter()
        
        self.a1,self.a2,self.a3,self.a4 = (node(0), 
                                           node(1), 
                                           node(2), 
                                           node(3))
        
        self.amigos_mock = amigos()
        self.amigos_mock.lista = (self.a1, 
                                  self.a2,
                                  self.a3,
                                  self.a4)
        
    def test_network(self):
        
        target = Analysis(self.amigos_mock)
        networks = target.networks()
        
        self.failUnlessEqual(1, len(networks))
        
        self.failUnlessEqual([node(1), node(3)], networks['friend'].neighbors(self.a1))
        self.failUnlessEqual([node(0), node(3)], networks['friend'].neighbors(self.a2))
        self.failUnlessEqual([], networks['friend'].neighbors(self.a3))
        self.failUnlessEqual([node(0), node(1)], networks['friend'].neighbors(self.a4))
    
    def test_metrics_setup(self):
        target = Analysis(self.amigos_mock)
        
        class m(object):
            def list_metrics(self):
                return ['a', 'b',]
            def run_metrics(self, network):
                pass
            def get_metric(self, metric, node):
                return node.id*2
        
        target.setup_metrics(m())
        metrified_network = target.run_metric('a', target.network('friend'))
        for node in metrified_network.nodes():
            self.failUnlessEqual(node.id*2, node.metric['a'])
            
    def test_friend_fill(self):
        class amigos:
            def get_nodes(self):
                return []
            def fill_links(self, linker):
                return None
    
        try:
            target = Analysis(amigos())
            self.fail('should rise an analysis config error')
        except Analysis.ConfigError:
            pass
    
def suite():
    return unittest.TestLoader().loadTestsFromTestCase(AnalysisTest)

if __name__ == '__main__':
    unittest.main()
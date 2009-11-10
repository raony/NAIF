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
        self.db.fillLinks(self.linker)
        if ('friend' not in self.linker.links) or (not self.linker['friend']): raise Analysis.ConfigError
        self.nodes = self.db.getNodes()
    
    def networks(self):
        result = LinkDict()
        for link in self.linker:
            if link.is_directed:
                nw = DiGraph()
            else:
                nw = Graph()

            for node in self.nodes:
                nw.add_node(node.id)
                for neighbor in self.linker[link].get_list(node.id): 
                    nw.add_edge(node.id, neighbor)
                
            result[link] = nw
        return result
            
        
class AnalysisTest(unittest.TestCase):
    def test_network(self):
        class node:
            pass

        class filter:
            def get_list(self, id):
                self.matriz = [[1,3],[0,3],[],[0,1]]
                return self.matriz[id]
        
        class amigos:
            def getNodes(self):
                return self.lista
            def fillLinks(self, linker):
                linker[Link('friend', True)] =filter()
        
        a1,a2,a3,a4 = (node(), node(), node(), node())
        a1.id, a2.id, a3.id, a4.id = (0,1,2,3)
        amigos_mock = amigos()
        amigos_mock.lista = (a1,a2,a3,a4)
        target = Analysis(amigos_mock)
        networks = target.networks()
        
        self.failUnlessEqual(1, len(networks))
        
        self.failUnlessEqual([1,3], networks['friend'].neighbors(0))
        self.failUnlessEqual([0,3], networks['friend'].neighbors(1))
        self.failUnlessEqual([], networks['friend'].neighbors(2))
        self.failUnlessEqual([0,1], networks['friend'].neighbors(3))
    
    def test_friend_fill(self):
        class amigos:
            def getNodes(self):
                return []
            def fillLinks(self, linker):
                return None
    
        try:
            target = Analysis(amigos())
            self.fail('should rise an analysis config error')
        except Analysis.ConfigError:
            pass
    
        
if __name__ == '__main__':
    unittest.main()
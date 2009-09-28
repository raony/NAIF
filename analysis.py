import unittest
from mock import Mock

class network(list):
    pass

class Analysis(object):
    def __init__(self, db):
        self.linker = {'friend': None}
        self.db = db
        self.db.fillLinks(self.linker)
        self.nodes = self.db.getNodes()
    
    def create_network(self, name, n):
        n = [[0 for i in range(n)] for j in range(n)]
        n.name = name
        return n
    
    def networks(self):
        result = []
        for link in self.linker:
            nw = network()
            nw.name = link
            for node in self.nodes:
                list = self.linker[link].get_list(node.id)
                nw.append([i.id in list for i in self.nodes])
            result.append(nw)
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
                linker['friend'] = filter()
        
        a1,a2,a3,a4 = (node(), node(), node(), node())
        a1.id, a2.id, a3.id, a4.id = (0,1,2,3)
        amigos_mock = amigos()
        amigos_mock.lista = (a1,a2,a3,a4)
        target = Analysis(amigos_mock)
        networks = target.networks()
        self.failUnlessEqual(1, len(networks))
        self.failUnlessEqual('friend', networks[0].name)
        self.failUnlessEqual([0,1,0,1], networks[0][0])
        self.failUnlessEqual([1,0,0,1], networks[0][1])
        self.failUnlessEqual([0,0,0,0], networks[0][2])
        self.failUnlessEqual([1,1,0,0], networks[0][3])
        
        
if __name__ == '__main__':
    unittest.main()
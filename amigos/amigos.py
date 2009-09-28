import unittest
import pg
from mock import Mock, ReturnValues
from friendlink import FriendLink

class Amigos(object):
    def __init__(self, conn):
        self.db_conn = conn
        self.links = [FriendLink(conn)]
        
    def getNodes(self):
        class Node:
            pass

        result = []      
        for id in [r[0] for r in self.db_conn.query('select distinct cd_user from user_resc').getresult()]:
            a = Node()
            a.id = id
            result.append(a)
        return result
    
    def fillLinks(self, linker):
        linker['friend'] = self.links[0]

class NodeCreatorTest(unittest.TestCase):
    def test_select(self):
        querymock = Mock({'getresult': ((1,),(4,),(5,))})
        pgmock = Mock({'query': querymock})
        target = Amigos(pgmock)
        nodes = target.getNodes()
        self.failUnlessEqual(3, len(nodes))
        self.failUnlessEqual(1, nodes[0].id)
        self.failUnlessEqual(4, nodes[1].id)
        self.failUnlessEqual(5, nodes[2].id)
        pgmock.mockCheckCall(0, 'query', 'select distinct cd_user from user_resc')
        querymock.mockCheckCall(0, 'getresult')
    
    def test_filter(self):
        linker = {'friend': None}
        target = Amigos(None)
        target.fillLinks(linker)
        self.failUnlessEqual(FriendLink, type(linker['friend']))
        
if __name__ == "__main__":
    import unittest
    unittest.main()
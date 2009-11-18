import unittest
from mock import Mock, ReturnValues
from basic import db, link

class FriendLink(link):
    def __init__(self, db_conn):
        self.db_conn = db_conn
    
    def _get_links(self, id):
        return [link[0] for link in self.db_conn.query('select cg.cd_user as contact from user_contc_group ucg join contact_group cg on cg.cd_contc_group=ucg.cd_contc_group where ucg.cd_user=%d'%id).getresult()]

class Amigos(db):
    def __init__(self, conn):
        self.db_conn = conn
        self.links = [FriendLink(conn)]
        
    def _get_nodes(self):
        class Node:
            pass

        result = []      
        for id in [r[0] for r in self.db_conn.query('select distinct cd_user from user_resc').getresult()]:
            a = Node()
            a.id = id
            result.append(a)
        return result
    
    def _fill_links(self, linker):
        linker['friend'] = self.links[0]
        

class FriendLinkTest(unittest.TestCase):
    def test_link(self):
        class node:
            pass
        
        a1 = node()
        a1.id = 1
        a2 = node()
        a2.id = 2
        a3 = node()
        a3.id = 3
        
        querymock = Mock({'getresult': ((3,),)})
        pgmock = Mock({'query': querymock})
        
        target = FriendLink(pgmock)
        links = target.get_links(a1.id)
        self.failUnlessEqual(1, len(links))
        self.failUnless(a3.id in links)
        self.failUnless(not a2.id in links)

        pgmock.mockCheckCall(0, 'query', 'select cg.cd_user as contact from user_contc_group ucg join contact_group cg on cg.cd_contc_group=ucg.cd_contc_group where ucg.cd_user=1')
        querymock.mockCheckCall(0, 'getresult')

if __name__ == '__main__':
    import unittest
    unittest.main()

class NodeCreatorTest(unittest.TestCase):
    def test_select(self):
        querymock = Mock({'getresult': ((1,),(4,),(5,))})
        pgmock = Mock({'query': querymock})
        target = Amigos(pgmock)
        nodes = target.get_nodes()
        self.failUnlessEqual(3, len(nodes))
        self.failUnlessEqual(1, nodes[0].id)
        self.failUnlessEqual(4, nodes[1].id)
        self.failUnlessEqual(5, nodes[2].id)
        pgmock.mockCheckCall(0, 'query', 'select distinct cd_user from user_resc')
        querymock.mockCheckCall(0, 'getresult')
    
    def test_filter(self):
        linker = {'friend': None}
        target = Amigos(None)
        target.fill_links(linker)
        self.failUnlessEqual(FriendLink, type(linker['friend']))
        

def suite():
    suite1 = unittest.TestLoader().loadTestsFromTestCase(FriendLinkTest)
    suite2 = unittest.TestLoader().loadTestsFromTestCase(NodeCreatorTest)
    return unittest.TestSuite([suite1, suite2])


if __name__ == "__main__":
    import unittest
    unittest.main()
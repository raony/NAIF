import unittest
from mock import Mock

class FriendLink(object):
    def __init__(self, db_conn):
        self.db_conn = db_conn
    
    def get_links(self, id):
        return [link[0] for link in self.db_conn.query('select cg.cd_user as contact from user_contc_group ucg join contact_group cg on cg.cd_contc_group=ucg.cd_contc_group where ucg.cd_user=%d'%id).getresult()]

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
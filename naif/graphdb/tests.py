#'''
#Created on Apr 15, 2010
#
#@author: raony
#'''
#from graphdb.graphdatabase import GraphDatabase, Node, Link, LinkType
#import unittest
#
#class GraphDatabaseTest(unittest.TestCase):
#
#    def setUp(self):
#        self.graphdb = GraphDatabase('neo_test/', True)
#        self.transaction= self.graphdb.transaction()
#
#    def tearDown(self):
#        self.transaction.failure()
#        self.transaction.finish()
#        self.graphdb.shutdown()
#
#            
#    
##    def test_pickle(self):
##        n = self.graphdb.node(1)
##        a = [1, 2, 3]
##        s1 = pickle.dumps(a)
##        n['e'] = s1
##        s2 = n['e']    
##        
##        pickle.loads(s1)
##        pickle.loads(str(s2))
##        
##        self.assertEquals(s1, s2)
#            
#if __name__ == "__main__":
##    import sys;sys.argv = ['', 'GraphDatabaseTest.test_pickle']
#    unittest.main()
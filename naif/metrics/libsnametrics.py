import unittest

from networkx.graph import Graph
from libsna.libSNA import SocialNetwork
from basic import Metrics 

class libSNAMetrics(Metrics):
    def __init__(self):
        self._metrics = {'closeness' : None, 'betweenness' : None, 'totalDegree' : None,}
        
    def _list_metrics(self):
        return self._metrics.keys()
    
    def _run_metrics(self, network):
        sn = SocialNetwork()
        sn.graph = network
        sn.calculateMeasures()
        self._metrics['closeness'] = sn.closenessDict
        self._metrics['betweenness'] = sn.betweennessDict
        self._metrics['totalDegree'] = sn.totalDegreeDict
    
    def _get_metric(self, metric, node):
        return self._metrics[metric][node]
        

class libSNAMetricsTest(unittest.TestCase):
    def setUp(self):
        data = {
                'andre': ['carol', 'diane', 'fernando', 'beverly'],
                'beverly': ['diane', 'garth', 'ed'],
                'carol': ['diane', 'fernando'],
                'diane': ['fernando', 'garth', 'ed'],
                'ed': ['garth'],
                'fernando': ['heather', 'garth'],
                'garth': ['heather'],
                'heather': ['ike'],
                'ike': ['jane'],
                }
        self.graph = Graph(data, 'test-graph')

    def test_initialize(self):
        target = libSNAMetrics()
        target.run_metrics(self.graph)
        self.failUnlessAlmostEqual(0.643, target.get_metric('closeness', 'fernando'), 3) 
        self.failUnlessAlmostEqual(0.231, target.get_metric('betweenness', 'fernando'), 3) 
        self.failUnlessAlmostEqual(0.556, target.get_metric('totalDegree', 'fernando'), 3)

def suite():
    return unittest.TestLoader().loadTestsFromTestCase(libSNAMetricsTest) 
        
if __name__ == '__main__':
    unittest.main()
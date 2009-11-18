import unittest

from networkx.graph import Graph
from libsna.libSNA import SocialNetwork


class libSNAMetrics(object):
    def __init__(self):
        self._metrics = {'closeness' : None, 'betweenness' : None, 'totalDegree' : None,}
        
    def listMetrics(self):
        return self._metrics.keys()
    
    def runMetrics(self, network):
        sn = SocialNetwork()
        sn.graph = network
        sn.calculateMeasures()
        self._metrics['closeness'] = sn.closenessDict
        self._metrics['betweenness'] = sn.betweennessDict
        self._metrics['totalDegree'] = sn.totalDegreeDict
    
    def getMetric(self, metric, node):
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
        target.runMetrics(self.graph)
        self.failUnlessAlmostEqual(0.643, target.getMetric('closeness', 'fernando'), 3) 
        self.failUnlessAlmostEqual(0.231, target.getMetric('betweenness', 'fernando'), 3) 
        self.failUnlessAlmostEqual(0.556, target.getMetric('totalDegree', 'fernando'), 3) 
        
if __name__ == '__main__':
    unittest.main()
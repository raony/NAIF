import unittest

class libSNAMetricsTest(unittest.TestCase):
    def setUp(self):
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
        self.analysis = Analysis(amigos_mock)

    def test_initialize(self):
        target = libSNAMetrics()
        target.runMetrics(self.analysis.networks[0])
        
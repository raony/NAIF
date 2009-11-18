class Metrics(object):
    def list_metrics(self):
        return self._list_metrics()
    
    def run_metrics(self, network):
        return self._run_metrics(network)
    
    def get_metric(self, metric, node):
        return self._get_metric(metric, node)
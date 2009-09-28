"""
 The idea is, for each node list the neighboors and count its 
 centrality degree.
 
 >>> n1, n2, n3, n4 = (Node(), Node(), Node(), Node())
 >>> node = Node([n1, n2, n4])
 >>> n1 in node.link_out
 True
 >>> n2 in node.link_out
 True
 >>> n3 in node.link_out
 False
 >>> n4 in node.link_out
 True
 >>> node.metrics.append(out_degree_metric)
 >>> node.metrics.out_degree_metric
 3
 >>> node.metrics.append(in_degree_metric)
 >>> node.metrics.in_degree_metric
 0
 >>> n1.metrics.append(in_degree_metric)
 >>> n1.metrics.in_degree_metric
 1

 each node is managed by an object that guarantee node consistency
 so only one node can exist at time with the same id.

 >>> NodeManager.nodes.clear()
 >>> n1, n2 = (Node(), Node())
 >>> n1.id
 1
 >>> n2.id
 2
 >>> NodeManager.nodes[1] == n1
 True
 >>> NodeManager.nodes[2] == n2
 True
 >>> n3 = Node(id=2)
 >>> n3 == n2
 True
"""

class metric(object):
    def __init__(self):
        self.value = None
    def calculate(self, node):
        pass

class out_degree_metric(metric):
    def calculate(self, node):
        self.value = len(node.link_out)

class in_degree_metric(metric):
    def calculate(self, node):
        self.value = len(node.link_in)

class NodeManager(object):
    nodes = {}
    @classmethod
    def get_or_create(cls, id=None):
        instance = NodeManager.nodes.get(id, None)
        if not instance:
            if id:
                new_id = id
            elif NodeManager.nodes.keys():
                new_id = max(NodeManager.nodes.keys()) + 1
            else:
                new_id = 1
            instance = super(type, Node).__new__(Node)
            instance.id = new_id
            NodeManager.nodes[new_id] = instance
        return instance

class Node(object):
    class Metrics(object):
        def __init__(self, node):
            self.list = {}
            self.node = node
            
        def append(self, metric):
            self.list[metric.__name__] = metric()
            self.list[metric.__name__].calculate(self.node)            
            
        def __getattribute__(self, attr):
            if attr in object.__getattribute__(self, 'list'):
                return object.__getattribute__(self,'list')[attr].value
            else:
                return object.__getattribute__(self,attr)
            
        def __iter__(self):
            return iter(self.list)
    
    def __new__(self, links=[], connection=None, id=None):
        return NodeManager.get_or_create(id)        
    
    def __init__(self, links=[], connection=None, id=None):
        self.metrics = Node.Metrics(self)
        self._link_in = None
        self._link_out = []
        self.conn = connection
        
        if links or not connection:
            self._link_in = []
            for link in links:
                self.link_out.append(link)
                link.link_in.append(self)
    
    @property
    def link_in(self):
        if self._link_in == None:
            if self.conn:
                self._link_in = [t[0] for t in self.conn.query('select ucg.cd_user as contact from user_contc_group ucg join contact_group cg on cg.cd_contc_group=ucg.cd_contc_group where cg.cd_user=%d'%self.id).getresult()]
            else:
                self._link_in = []
        return self._link_in
    
    @property
    def link_out(self):
        if self._link_out == None:
            if self.conn:
                self._link_out = [t[0] for t in self.conn.query('select cg.cd_user as contact from user_contc_group ucg join contact_group cg on cg.cd_contc_group=ucg.cd_contc_group where ucg.cd_user=%d'%self.id).getresult()]
            else:
                self._link_out = []
        return self._link_out

if __name__ == "__main__":
    import doctest
    doctest.testmod()

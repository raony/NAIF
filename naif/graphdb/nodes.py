'''
Created on Apr 26, 2010

@author: raony
'''
from base import GraphTypeManager

class NodeTypeManager(GraphTypeManager):
    PROPERTY_NAME = 'node_type'
    def __call__(self, type):
        for node in self.__graphdb__._traverse_node_by_type([type]):
            yield Node(node)

class NodeManager(object):
    def __init__(self, graphdb):
        self.graphdb = graphdb
        self._index = self.graphdb._index('node index')
        self.__type_manager__ = None
        
    @property
    def type(self):
        if not self.__type_manager__:
            self.__type_manager__ = NodeTypeManager(self.graphdb)
        return self.__type_manager__
    
    def __getitem__(self, index):
        result = self._index[str(index)]
        if result:
            result = Node(result)
        return result
    
    def __iter__(self):
        all_types = [type for type in self.type]
        for node in self.graphdb._traverse_node_by_type(all_types):
            yield Node(node)
            
    def __contains__(self, key):
        return self._index[str(key)] != None
    
    def __call__(self, id, type='node', **kwargs):
        if self[id]:
            raise Node.AlreadyExist
        node = self.graphdb._new_node(id, type, **kwargs)
        self._index[str(id)] = node
        self.type.add(type)
        return Node(node)


class NodeType(object):
    def __init__(self, name):
        self.name = name
    
    def __str__(self):
        return self.name
    
    def __eq__(self, obj):
        return (type(obj) == type(self.name) and self.name == obj) or (type(obj) == type(self) and self.name == obj.name)
    
    __unicode__ = __str__
    
class Node(object):
    class AlreadyExist(Exception):
        pass
    def __init__(self, neo_node):
        self.__node__ = neo_node
    
    @property
    def id(self):
        return self.__node__['net_id']
    
    @property
    def type(self):
        return self.__node__['type']
    
    def __eq__(self, obj):
        return type(self) == type(obj) and self.__node__ == obj.__node__
    
    def __getitem__(self, index):
        return self.__node__[index]
    
    def __delitem__(self, index):
        if index == 'type':
            raise KeyError("'type' attribute cannot be deleted.")
        del self.__node__[index]
    
    def __setitem__(self, index, value):
        if index == 'type':
            raise KeyError("'type' attribute is read-only.")
        self.__node__[index] = value
    
    def __contains__(self, key):
        return key in self.__node__
    
    def update(self, *args, **kwargs):
        return self.__node__.update(*args, **kwargs)

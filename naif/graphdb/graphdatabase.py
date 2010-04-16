# -*- coding: utf-8 -*-
"""
This is a wrapper module over neo4j



"""
import neo4j

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
    
    def __setitem__(self, index, value):
        if index == 'type':
            raise KeyError("'type' attribute is read-only.")
        self.__node__[index] = value
    
    def __contains__(self, key):
        return key in self.__node__
    
class GraphDatabase(object):
    def __init__(self, path='neo/', verbose=False):
        if verbose:
            import logging
            logger = logging.getLogger("neo")
            logger.setLevel(logging.DEBUG)
            logger.addHandler(logging.StreamHandler())
        else:
            logger = None
            
        self.__neo__ = neo4j.GraphDatabase(path, log=logger)
        self.__node_index__ = self.__neo__.index('node index', create=True)
    
    def __node_types__(self):
        types = self.__neo__.ref.get('node_types', '')
        return types and types.split(',') or []
    
    def __nodes_by_type__(self, type):
        class NodeByType(neo4j.Traversal):
            types = [ neo4j.Outgoing.__getattr__(type.upper()) ] #@UndefinedVariable
            order = neo4j.DEPTH_FIRST #@UndefinedVariable
            stop = neo4j.STOP_AT_END_OF_GRAPH #@UndefinedVariable
            returnable = neo4j.RETURN_ALL_BUT_START_NODE #@UndefinedVariable
        
            def __iter__(self):
                class MyIter(object):
                    def __init__(self, iter):
                        self.iter = iter
                    def next(self):
                        return Node(self.iter.next())
                return MyIter(super(NodeByType, self).__iter__())
                
        return NodeByType(self.__neo__.ref)
    
    
    def add_node_type(self, type):
        types = self.__node_types__()
        if type not in types:
            types.append(type)
        self.__neo__.ref['node_types'] = ','.join(types)        
    
    @property
    def node(self):
        gdb = self
        class nodelist(object):
            @property
            def type(self):
                class nodetype(object):
                    def __init__(self):
                        self.types = gdb.__node_types__()
                    def __getitem__(self, index):
                        return self.types[index]
                    def __contains__(self, obj):
                        return obj in self.types
                    def __len__(self):
                        return len(self.types)
                    def __call__(self, type):
                        return gdb.__nodes_by_type__(type)
                return nodetype()
            
            def __getitem__(self, index):
                return Node(gdb.__node_index__[str(index)])
            def __contains__(self, key):
                return gdb.__node_index__[str(key)] != None
            def __call__(self, id, type='node', **kwargs):
                if gdb.__node_index__[str(id)]:
                    raise Node.AlreadyExist
                node = gdb.__neo__.node(net_id=id, type=type, **kwargs)
                
                gdb.add_node_type(type)
                
                gdb.__neo__.ref.__getattr__(type.upper())(node)
                gdb.__node_index__[str(id)] = node
                return Node(node)
        return nodelist()

    def shutdown(self):
        self.__neo__.shutdown()
    
    @property
    def transaction(self):
        return self.__neo__.transaction
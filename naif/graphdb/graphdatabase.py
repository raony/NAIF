# -*- coding: utf-8 -*-
"""
This is a wrapper module over neo4j



"""
import neo4j

class Node(object):
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
    
    @property
    def node(self):
        gdb = self
        class nodelist(object):
            def __getitem__(self, index):
                return Node(gdb.__node_index__[str(index)])
        return nodelist()

    def new_node(self, id):
        node = self.__neo__.node(net_id=id, type='node')
        self.__neo__.ref.NODE(node)
        self.__node_index__[str(id)] = node
        return Node(node)

    def shutdown(self):
        self.__neo__.shutdown()
    
    @property
    def transaction(self):
        return self.__neo__.transaction
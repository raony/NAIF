'''
Created on Apr 26, 2010

@author: raony
'''
import pickle

class GraphTypeManager(object):
    def __init__(self, graphdb):
        str_types = graphdb._get_global_property(self.PROPERTY_NAME)
        self.__types__ = str_types and pickle.loads(str_types) or []
        self.__graphdb__ = graphdb

    def __contains__(self, type):
        return type in self.__types__
    
    def __iter__(self):
        return iter(self.__types__)
    
    def __len__(self):
        return len(self.__types__)
    
    def _save(self):
        self.__graphdb__._add_global_property(self.PROPERTY_NAME, pickle.dumps(self.__types__))
        
    def add(self, type):
        if type not in self:
            self.__types__.append(type) 
            self._save()


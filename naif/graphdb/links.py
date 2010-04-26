'''
Created on Apr 26, 2010

@author: raony
'''
from base import GraphTypeManager
from nodes import Node

class LinkType(object):
    BINARY = 1
    VALUED = 0
    MIXED = -1
    
    def __eq__(self, obj):
        return (type(obj) == type(self.name) and self.name == obj) or (type(obj) == type(self) and self.name == obj.name)
    
    def __init__(self, name, binary):
        if binary not in [1,0,-1]:
            raise ValueError("Invalid binary value.")
        self.name = name
        self.binary = binary
    
    def __str__(self):
        bstr = ''
        if self.binary == 0:
            bstr = 'V'
        elif self.binary == 1:
            bstr = 'B'
        else:
            bstr = 'M'
        return '{0} - {1}'.format(self.name, bstr)
    
    __unicode__ = __str__

class Link(object):
    class AlreadyExist(Exception):
        pass
    
    def __init__(self, neo_link):
        self.__link__ = neo_link
    
    def __str__(self):
        return '< Link id = {0} >'.format(self.id)

    @property
    def start(self):
        return Node(self.__link__.start)
    
    @property
    def end(self):
        return Node(self.__link__.end)
    
    @property
    def id(self):
        return int(self.__link__['net_id'].toString())
    
    @property
    def type(self):
        return self.__link__.type
    
    @property
    def binary(self):
        return self.__link__['binary']

    @property
    def strength(self):
        return self.__link__['strength']
    
    def _delete(self):
        return self.__link__.delete()
    
    def __eq__(self, obj):
        return type(obj) == type(self) and obj.__link__ == self.__link__

    def __getitem__(self, index):
        return self.__link__[index]
    
    def __delitem__(self, key):
        if key in ['binary', 'strength']:
            raise KeyError('{0} cannot be deleted.'.format(key))
        del self.__link__[key]
    
    def __setitem__(self, key, value):
        if key in ['binary', 'strength']:
            raise KeyError('{0} is a read-only attribute.'.format(key))
        self.__link__[key] = value
    
    def __contains__(self, key):
        return key in self.__link__


class LinkTypeManager(GraphTypeManager):
    PROPERTY_NAME = 'link_type'
    def __call__(self, type):
        raise NotImplementedError
    
    def add(self, type):
        found = False
        for saved_type in self:
            if saved_type == type:
                found = True
                if saved_type.binary != type.binary:
                    saved_type.binary = LinkType.MIXED
                break
        if not found:
            self.__types__.append(type)
        self._save()

class LinkManager(object):
    
        def __init__(self, graphdb):
            self._graphdb = graphdb
            self._index = graphdb._raw_index('list index')
        
        @property
        def type(self):
            return LinkTypeManager(self._graphdb)
        
        def __delitem__(self, key):
            self[key]._delete()
            del self._index[key]
        
        def __getitem__(self, key):
            neo_id = self._index[key]
            return neo_id and Link(self._graphdb._relationship[neo_id]) or None
        
        def __iter__(self):
            for key in self._index:
                yield Link(self._graphdb._relationship[self._index[key]])

        def __contains__(self, key):
            return key in self._index
        
        def __call__(self, id, start, end, type='LINK', strength=None, **kwargs):
            if kwargs.get('binary', None) and strength:
                raise ValueError('Binary links cannot have strength.')
            if id in self:
                raise Link.AlreadyExist
            if strength == None:
                binary = True
                strength = 1.0
            else:
                binary = False
            self.type.add(LinkType(type, binary and LinkType.BINARY or LinkType.VALUED))
            link = start.__node__.__getattr__(type)(end.__node__, net_id=id, binary=binary, strength=strength, **kwargs)
            self._index[id] = link.id
            return Link(link)
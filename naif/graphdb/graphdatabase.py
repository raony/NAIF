# -*- coding: utf-8 -*-
"""
This is a wrapper module over neo4j



"""
import neo4j
import pickle
from datetime import datetime

class NodeType(object):
    def __init__(self, name):
        self.name = name
    
    def __str__(self):
        return self.name
    
    def __eq__(self, obj):
        return (type(obj) == type(self.name) and self.name == obj) or (type(obj) == type(self) and self.name == obj.name)
    
    __unicode__ = __str__
    
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

    @property
    def start(self):
        return Node(self.__link__.start)
    
    @property
    def end(self):
        return Node(self.__link__.end)
    
    @property
    def id(self):
        return self.__link__['net_id']
    
    @property
    def type(self):
        return self.__link__.type
    
    @property
    def binary(self):
        return self.__link__['binary']

    @property
    def strength(self):
        return self.__link__['strength']
    
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
    
class FeedResult(object):
    OK = 1
    FAILURE = 0
    
    def __init__(self, datetime):
        self.status = self.FAILURE
        self.datetime = datetime
        self.inserted = 0
        self.conflicted = 0
        self.updated = 0
    
    @property
    def total(self):
        return self.inserted + self.conflicted + self.updated

class FeedHistory(object):
            
    def __init__(self, neo):
        self.__neo__ = neo
        if self.__neo__.ref.FEEDHISTORY.single:
            self.__history_head__ = self.__neo__.ref.FEEDHISTORY.single.end 
        else:
            self.__history_head__ = self.__neo__.node()
            self.__neo__.ref.FEEDHISTORY(self.__history_head__)
            
    def __node__(self, result):
        return self.__neo__.node(datetime=result.datetime.strftime('%Y%m%d%H%M%S%f'),
                                 status=result.status, 
                                 inserted=result.inserted,
                                 updated = result.updated,
                                 conflicted = result.conflicted)
    
    def __result__(self, node):
        result = FeedResult(datetime.strptime(node['datetime'], '%Y%m%d%H%M%S%f'))
        result.status = int(node['status'].toString())
        result.inserted = int(node['inserted'].toString())
        result.conflicted = int(node['conflicted'].toString())
        result.updated = int(node['updated'].toString())
        return result
    
    def __getitem__(self, id):
        fh = self
        class FeedLine(neo4j.Traversal):
            types = [ neo4j.Outgoing.__getattr__(str(id)) ] #@UndefinedVariable
            returnable = neo4j.RETURN_ALL_BUT_START_NODE #@UndefinedVariable

            def __init__(self, startnode):
                super(FeedLine, self).__init__(startnode)
                self.__data__ = []
                self.__next__ = self.__iter__().next
                
            def __getitem__(self, key):
                try:
                    return self.__data__[key]
                except IndexError:
                    pass
                try:
                    for i in range(key - len(self.__data__) + 1): #@UnusedVariable
                        self.__data__.append(self.next())
                    return self.__data__[-1]
                except StopIteration:
                    raise IndexError
            
            def __len__(self):
                end = len(self.__data__)
                try:
                    while (end):
                        self.__getitem__(end)
                        end += 1
                except IndexError:
                    pass
                return len(self.__data__)
                    

            def next(self):
                return fh.__result__(self.__next__())
            
            def append(self, result):
                new = fh.__node__(result)
#                import pdb
#                pdb.set_trace()
                rel = fh.__history_head__.__getattr__(str(id)).single
                tmp = None
                if rel != None:
                    tmp = rel.end
                    rel.delete()
                fh.__history_head__.__getattr__(str(id))(new)
                if tmp:
                    new.__getattr__(str(id))(tmp)

        return FeedLine(self.__history_head__)
        
        
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
        self.__neopath__ = path
        self.__node_index__ = self.__neo__.index('node index', create=True)
        self.__history__ = None 
    
    def __types__(self, type_attr):
        types = self.__neo__.ref.get(type_attr, '')
        return types and pickle.loads(str(types)) or []
    
    def __node_types__(self):
        return self.__types__('node_types')
    
    def __link_types__(self):
        return self.__types__('link_types')
    
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
        type = NodeType(type)
        types = self.__types__('node_types')
        if type not in types:
            types.append(type)
        self.__neo__.ref['node_types'] = pickle.dumps(types)
    
    def add_link_type(self, type, binary):
        type = LinkType(type, binary and LinkType.BINARY or LinkType.VALUED)
        types = self.__types__('link_types')
        if type in types and types[types.index(type)].binary != binary:
            types[types.index(type)].binary = LinkType.MIXED
        else:
            types.append(type)
        self.__neo__.ref['link_types'] = pickle.dumps(types)
        
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
                result = gdb.__node_index__[str(index)]
                if result:
                    result = Node(result)
                return result
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
    
    @property
    def link(self):
        gdb = self
        class linklist(object):
            @property
            def type(self):
                class nodetype(object):
                    def __init__(self):
                        self.types = gdb.__link_types__()
                    def __getitem__(self, index):
                        return self.types[index]
                    def __contains__(self, obj):
                        return obj in self.types
                    def __len__(self):
                        return len(self.types)
#                    def __call__(self, type):
#                        return gdb.__nodes_by_type__(type)
                return nodetype()
            
            def __getitem__(self, key):
                result = None
                try:
                    for rel in gdb.__neo__.relationship:
                        if rel.get('net_id', None) == key:
                            result = Link(rel)
                            break
                except Exception, e:
                    if 'No transaction found' in str(e):
                        raise e
                return result
            def __contains__(self, key):
                return self[key] != None
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
                gdb.add_link_type(type, binary)
                link = start.__node__.__getattr__(type)(end.__node__, net_id=id, binary=binary, strength=strength, **kwargs)
                return Link(link)
        return linklist()

    def shutdown(self):
        self.__neo__.shutdown()
        
    def read_nodes(self, nodeFeed, transaction):
        
        result = FeedResult(datetime.today())
        
        try:
            for node in nodeFeed:
                if node['id'] in self.node:
                    if nodeFeed.always_update or nodeFeed.conflict(node, self.node[node['id']]):
                        node['net_id'] = node['id']
                        del node['id']
                        self.node[node['net_id']].update(node)        
                        result.updated += 1
                    else:
                        result.conflicted += 1
                else:
                    result.inserted += 1
                    self.node(**node)
        except:
            result.status = FeedResult.FAILURE
            transaction.failure()
        else:
            result.status = FeedResult.OK
            transaction.success()
        finally:
            self.add_to_history(nodeFeed.id, result)
            transaction.finish()
        return result    
    
    def add_to_history(self, feed_id, result):
#        if feed_id not in self.__history__:
#            self.__history__[feed_id] = [] 
        self.history[feed_id].append(result)
    
    def feed_history(self, feed_id):
        return self.history[feed_id]
    
    @property
    def history(self):
        if not self.__history__:
            self.__history__ = FeedHistory(self.__neo__)
        return self.__history__
    
    @property
    def transaction(self):
        return self.__neo__.transaction
#        if self.__transaction__:
#            return self.__transaction__
#        
#        gdb = self
#        
#        class MyTransaction(object):
#            class TransactionClosed(Exception):
#                pass
#            def __init__(self):
#                self.__tx__ = None
#                self.closed = False
#                self.failure = False
#                
#            def begin(self):
#                if self.closed:
#                    raise self.TransactionClosed
#                if not self.__tx__:
#                    self.__tx__ = gdb.__neo__.transaction()
#                    gdb.__begin_transaction__()
#            def failure(self):
#                self.__tx__.failure()
#                gdb.__end_transaction__(False)
#            def success(self):
#                self.__tx__.success()
#                gdb.__end_transaction__(True)
#            def finish(self):
#                self.__tx__.finish()
#                gdb.__transaction__ = None
#                self.closed = True   
#            __enter__ = __call__ = begin
#            def __exit__(self,  type=None, value=None, traceback=None):
#                if self.__tx is not None:
#                    if type is None:
#                        self.success()
#                    else:
#                        self.failure()
#                    self.finish()
#        
#        self.__transaction__ = MyTransaction()
#        return  self.__transaction__
        

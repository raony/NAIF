# -*- coding: utf-8 -*-
"""
This is a wrapper module over neo4j



"""
import neo4j
from datetime import datetime

#from links import Link
from graphdb.nodes import NodeManager
from graphdb.links import LinkManager
    
class FeedResult(object):
    OK = 1
    FAILURE = 0
    
    def __init__(self, datetime):
        self.status = self.FAILURE
        self.datetime = datetime
        self.inserted = 0
        self.conflicted = 0
        self.updated = 0
        self.missing = []
    
    @property
    def total(self):
        return self.inserted + self.conflicted + self.updated + len(self.missing)
    
    def __str__(self):
        return '{3} >> inserted: {0}, updated: {1}, conflicted: {2}, missing: {4}'.format(self.inserted, 
                                                                                          self.updated, 
                                                                                          self.conflicted, 
                                                                                          self.status, 
                                                                                          len(self.missing))

class FeedHistory(object):
            
    def __init__(self, neo):
        self.__neo__ = neo
        if self.__neo__.ref.FEEDHISTORY.single:
            self.__history_head__ = self.__neo__.ref.FEEDHISTORY.single.end 
        else:
            self.__history_head__ = self.__neo__.node()
            self.__neo__.ref.FEEDHISTORY(self.__history_head__)
            
    def __node__(self, result):
        result_node = self.__neo__.node(datetime=result.datetime.strftime('%Y%m%d%H%M%S%f'),
                                 status=result.status, 
                                 inserted=result.inserted,
                                 updated = result.updated,
                                 conflicted = result.conflicted)
        if 'nodes' in result.__dict__:
            result_nodes_node = self.__node__(result.nodes)
            result_node.NODES(result_nodes_node)
        return result_node 
    
    def __result__(self, node):
        result = FeedResult(datetime.strptime(node['datetime'], '%Y%m%d%H%M%S%f'))
        result.status = int(node['status'].toString())
        result.inserted = int(node['inserted'].toString())
        result.conflicted = int(node['conflicted'].toString())
        result.updated = int(node['updated'].toString())
        if node.NODES.outgoing.single != None:
            result.nodes = self.__result__(node.NODES.single.end)
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
                    while (True):
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
        self.__history__ = None 

    def _add_global_property(self, key, value):
        self.__neo__.ref[key] = value
    
    def _get_global_property(self, key, default=''):
        return str(self.__neo__.ref.get(key, default))
    
    def _traverse_node_by_type(self, node_types):
        class NodeTraverse(neo4j.Traversal):
            types = map(lambda x: neo4j.Outgoing.__getattr__(x), node_types) #@UndefinedVariable
            order = neo4j.BREADTH_FIRST #@UndefinedVariable
            stop = neo4j.StopAtDepth(1) #@UndefinedVariable
            returnable = neo4j.RETURN_ALL_BUT_START_NODE #@UndefinedVariable
        
        return NodeTraverse(self.__neo__.ref)

    def _new_node(self, id, type, **kwargs):
        node = self.__neo__.node(net_id=id, type=type, **kwargs)
        self.__neo__.ref.__getattr__(type)(node)
        return node
    
    def _index(self, name):
        return self.__neo__.index(name, create=True)
    
    def _raw_index(self, name):
        class RawIndexTraverse(neo4j.Traversal):
            types = [ neo4j.Outgoing.index ] #@UndefinedVariable
            order = neo4j.BREADTH_FIRST #@UndefinedVariable
            def isReturnable(self, position):
                return (not position.is_start and
                        position['index_name'] == name)
        
        class raw_index(object):
            def __init__(self, _raw_index):
                self._raw_index = _raw_index
            def __getitem__(self, key):
                result = self._raw_index.get(str(key), None)
                return result and int(result.toString()) or None
            def __setitem__(self, key, value):
                self._raw_index[str(key)] = value
            def __delitem__(self, key):
                del self._raw_index[str(key)]
            def __contains__(self, key):
                return str(key) in self._raw_index
            def __iter__(self):
                for key in self._raw_index:
                    if key == 'index_name': continue
                    yield key
                        
        indexes = [n for n in RawIndexTraverse(self.__neo__.ref)]
        if indexes:
            return raw_index(indexes[0])
        else:
            index = self.__neo__.node(index_name=name)
            self.__neo__.ref.index(index)
            return raw_index(index)

    @property
    def _relationship(self):
        return self.__neo__.relationship
    
    @property
    def node(self):
        return NodeManager(self)
    
    @property
    def link(self):
        return LinkManager(self)

    def shutdown(self):
        self.__neo__.shutdown()
        

    def read_node(self, node, policy, result):
        node = dict(node)
        return_node = None
        if node['id'] in self.node:
            if policy.update_when_found(node, self.node[node['id']]):
                node['net_id'] = node['id']
                del node['id']
                self.node[node['net_id']].update(node)
                return_node = self.node[node['net_id']]
                result.updated += 1
            else:
                result.conflicted += 1
        elif policy.create_when_not_found(node):
            result.inserted += 1
            return_node = self.node(**node)
        else:
            if not [n['id'] for n in result.missing if n['id'] == node['id']]:
                result.missing.append(node)
            return_node = None
        return return_node

    def read_nodes(self, nodeFeed, transaction):
        
        result = FeedResult(datetime.today())
        
        try:
            for node in nodeFeed:
                self.read_node(node, nodeFeed.node_policy, result)
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

    def read_links(self, linkFeed, transaction):
        result = FeedResult(datetime.today())
        result.nodes = FeedResult(result.datetime)
        try:
            update = False
            create = False
#            print 1
            for link in linkFeed:
#                print 2
#                import pdb
#                pdb.set_trace()
                if link['id'] in self.link:
#                    print 3
                    if linkFeed.link_policy.update_when_found(link, self.link[link['id']]):
                        update = True                        
                    else:
                        result.conflicted += 1
                elif linkFeed.link_policy.create_when_not_found(link):
#                    print 4
                    create = True
                else:
#                    print 5
                    if not [l['id'] for l in result.missing if l['id'] == link['id']]:
                        result.missing.append(link)  
                
                if update or create:
                    dlink = dict(link)
                    
                    dlink['start'] = self.read_node(dlink['start'], linkFeed.node_policy, result.nodes)
                    dlink['end'] = self.read_node(dlink['end'], linkFeed.node_policy, result.nodes)
                    if not(dlink['start'] and dlink['end']):
                        result.missing.append(link)
                    else:
                        if update:
                            del self.link[dlink['id']]
                            result.updated += 1
                        else:
                            result.inserted += 1
#                        print 6
#                        import pdb
#                        pdb.set_trace()
                        self.link(**dlink)
#                        print 7
                    
        except:
            result.status = FeedResult.FAILURE
            transaction.failure()
        else:
            result.status = FeedResult.OK
            transaction.success()
        finally:
            self.add_to_history(linkFeed.id, result)
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

class FeedPolicy(object):
    NONE = 2
    CREATE_AND_UPDATE = 3
    CREATE = 5
    UPDATE = 7
    
    def __init__(self, policy, conflict=None):
        self.policy = policy
        self.conflict = conflict
        
    def update_when_found(self, new, old):
        return self.policy == self.UPDATE or self.policy == self.CREATE_AND_UPDATE or (self.conflict and self.conflict(new, old))
    
    def create_when_not_found(self, new):
        return self.policy == self.CREATE or self.policy == self.CREATE_AND_UPDATE 

class LinkFeed(object):
    def __init__(self, data, id=1, link_policy=None, node_policy=None):
        self.id = id
        self.data = data
        self.link_policy = link_policy or FeedPolicy(FeedPolicy.NONE)
        self.node_policy = node_policy or FeedPolicy(FeedPolicy.NONE)
    
    def __getitem__(self, key):
        return self.data[key]

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
        

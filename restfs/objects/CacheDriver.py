from tornado.options import options,define

class Cache(object):
    
#cache example
#{IdObj : {field : value}}
#{11111 : {ACL : objectACL}}
    
    def __init__(self, cache={}, cacheList=[]*options.cache_size):
        
    
        self.cache = cache
        self.cacheList = cacheList
        
    def getCacheList(self, idObj):
        if idObj in self.cacheList:
            return self.cacheList
        return None
        
    def getCache(self, idObj, field=None) :
        if field :
            return self.cache[idObj][field]
        return self.cache[idObj]
    
    def setCache(self, idObj, field, objDict):
        self.cache[idObj][field] = objDict
        self.cacheList.pop()
        self.cacheList.insert(0,idObj)
        
    def cleanCache(self):
        self.cache     = {}
        self.cacheList = []*options.cache_size
        
#    def setMultiCache(self, listIdObj, field,  listObjDict):
#        self.cache.update(dict(zip(listIdObj,listObjDict)))
    
    
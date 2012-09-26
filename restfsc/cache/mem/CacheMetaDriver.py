import logging

class CacheMetaDriver(object):

    _LOGGER = logging.getLogger('META_CACHE_DRIVER')
    
    def __init__(self):
    
        self.cache = {}
        
    def getCache(self):
        self._LOGGER.info("Getting cache items %s") 
        return self.cache.items()
        
    def getItem(self, idObj, field) :
        self._LOGGER.info("Getting item %s" % idObj) 
        if self.cache.has_key(idObj):
            if self.cache[idObj].has_key(field):
                self._LOGGER.info("Founded in cache") 
                self._LOGGER.debug("idObj %s" % idObj) 
                self._LOGGER.debug("field %s" % field) 
                return self.cache[idObj][field]
        self._LOGGER.info("Object not found in cache %s" % idObj) 
        return None
    
    def setItem(self, idObj, field, objDict):
        self._LOGGER.info("Setting item %s" % idObj) 
        self._LOGGER.debug("field %s" % field) 
        self._LOGGER.debug("objDict %s" % objDict) 
        if not self.cache.has_key(idObj):
            self.cache[idObj] = {}
        self.cache[idObj][field] = objDict
        self._LOGGER.info("Item added in cache %s") 
        
    def delObj(self,idObj):  
        self._LOGGER.info("Removing item %s" % idObj) 
        del self.cache[idObj]
        
        
    def getCacheList(self, idObj):
        self._LOGGER.info("Getting cache list %s" % idObj) 
        if idObj in self.cacheList:
            return self.cacheList
        return None
    
    def isPresent(self,idObj):
        self._LOGGER.info("Check if object is present %s" % idObj) 
        return self.cache.has_key(idObj)
#    def setMultiCache(self, listIdObj, field,  listObjDict):
#        self.cache.update(dict(zip(listIdObj,listObjDict)))
    
    
from tornado.options import options,define
import uuid
import logging
from time import time
import thread

class CacheManager(object):
    blockCounter = 0
    objCounter   = 0 
    blockIdList = []
    metaIdList  = []
    chaceBlockList = {} 
    cacheIdList    = {}
    _LOGGER = logging.getLogger('CACHE_MANAGER')

    def __init__(self):
        self.blockCounter = 0
        self.objCounter   = 0

        
        if 'cache_meta_driver' and 'cache_storage_driver' and 'cache_block_size' and 'cache_meta_size' not in options.keys():
                define("cache_meta_driver",     default="mem",  help="Metadata Driver")
                define("cache_storage_driver",  default="disk", help="Block Storage system")
                define("cache_block_size",      default=100,    help="Cache Block Max size")
                define("cache_meta_size",       default=100,    help="Cache Block Max size")
                


        storage_plugin = "restfsc.cache.%s.CacheStorageDriver" % options.cache_storage_driver
        storage_mod = __import__(storage_plugin, globals(), locals(), ['CacheStorageDriver'])
        CacheStorageDriver = getattr(storage_mod, 'CacheStorageDriver')
        self.cacheStorageDriver = CacheStorageDriver()
                
        meta_plugin = "restfsc.cache.%s.CacheMetaDriver" % options.cache_meta_driver
        meta_mod = __import__(meta_plugin, globals(), locals(), ['CacheMetaDriver'])
        CacheMetaDriver = getattr(meta_mod, 'CacheMetaDriver')
        self.cacheMetaDriver = CacheMetaDriver()

        
        self.meta_lock  = thread.allocate_lock()
        self.block_lock = thread.allocate_lock()
        
        #####STATS
        self.num_get        = 0
        self.num_set        = 0
        self.num_get_failed = 0
            
    #### METADATA ###
    
    def getMetaCache(self):
        self._LOGGER.info("Getting cache") 
        cache =  self.cacheMetaDriver.getCache()
        return cache
        
    def getMetaItem(self, idObj, field):
        self._LOGGER.info("Getting item {0} with field {1}".format(idObj,field)) 
        self.meta_lock.acquire()
        item = self.cacheMetaDriver.getItem(idObj,field)
        self.meta_lock.release()
        
        if not item:
            self._LOGGER.info("Item not found in cache") 
            self.num_get_failed += 1
            return None, None
        
        self._LOGGER.info("Item founded in cache") 
        cacheMeta = self.cacheMetaDriver.getItem(idObj, field+'.cache_meta')
                
        if cacheMeta.has_key('subscribe') :
            if cacheMeta['subscribe']== True:
                self._LOGGER.info("Item is subscribed") 
                return item, -1
            
        self.num_get += 1
        return item, cacheMeta
        
    def setMetaItem(self, idObj, field, value , version):
        self._LOGGER.info("Setting item %s",idObj)
        self._LOGGER.debug("field  %s ", field)
        self._LOGGER.debug("value  %s ", value)
        self._LOGGER.debug("version %s ", version)

        self.meta_lock.acquire()
        item = self.cacheMetaDriver.isPresent(idObj)
        self.num_get += 1
        if self.objCounter < options.cache_meta_size and not item:
            self._LOGGER.info("There is space in cache, and there is not the item")
            self._LOGGER.info("Writing the item in cache")
            self.num_get_failed += 1
            self.blockCounter += 1
            self.metaIdList.insert(0,idObj)
        elif self.objCounter > options.cache_meta_size and not item:
            self._LOGGER.info("There is NOT space in cache, and there is not the item")
            self._LOGGER.info("Writing the item in cache, deleting the last one present")
            self.num_get_failed += 1
            oKey = self.metaIdList.pop()
            self._LOGGER.debug("old_key  %s ", oKey)
            self.cacheMetaDriver.delObj(oKey)
            self.metaIdList.insert(0,idObj)
            
        self.cacheMetaDriver.setItem(idObj, field, value)
        cacheMeta = {'vers':version}
        self._LOGGER.info("Setting cacheMeta : %s", cacheMeta)
        self.cacheMetaDriver.setItem(idObj,field+'.cache_meta', cacheMeta)
        self.num_set += 1
        self.meta_lock.release()
        
        
        
    def delMetaObj(self,idObj):

        self._LOGGER.info("Deleting item %s",idObj)
        self.meta_lock.acquire()
        item = self.cacheMetaDriver.isPresent(idObj)
        if item:
            self._LOGGER.info("Item founded, now deleting it")
            self.num_get +=1
            self.cacheMetaDriver.delObj(idObj)
            self.blockCounter -= 1
            self.metaIdList.remove(idObj)
        else:
            self._LOGGER.error("Item NOT founded, error")
            self.num_get_failed +=1
        self.meta_lock.release()
        
        
    
    #### STORAGE ###
    
    def writeBlock(self, bucket, idObj, bKey, bhash, block):
        self._LOGGER.info("Writing block %s",idObj)
        self._LOGGER.debug("bucket  %s ", bucket)
        self._LOGGER.debug("idObj  %s ", idObj)
        self._LOGGER.debug("bKey %s ", bKey)
        self._LOGGER.debug("bhash  %s ", bhash)
        #self._LOGGER.debug("block %s ", block)
        self.block_lock.acquire()
        cacheBlockKey = self.chaceBlockList.get(bKey,None)
        self.block_lock.release()
        
        if cacheBlockKey:
            self._LOGGER.info("Item founded in cache, updating it")
            self.num_get +=1
            self.cacheStorageDriver.write(bKey,block)
            self.chaceBlockList[bKey]["hash"] = bhash
            self.num_set += 1
            return 
    
        self._LOGGER.info("Item NOT founded in cache, writing it")
        self.num_get_failed +=1
            
        cacheBlockKey = self._getCacheBlockKey()
        self.cacheStorageDriver.write(cacheBlockKey,block)
        self.num_set += 1
        #Update cache index
        self.block_lock.acquire()
        self.cacheIdList[cacheBlockKey] = bKey
        self.chaceBlockList[bKey] = {"bkey":cacheBlockKey,"hash":bhash}
        self.blockIdList.insert(0,cacheBlockKey)
        self._LOGGER.debug("cacheBlockKey :%s",cacheBlockKey)

        self.block_lock.release()
    
    def readBlock(self, bucket, idObj, bKey, bhash):
        print 'CACHEMNG'
        self._LOGGER.info("Reading block %s",idObj)
        self._LOGGER.debug("bucket  %s ", bucket)
        self._LOGGER.debug("idObj  %s ", idObj)
        self._LOGGER.debug("bKey %s ", bKey)
        self._LOGGER.debug("bhash  %s ", bhash)
        item = None
        self.block_lock.acquire()
        if self.chaceBlockList.has_key(bKey):
            self._LOGGER.info("The block is in cache")
            cacheBlockKey =  self.chaceBlockList[bKey]
            self.block_lock.release()
            if cacheBlockKey and cacheBlockKey['hash'] == bhash:
                self._LOGGER.info("The checksum are the same,so it is in cache definetly")
                item = self.cacheStorageDriver.getBlock(cacheBlockKey["bkey"])
                self.num_get += 1
            #FIXME clean the block 
            else:
                self._LOGGER.info("The block in cache is old, removing it")
                self.removeBlock(bucket,idObj,bKey)
     
        else:
            self.block_lock.release()
        return item
            
            
    def removeBlock(self,bucket,idObj,bKey):
        self._LOGGER.info("Removing the block %s",idObj)
        self._LOGGER.debug("bucket  %s ", bucket)
        self._LOGGER.debug("idObj  %s ", idObj)
        self._LOGGER.debug("bKey %s ", bKey)
        self.block_lock.acquire()
        cacheBlockKey =  self.chaceBlockList[bKey]["bkey"]
        self.num_get += 1
        self.blockIdList.remove(cacheBlockKey)
        self.blockIdList.append(cacheBlockKey)
        self.block_lock.release()
       
    
########################
####### INTERNAL #######       
########################      
    
    
    def _getCacheBlockKey(self):
        self._LOGGER.info("Getting the block  key ")
        self.meta_lock.acquire()
        if self.blockCounter < options.cache_block_size:
            self._LOGGER.info("I have space in cache, so i write the block")
            cbKey = uuid.uuid4().get_hex()
            self.blockCounter += 1
            self.blockIdList.insert(0,cbKey)
        else:
            self._LOGGER.info("I have no space in cache, so use an old key")
            cbKey = self.blockIdList.pop()
            self._LOGGER.debug("cbKey %s",cbKey)
            idObj = self.cacheIdList.get(cbKey)
            self.cacheIdList.remove(cbKey)
            self.chaceBlockList.remvoe(idObj)
            self.blockIdList.insert(0,cbKey)
        self.meta_lock.release()
        return cbKey
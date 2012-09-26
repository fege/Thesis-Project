import hashlib
import random
import logging
import time
from tornado.options import  options,define

def rpcmethod(func):
        """ Decorator to expose Node methods as remote procedure calls
       
        Apply this decorator to methods in the Node class (or a subclass) in order
        to make them remotely callable via the DHT's RPC mechanism.
        """
        func.rpcmethod = True
        return func

class StorageManager (object):

    
    _LOGGER = logging.getLogger('STORAGE_MANAGER') 
    def __init__(self):
        
        if 'storage_driver' not in options.keys():
                define("storage_driver",     default="fs2",  help="Storage Driver")
                
        
        storage_plugin_bucket = "restfs.storage.%s.StorageBucketDriver" % options.storage_driver
        storage_mod_bucket = __import__(storage_plugin_bucket, globals(), locals(), ['StorageBucketDriver'])
        StorageBucketDriver = getattr(storage_mod_bucket, 'StorageBucketDriver')
        self.storageBuck = StorageBucketDriver()
        
       
        storage_object_plugin = "restfs.storage.%s.StorageObjectDriver" % options.storage_driver
        storage_object_mod = __import__(storage_object_plugin, globals(), locals(), ['StorageObjectDriver'])
        StorageObjectDriver = getattr(storage_object_mod, 'StorageObjectDriver')
        self.storage = StorageObjectDriver()
         
    @rpcmethod
    def writeBlock(self,bucket_name, idObject, key , block, bhash, uid=None, gid=None, context=None):
        self._LOGGER.info("WRITE Block") 
        self.storage.writeBlock(bucket_name, idObject, key, block)
    
    @rpcmethod
    def removeBlock(self,bucket_name, idObject, idBlock, uid=None, gid=None, context=None):
        self._LOGGER.info("REMOVE Block") 
        return self.storage.delBlock(bucket_name , idObject, idBlock)
    
    @rpcmethod   
    def remove(self,bucket_name, idObject, segBlock,  uid=None, gid=None, context=None):
        self._LOGGER.info("REMOVE Segment") 
        return self.storage.delSegment(bucket_name , idObject, segBlock)
    
    @rpcmethod
    def readBlock(self, bucket_name, idObject, key, uid=None, gid=None, context=None):
        '''get block
        sbyte = offset-block_size*block_start 
        ebyte  = (offset+size)-block_size*block_stop 
        if start == stop
        #come andra con il binario ? 
            getstring byte 
            return byte

        data= sbyte-(sblock+1)*block_size 
        for x in sblock:(eblock-1):
            readblock
        data=+ block

        readblock
        data=+ :(sblock-eblock+1)*block_size-size'''
        
        self._LOGGER.info("READ Block") 
        return self.storage.readBlock(bucket_name, idObject, key)




    #  Bucket OPeration 
    #   
    ################################################################ 
    def createBucket(self,bucket_name, context):
        self._LOGGER.info("CREATE Bucket") 
        return self.storageBuck.create(bucket_name)
    
    def removeBucket(self,bucket_name, context):
        self._LOGGER.info("REMOVE Bucket") 
        return self.storageBuck.remove(bucket_name)

    # Remove (delete) the given file, symbolic link, hard link, or special node. 
    # Note that if you support hard links, unlink only deletes the data when the last hard link is removed. 
    # See unlink(2) for details.
    
    @rpcmethod
    def removeObject(self, bucket_name, objectId, uid=None, gid=None, context=None):
        self._LOGGER.info("REMOVE Object") 
        self.storage.remove(bucket_name, objectId)

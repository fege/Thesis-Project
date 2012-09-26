import logging

from tornado.options import  options
from restfs.objects.BucketOwner import BucketOwner

def rpcmethod(func):
        """ Decorator to expose Node methods as remote procedure calls
       
        Apply this decorator to methods in the Node class (or a subclass) in order
        to make them remotely callable via the DHT's RPC mechanism.
        """
        func.rpcmethod = True
        return func

class ResourceManager(object):
    _LOGGER = logging.getLogger('RESOURCE_MANAGER')
    
    def __init__(self):
          
        #FIXME SERVICE .. 
        resource_plugin = "restfs.resource.%s.ResourceDriver" % options.resource_driver
        resource_mod = __import__(resource_plugin, globals(), locals(), ['ResourceDriver'])
        Resource = getattr(resource_mod, 'ResourceDriver')
        self.resource = Resource()
         
    ##########################################################################    
    # Bucket
    ##########################################################################        
        
    def findBucket(self,bucket_name): 
        self._LOGGER.debug("FIND Bucket %s" % bucket_name) 
        return self.resource.findBucket(bucket_name)
    
    @rpcmethod
    def findCluster(self,bucket_name): 
        
        return self.resource.findCluster(bucket_name)
           
    
    def getBucketListByOwner(self,idUser):
        self._LOGGER.debug("Get Bucket List by user %s " % idUser)
        return self.resource.getBucketListByOwner(idUser)
        
    def getCountBucketByOwner(self,idUser): 
        self._LOGGER.info("GET Count Bucket By Owner") 
        return self.resource.getCountBucketByOwner(idUser)

    def getRegionList(self): 
        self._LOGGER.info("GET Region List") 
        return self.resource.getRegionList()


    def addBucket(self, bucket_name, idUser):
        self._LOGGER.info("Add Bucket") 
        bucket = BucketOwner(idUser,bucket_name)
        self.resource.addBucket(bucket)
        
    def removeBucket(self, bucket_name, context):
        self._LOGGER.info("Remove Bucket") 
        self.resource.removeBucket(bucket_name)
        
    def setBucketStatus(self, bucket_name, status, context):
        self._LOGGER.info("SET Bucket Status")     
        self.resource.setBucketStatus(bucket_name, status)
        
       
            
import logging
import os.path
import shutil
from tornado import web
from restfs.storage.fs2.StorageBase import StorageBase

class StorageBucketDriver(StorageBase):
    '''
    Bucket operations on the storage
    '''
    
    def __init__(self ):
        StorageBase.__init__(self)     
        if not os.path.exists(self._BUCKET_PATH):
            logging.debug('Create root direcoty %s' % self._BUCKET_PATH)
            os.makedirs(self._BUCKET_PATH)         
    
    def create(self,bucket_name):
        """
        Create Bucket
        """  
        bucket_path = self._getPath(bucket_name)                
        os.makedirs(bucket_path)
     
    
    def remove(self,bucket_name):
        """
        Delete Bucket
        """ 
        bucket_path = self._getPath(bucket_name)
        
        if not os.path.isdir(bucket_path):
            logging.info("Remove Bucket: Path not exist: %s " % (bucket_path))
            return
        
        shutil.rmtree(bucket_path)
        #os.rmdir(bucket_path)
        logging.debug("Remove Directory: %s " % (bucket_path))
        

       
    #
    # INTERNALS
    ######################################################################
    
   
              
    def _getPath(self,bucket_name):
        path = os.path.abspath(os.path.join(self._BUCKET_PATH,
                                           bucket_name))
        if not path.startswith(self._BUCKET_PATH) :
            raise web.HTTPError(404)
        
        return path
     


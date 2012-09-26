import os 
import logging
import datetime
import os.path
import tornado.options
import bisect
import hashlib
import shutil

from tornado import web
from tornado.options import define, options
#from  restfs.backends.fs.FsObject import FsObject

#Ultima classe, lei opera direttamente sul FS
class StorageBucketDriver(object):
    _ROOT_PATH      = "/tmp/s3"
    _BUCKET_PATH    = "/tmp/s3/bucket"
    _USERS_PATH     = "/tmp/s3/users"
    _BUCKET_DEPTH   = 0
    
    def __init__(self):

        define("root_path"   , default="/tmp/bucket" , help="storage direcotry")
        define("bucket_depth", default=0  , help="object distribution inside of bucket", type=int)
    
        #Read Command line with new definitions
        tornado.options.parse_config_file(options.conf) 
          
        self._ROOT_PATH    = os.path.abspath(options.root_path)
        self._BUCKET_PATH  = os.path.abspath(os.path.join(self._ROOT_PATH,'buckets'))
        self._USER_PATH    = os.path.abspath(os.path.join(self._ROOT_PATH,'users'))
        self._BUCKET_DEPTH = options.bucket_depth 
                
        if not os.path.exists(self._ROOT_PATH):
            logging.debug('Create root direcoty %s' % self._ROOT_PATH)
            os.makedirs(self._ROOT_PATH)

        if not os.path.exists(self._BUCKET_PATH):
            logging.debug('Create bucket direcoty %s' % self._BUCKET_PATH)
            os.makedirs(self._BUCKET_PATH)
        
        if not os.path.exists(self._USER_PATH):
            logging.debug('Create user direcoty %s' % self._USER_PATH)
            os.makedirs(self._USER_PATH)
            
                 
    """
    Create Bucket
    
    """  
    def createBucket(self,bucket_name):
        bucket_path = self._getBucketPath(bucket_name)                
        os.makedirs(bucket_path)
     
    """
    Delete Bucket
    
    """ 
    def removeBucket(self,bucket_name):
        bucket_path = self._getBucketPath(bucket_name)
        
        if not os.path.isdir(bucket_path):
            logging.debug("Path not exist: %s " % (bucket_path))
            raise web.HTTPError(404)
        
        shutil.rmtree(bucket_path)
        #os.rmdir(bucket_path)
        logging.debug("Remove Directory: %s " % (bucket_path))
        
    def countBucket(self,bucket_name):
        bucket_path = self._getBucketPath(bucket_name)
        
        if not os.path.isdir(bucket_path):
            logging.debug("Path not exist: %s " % (bucket_path))
            raise web.HTTPError(404)
        
        contents = os.listdir(bucket_path)
        #Count 
        count = 0
        if len(contents) > 0:
            for el in contents:
                el_path = os.path.join(bucket_path,el)
                if os.path.isdir(el_path):
                    count += 1
        return count
        
       
    #
    # INTERNALS
    ######################################################################
    
    def _findObjects(self, path, object_names):
        dirs = os.listdir(path)

        i = 0
       
        for dir in dirs:
            new_path = os.path.join(path,dir)
           
            if os.path.isdir(new_path):
                self._findObjects(new_path,object_names)
                i +=1
                
        if i == 0:
            object_names.append(path)
              
    def _getBucketPath(self,bucket_name):
        path = os.path.abspath(os.path.join(self._BUCKET_PATH,
                                           bucket_name))
        if not path.startswith(self._BUCKET_PATH) :
            raise web.HTTPError(404)
        
        return path
     
 
     
    def _getObjectPath(self,bucket_name,object_name):
        
        if self._BUCKET_DEPTH < 1:
            path_prefix = os.path.abspath(os.path.join(
                   self._BUCKET_PATH, bucket_name, object_name))
        else:
            #Multi Level storage
            hash = hashlib.md5(object_name).hexdigest()
            path = os.path.abspath(os.path.join(
                  self._BUCKET_PATH, bucket_name))
            for i in range(self.BUCKET_DEPTH):
                path_prefix = os.path.join(path, hash[:2 * (i + 1)])
             
        object_path = os.path.join(path_prefix, "data.dat")
        
        if not object_path.startswith(self._BUCKET_PATH) :
            raise web.HTTPError(404)
        
        return object_path

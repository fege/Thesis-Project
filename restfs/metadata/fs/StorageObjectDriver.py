import os 
import logging
import tempfile
import datetime
import os.path
import tornado.options
import bisect
import shutil
import cStringIO


try:
        from hashlib import md5, sha1
except ImportError:
        from md5 import md5
        import sha as sha1

from tornado import web
from tornado.options import define, options
#from  restfs.backends.fs.FsObject import FsObject

class StorageBucketDriver(object):
    _ROOT_PATH      = "/tmp/s3"
    _BUCKET_PATH    = "/tmp/s3/bucket"
    _USERS_PATH     = "/tmp/s3/users"
    _BUCKET_DEPTH   = 0
    
    def __init__(self):
        if not hasattr(options,"root_path"):
            define("root_path"   , default="/tmp/bucket" , help="storage direcotry")
        if not hasattr(options,"root_path"):
            define("bucket_depth", default=0  , help="object distribution inside of bucket", type=int)
    
        #Read Command line with new definitions
        tornado.options.parse_config_file(options.conf) 
          
        self._ROOT_PATH    = os.path.abspath(options.root_path)
        self._BUCKET_PATH  = os.path.abspath(os.path.join(self._ROOT_PATH,'buckets'))
        self._BUCKET_DEPTH = options.bucket_depth 
                
        if not os.path.exists(self._ROOT_PATH):
            logging.debug('Create root direcoty %s' % self._ROOT_PATH)
            os.makedirs(self._ROOT_PATH)

        if not os.path.exists(self._BUCKET_PATH):
            logging.debug('Create bucket direcoty %s' % self._BUCKET_PATH)
            os.makedirs(self._BUCKET_PATH)
        
            
          
  
     
  
        
    """
    READ Object
    
    """    
    def readObject(self, bucket_name, object_name):
        #object_path = self._getObjectPath(bucket_name, object_name)
        #FD = open(os.path.join(object_path,"data.dat"), "rb")
        #return FD.read()
        object_path = None
        try:
            object_path = self._getObjectPath(bucket_name, object_name)
            FD = open(os.path.join(object_path,"data.dat"), "rb")
        except:
            FD = tempfile.NamedTemporaryFile( dir=object_path, delete= False)
            FD = open()
            raise
        
        return FD.read()
    
     
    """
    WRITE Object
    
    """    
    def writeObjects(self, bucket_name, object_name, data):
                
        object_path = self._getObjectPath(bucket_name, object_name)
        
        #directory = os.path.dirname(object_path)
        
        if not os.path.exists(object_path): 
            os.makedirs(object_path)
         
        file_name = os.path.join(object_path,"data.dat") 
        temp      = None  
        file_temp = None
        try:
            temp = tempfile.NamedTemporaryFile( dir=object_path, delete= False)
            file_temp = temp.name
            res = temp.write(data)
            temp.close()     
            if os.path.exists(file_name):
                os.unlink(file_name)
            os.rename( file_temp, file_name)
            info = os.stat(file_name)
        except:
            if temp:
                temp.close()
            if file_temp and os.path.exists(file_name):
                os.unlink(file_name)
            raise 
        size = info.st_size
        #os.unlink(file_name)    
        return size
    
   
    def copyObject(self, bucket_name, object_name, bucket_orig, object_orig):
                
        object_path = self._getObjectPath(bucket_name, object_name)
        object_orig_path = self._getObjectPath(bucket_orig, object_orig)
        
        
        #directory = os.path.dirname(object_path)       
        if not os.path.exists(object_path): 
            os.makedirs(object_path)
        
        dst = os.path.join(object_path,"data.dat")
        src = os.path.join(object_orig_path,"data.dat")
       
        shutil.copy2(src, dst)
        
        info = os.stat(dst)

        return info.st_size 
    
    
    def MD5(self,bucket_name, object_name):
        object_path = self._getObjectPath(bucket_name, object_name)
        FD = open(os.path.join(object_path,"data.dat"), "rb")

        h = md5()
        while True:
            data = FD.read(32*1024)
            if not data:
                break
            h.update(data)
        return h.hexdigest()
 
  
    def lastModify(self,bucket_name, object_name):
                
        object_path = self._getObjectPath(bucket_name, object_name)
        info = os.stat(os.path.join(object_path,"data.dat"))
       
        return info.st_mtime
        
     
    """
    DELETE Object
    
    """  
    def deleteObject(self,bucket_name,object_name):
        object_path = self._getObjectPath(bucket_name, object_name)
       
      
        files = os.listdir(object_path)
        for file in files:
            el =  os.path.join(object_path, file)
            os.unlink(el)
        
        os.removedirs(object_path)
       
     
    """
    WRITE Block
    
    This fuction will write a block of datas
     
    """
    def writeBlock(self,bucket_name, object_name, idSegment, idBlock, little_data):
        
        
        #Check bucket path    ???  
        object_path = self._getObjectPath(bucket_name, object_name)
        #directory = os.path.dirname(object_path)
        
        if not os.path.exists(object_path): 
            os.makedirs(object_path)
         
        file_name = os.path.join(object_path,"{0}data{1}.dat".format(idSegment, idBlock) )
        temp      = None  
        file_temp = None
        try:
            temp = tempfile.NamedTemporaryFile( dir=object_path, delete= False)
            file_temp = temp.name
            res = temp.write(little_data)
            temp.close()     
            if os.path.exists(file_name):
                os.unlink(file_name)
            os.rename( file_temp, file_name)
            info = os.stat(file_name)
        except:
            if temp:
                temp.close()
            if file_temp and os.path.exists(file_name):
                os.unlink(file_name)
            raise   
        
        
    """
    READ BLOCK
    
    """    
    def readBlocks(self, bucket_name, object_name,num_segment,num_block):
        object_path = None
        lista = []
        idSegm = 0
        
        while num_segment > 0:
            idSegm += 1
            for idBlock in range(1,31): 
                object_path = self._getObjectPath(bucket_name, object_name)
                if num_block != 0:
                    FD = open(os.path.join(object_path,"{0}data{1}.dat".format(idSegm, idBlock)), "rb")
                    lista.append(FD.read())
                    FD.close()
                    num_block = num_block-1
            num_segment -=1
            
        return lista
    #
    # INTERNALS
    ######################################################################
    
  
              
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
             
        #object_path = os.path.join(path_prefix, "data.dat")
        object_path = path_prefix
        
        if not object_path.startswith(self._BUCKET_PATH) :
            raise web.HTTPError(404)
        
        return object_path

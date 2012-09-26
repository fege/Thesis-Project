import tempfile
import os.path
import tornado.options
import cStringIO
import shutil
from tornado import web
from tornado.options import define, options
from restfs.storage.fs2.StorageBase import StorageBase
#from  restfs.backends.fs.FsObject import FsObject

class StorageObjectDriver(StorageBase):
    '''
    Object operations on the storage
    '''
    
    def __init__(self):
        StorageBase.__init__(self)
        define("storage_object_depth", default=0  , help="object distribution inside of bucket", type=int)
        #Read Command line with new definitions
        tornado.options.parse_config_file(options.conf) 
        self._OBJECT_DEPTH = options.storage_object_depth 
                 
      
    
    def writeBlock(self,  bucket_name, idObject, key, block):
        """
        WRITE Block
        This fuction will write a block of datas
        """
        path = self._getBlockPath(bucket_name, idObject)
        
        if not os.path.exists(path):
            os.makedirs(path)
         
        file_name = os.path.join(path,(key+".dat") )
        
        temp      = None  
        file_temp = None
        
        try:
            temp = tempfile.NamedTemporaryFile( dir=path, delete=False)
            file_temp = temp.name
            res = temp.write(block)
            temp.close()     
            if os.path.exists(file_name):
                os.unlink(file_name)
            os.rename( file_temp, file_name)
            
        except:
            if temp:
                temp.close()
            if file_temp and os.path.exists(file_name):
                os.unlink(file_temp)
            raise 
         
        return 
    
    
    
    def readBlock(self, bucket_name, idObject, key):
        """
        READ Block
        This fuction will read a block of datas
        """
        myStringIO = cStringIO.StringIO()
        path = self._getBlockPath(bucket_name, idObject)
        FD = open(os.path.join(path,(str(key)+".dat")), "rb")
        myStringIO.write(FD.read())
        FD.close()
        return myStringIO.getvalue()
    
    
        
    def remove(self, bucket_name, idObject):
        '''
        REMOVE
        This fuction will remove the whole object
        '''
        object_path = self._getObjectPath(bucket_name, idObject)
        shutil.rmtree(object_path)
        
     
    def delSegment(self, bucket_name, idObject, segBlock):
        '''
        DELETE SEGMET 
        This fuction will remove the segment of datas
        '''
        for idBlock in segBlock.values():
            self.delBlock(bucket_name, idBlock)
    
    def delBlock(self, bucket_name, idObject, idBlock):
        '''
        DELETE BLOCK
        This fuction will remove the single data
        '''
        block_path = self._getObjectPath(bucket_name, idObject)
        os.remove(os.path.join(block_path,(str(idBlock)+'.dat')))

            
    ######################################################################
    # INTERNALS
    ######################################################################
    
     
    def _getObjectPath(self, bucket_name, idObject):  
        object_path = os.path.join(self._getPath(bucket_name), idObject)
        
        if not object_path.startswith(self._BUCKET_PATH) :
            raise web.HTTPError(404)
        
        return object_path
     
    def _getBlockPath(self,idBucket, idObject):
        path_prefix = ""
        
        if self._OBJECT_DEPTH < 1:
            path_prefix = os.path.abspath(os.path.join(
                   self._BUCKET_PATH, idBucket, idObject))
        else:
            #Multi Level storage
            path = os.path.abspath(os.path.join(
                  self._BUCKET_PATH, idBucket))
            
            for i in range(self._OBJECT_DEPTH):
                    path_prefix = os.path.join(path, idObject[:2 * (i + 1)])
                     
        if not path_prefix.startswith(self._BUCKET_PATH) :
            raise web.HTTPError(404)
        
        return path_prefix
    
   
              
    def _getPath(self,bucket_name):
        path = os.path.abspath(os.path.join(self._BUCKET_PATH,
                                           bucket_name))
        if not path.startswith(self._BUCKET_PATH) :
            raise web.HTTPError(404)
        
        return path

import logging
import os.path
import cStringIO
from tornado.options import options, define

class CacheStorageDriver(object):
    
    _LOGGER = logging.getLogger('STORAGE_CACHE_DRIVER')
    
    def __init__(self):
        
        if 'cache_storage_path' not in options.keys():
            define("cache_storage_path",     default="/tmp/cache",  help="Data Block Storage")
        self.path       = options.cache_storage_path

                
    def write(self, key, block):
        self._LOGGER.info("Write block %s" % key) 
        
        if not os.path.exists(self.path):
            os.makedirs(self.path)
        
        file_name = os.path.join(self.path,(str(key)+".dat") )
        self._LOGGER.debug("File name : %s" % file_name) 
        file = open(file_name,'a')
        res = file.write(block)
        file.close() 

        return 
    
    
    def getBlock(self, key):
        self._LOGGER.info("Read block %s" % key) 
        myStringIO = cStringIO.StringIO()
        self._LOGGER.debug("File name : %s" % (str(key)+".dat")) 
        FD = open(os.path.join(self.path,(str(key)+".dat")), "rb")
        myStringIO.write(FD.read())
        FD.close()
        return myStringIO.getvalue()
import os.path
import tornado.options
from tornado import web
from tornado.options import define, options


class StorageBase(object):
    _BUCKET_PATH    = "/tmp/s3/bucket"
    
    def __init__(self):
        if not hasattr(options, "storage_root_path" ):
            define("storage_root_path"   , default="/tmp/buckets/cache" , help="storage direcotry")
        if not hasattr(options, "storage_bucket_depth" ):
            define("storage_bucket_depth", default=0  , help="object distribution inside of bucket", type=int)
    
        #Read Command line with new definitions
        tornado.options.parse_config_file(options.conf) 
          
        self._BUCKET_PATH    = os.path.abspath(options.storage_root_path)
        self._BUCKET_DEPTH = options.storage_bucket_depth 
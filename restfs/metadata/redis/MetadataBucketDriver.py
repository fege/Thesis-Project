import redis
import restfs.factories.BucketFactory as BF
import tornado.options
from tornado.options import define, options
from restfs.metadata.redis.MetadataBase import MetadataBase



class MetadataBucketDriver(MetadataBase):

    def __init__(self):
        define("metadata_bucket_db",     default="7"         , help="Redis DB for Bucket Metadata",)
        define("metadata_bucket_host",   default="localhost" , help="Redis DB host for Bucket Metadata",)
        define("metadata_bucket_port",   default=6379      , help="Redis DB port for Bucket Metadata", type=int)
        define("metadata_bucket_passwd", default=None        , help="Redis DB password for Bucket Metadata",)

        #READ Options
        tornado.options.parse_command_line()

        self.redis = redis.StrictRedis(host=options.metadata_bucket_host, port=options.metadata_bucket_port, db=options.metadata_bucket_db,password=options.metadata_bucket_passwd)


    
    ##########################################################################
    # Bucket METADATA
    ########################################################################## 
    
    """
    Bucket LIST present in this db
    
    """ 

    def getList(self,uid):
        return  self.redis.keys('#$*')

    
    # fixing default website, logging , versionig
    def create(self, name,  prop, location):
        #FIXME replace with single command (pipeline ! )    
        self.setProperty(name, BF.PROPERTIES_BUCKET, prop)
        self.setProperty(name, BF.PROPERTIES_LOCATION, location)
        

       
    def delete(self,bucket_name):
        name = '#$'+bucket_name
        self.redis.delete(name)
        
    def setProperty(self, bucket_name, field, value):
        name = '#$'+bucket_name
        serialized = self._dumps(value)
        self.redis.hset(name, field , serialized)
        
    def getProperty(self, bucket_name, field):
        name = '#$'+bucket_name
        serialized = self.redis.hget(name, field)
        return self._load(serialized)    
 
    def delProperty(self, bucket_name, field):
        name = '#$'+bucket_name
        self.redis.hdel(name, field)

 
    
    ##########################################################
    #     Internals
    ##########################################################   

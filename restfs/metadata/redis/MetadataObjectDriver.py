import redis
import time
import tornado.options
import restfs.factories.ObjectFactory as OF
from tornado.options import define, options
from restfs.metadata.redis.MetadataBase import MetadataBase

class MetadataObjectDriver(MetadataBase):
    '''
    Object METADATA operation
    '''
    def __init__(self, ip='localhost', port=6379, db=1, password=None):
        define("metadata_object_db",     default="6"         , help="Redis DB for Object Metadata",)
        define("metadata_object_host",   default="localhost" , help="Redis DB host for Object Metadata",)
        define("metadata_object_port",   default=6379      , help="Redis DB port for Object Metadata", type=int)
        define("metadata_object_passwd", default=None        , help="Redis DB password for Object Metadata",)
        
        #READ Options
        tornado.options.parse_command_line()
        self.redis = redis.StrictRedis(host=options.metadata_object_host, port=options.metadata_object_port, db=options.metadata_object_db,password=options.metadata_object_passwd)


    
    ##########################################################################
    # Object METADATA
    ##########################################################################    
    def getKeyList(self, prefix):
        """
        Object LIST present in the specific Object Dir
        """ 
        return self.redis.keys(prefix+"*")
    
    
    def create(self, idObject, prop, acl=None):
        '''
        CREATE OBJECT
        '''
        self.setProperty(idObject,OF.PROPERTY_OBJECT, prop)
        if acl:
            self.setProperty(idObject,OF.PROPERTY_ACL, acl)
        
    def remove(self,idObject):
        '''
        DELETE OBJECT
        '''   
        self.redis.delete(idObject)
    
    def setProperty(self,idObject,field,value):
        self.redis.hset(idObject, field , self._dumps(value))
        
    def getProperty(self,idObject,field):
        print'get properti', idObject, field
        serialized= self.redis.hget( idObject, field)
        print'serialized', serialized
        return self._load(serialized)
    
    def delProperty(self, bucket_name, field):
        name = '#$'+bucket_name
        self.redis.hdel(name, field)
            
    def setSegment(self,key, pos, seg):
        field = 'segment-'+str(pos)
        seg['vers'] = time.time()
        serialized = self._dumps(seg)
        self.redis.hset(key, field , serialized)
    
    
    def getPropertiesList(self, idObject):
        return self.redis.hkeys(idObject)
        
    def getSegment(self,key, pos):
        serialized= self.redis.hget(key, 'segment-'+str(pos))
        #FIXME
        seg = ''
        try : 
            seg = self._load(serialized)
        except:
            pass
        
        #Not Found
        if not seg:
            seg = dict()
            
        return seg
    
    def getSegments(self,key,number_of_segments):
        list_seg = []
        for elem in range(number_of_segments):
            seg = self.getObjectSegment(key,elem)
            list_seg.append(dict(seg))
        return list_seg
            
    def delSegment(self, objId,segId):
        self.redis.hdel(objId, 'segment-'+str(segId))


import logging
import os.path
import tornado.options
import bisect
import hashlib
import datetime
import types

from tornado import web
from tornado.options import define, options

from restfs.objects.BucketProperties import BucketProperties
from restfs.objects.Grant import Grant
from restfs.objects.ACP import ACP
from restfs.objects.BucketLogging import BucketLogging
from restfs.objects.BucketVersioning import BucketVersioning
from restfs.objects.BucketWebsite import BucketWebsite
from restfs.objects.BucketLocation import BucketLocation
from restfs.objects.ObjectProperties import ObjectProperties
from restfs.objects.ObjectQueryResult import ObjectQueryResult
from  restfs.utils.XmlUtils import DictToXml, XmlToDict, DataToXML



class MetadataDriver(object):
    _ROOT_PATH      = "/tmp/s3"
    _BUCKET_PATH    = "/tmp/s3/bucket"
    _BUCKET_DEPTH   = 0
    
    def __init__(self):

        if not hasattr(options ,"root_path"):
            define("root_path"   , default="/tmp/bucket" , help="storage direcotry")
            
        if not hasattr(options ,"bucket_depth"):
            define("bucket_depth", default=0  , help="object distribution inside of bucket", type=int)
            
    
        #Read Command line with new definitions
        tornado.options.parse_config_file(options.conf) 
          
        self._ROOT_PATH     = os.path.abspath(options.root_path)
        self._BUCKET_PATH   = os.path.abspath(os.path.join(self._ROOT_PATH,'buckets'))
        self._BUCKET_DEPTH  = options.bucket_depth
       
                
        if not os.path.exists(self._ROOT_PATH):
            logging.debug('Create root direcoty %s' % self._ROOT_PATH)
            os.makedirs(self._ROOT_PATH)

        if not os.path.exists(self._BUCKET_PATH):
            logging.debug('Create bucket direcoty %s' % self._BUCKET_PATH)
            os.makedirs(self._BUCKET_PATH)
        
 
    ##########################################################################
    # Bucket METADATA
    ##########################################################################
    def setBucketProperties(self, name, prop):
        bucket_path = self._getBucketPath(name)
        path = os.path.join(bucket_path,"global.xml")
        myDict = {}
        myDict['Global'] = prop.getDict()
        xml = DictToXml(myDict)
        self._writeFile(path,xml.getXml())
        

   
    def getBucketProperties(self,name):

        bucket_path = self._getBucketPath(name)
        path = os.path.join(bucket_path,"global.xml")
        
        if not os.path.exists(path):
            return None
        data = self._readFile(path) 
        myDict = XmlToDict(data).getDict()
        prop = BucketProperties()
        prop.setByDict(myDict['Global'])
        return prop
        
        
    def removeBucketProperties(self,name):
        bucket_path = self._getBucketPath(name)
        path = os.path.join(bucket_path,"global.xml")
        os.unlink(path)
        
    def removeBucketWebsite(self,name):
        bucket_path = self._getBucketPath(name)
        path = os.path.join(bucket_path,"website.xml")
        os.unlink(path)

        
 
    def setBucketACP(self, name , acp):
        bucket_path = self._getBucketPath(name)
        path = os.path.join(bucket_path,"acp.xml")
        myDict = {}
        myDict['ACP'] = {}
        myDict['ACP']['Grant'] = []
        for grant in acp.getGrantList():
            myDict['ACP']['Grant'].append(grant.getDict())

        xml = DictToXml(myDict)
        self._writeFile(path,xml.getXml())

        
     
    def getBucketACP(self,bucket_name):
        
        
        bucket_path = self._getBucketPath(bucket_name)
        path = os.path.join(bucket_path,"acp.xml")
        if not os.path.exists(path):
            return None
        data = self._readFile(path)
        myDict = XmlToDict(data).getDict()
      
        list = myDict['ACP']['Grant']
        
        grantList = []
        
        if not type(list) is types.DictType:
            for grantDict in list:
                grant = Grant()
                grant.setByDict(grantDict)
                grantList.append(grant)
        else:
                grant = Grant()
                grant.setByDict(list) 
                grantList.append(grant)
        
        acp = ACP()
        acp.setByGrants(grantList)
        return acp
    
     
    def getBucketLogging(self,bucket_name):
        
        bucket_path = self._getBucketPath(bucket_name)
        path = os.path.join(bucket_path,"logging.xml")
        if not os.path.exists(path):
            return None
        data = self._readFile(path)
        myDict = XmlToDict(data).getDict()
      
        list = myDict['Logging']['Grant']
        
        grantList = []
        
        if not type(list) is types.DictType:
            for grantDict in list:
                grant = Grant()
                grant.setByDict(grantDict)
                grantList.append(grant)
        else:
                grant = Grant()
                grant.setByDict(list) 
                grantList.append(grant)
        
        log = BucketLogging()
        log.setByGrants(grantList)
        return log
    
 
    def setBucketLogging(self, name , log, target, prefix):
        bucket_path = self._getBucketPath(name)
        path = os.path.join(bucket_path,"logging.xml")
        myDict = {}
        myDict['Logging'] = {}
        myDict['Logging']['TargetBucket'] = target
        myDict['Logging']['TargetPrefix'] = prefix
        myDict['Logging']['TargetGrants'] = {}
        for grant in log.getGrantList():
            myDict['Logging']['TargetGrants']['Grant'].append(grant.getDict())

        xml = DictToXml(myDict)
        self._writeFile(path,xml.getXml())
    
    def setBucketVersioning(self, name , v):
        bucket_path = self._getBucketPath(name)
        path = os.path.join(bucket_path,"versioning.xml")
        myDict = {}
        myDict['VersioningConfiguration'] = v.getDict()
        xml = DictToXml(myDict)
        self._writeFile(path,xml.getXml())

    
 
    def getBucketVersioning(self,name):
        bucket_path = self._getBucketPath(name)
        path = os.path.join(bucket_path,"versioning.xml")
        if not os.path.exists(path):
            return None
        data = self._readFile(path)
        myDict = XmlToDict(data).getDict()
        
        ver = BucketVersioning()
        ver.setByDict(myDict['VersioningConfiguration'])
        return ver  
    
        
    def setBucketWebsite(self, name , w):
        bucket_path = self._getBucketPath(name)
        path = os.path.join(bucket_path,"website.xml")
        myDict = {}
        myDict['WebsiteConfiguration'] = w.getDict()
        xml = DictToXml(myDict)
        self._writeFile(path,xml.getXml())
    
    def getBucketWebsite(self,name):
        bucket_path = self._getBucketPath(name)
        path = os.path.join(bucket_path,"website.xml")
        if not os.path.exists(path):
            return None
        data = self._readFile(path)
         
        myDict = XmlToDict(data).getDict()
        web = BucketWebsite()
        web.setByDict(myDict['WebsiteConfiguration'])
        return web  
  
    def setBucketLocation(self, name,  location):
        bucket_path = self._getBucketPath(name)
        path = os.path.join(bucket_path,"location.xml")
        myDict = {}
        myDict['Location'] = location
        xml = DictToXml(myDict)
        self._writeFile(path,xml.getXml())

    
    def getBucketLocation(self,name):
        bucket_path = self._getBucketPath(name)
        path = os.path.join(bucket_path,"location.xml")
        
        if not os.path.exists(bucket_path):
            return None
        
        data = self._readFile(path) 
        myDict = XmlToDict(data).getDict()
        loc = BucketLocation()
        loc.set(myDict['Location'])
        return loc  
    
 
 
 
    ##########################################################################
    # Object METADATA
    ########################################################################## 
    """
    Object LIST present in the specific Bucket
    
    """ 
    def getObjectList(self,bucket_name,prefix,marker,max_keys,terse=0):
         
        path = self._getBucketPath(bucket_name)
       
        if not os.path.isdir(path):
            raise web.HTTPError(404)
        
        #FIXME
        object_names = []
        dirs = os.listdir(path)
        for dir in dirs:
            new_path = os.path.join(path,dir)
            if os.path.isdir(new_path):
                self._findObjects(new_path, object_names)
                
        skip = len(path) + 1
        for i in range(self._BUCKET_DEPTH):
            skip += 2 * (i + 1) + 1 
        object_names = [n[skip:] for n in object_names]
        object_names.sort()
        
        contents = []

        #FIXME move filter on file scan
        start_pos = 0
        if marker:
            start_pos = bisect.bisect_right(object_names, marker, start_pos)
        if prefix:
            start_pos = bisect.bisect_left(object_names, prefix, start_pos)

        truncated = False
        for object_name in object_names[start_pos:]:
            if not object_name.startswith(prefix):
                break
            if len(contents) >= max_keys:
                truncated = True
                break
            
            obj = ObjectProperties(bucket_name=bucket_name,object_name=object_name)
            
            if not terse:
                obj =  self.getObjectProperties(bucket_name,object_name)
                
            contents.append(obj)
            marker = object_name
        
        res = ObjectQueryResult(bucket_name, prefix,marker,max_keys,truncated)
        res.setObjectList(contents)
        return res
 
 
    def getObjectProperties(self,bucket_name,object_name):
        object_path = self._getObjectPath(bucket_name,object_name)
        path = os.path.join(object_path,"object.xml")
        
        if not os.path.exists(path):
            return None
        
        data = self._readFile(path) 
        myDict = XmlToDict(data).getDict()
        prop = ObjectProperties()
        prop.setByDict(myDict['Global'])
        return prop
        
    
    def setObjectProperties(self, bucket_name, object_name, prop):
        object_path = self._getObjectPath(bucket_name,object_name)
        path = os.path.join(object_path,"object.xml")
        myDict = {}
        myDict['Global'] = prop.getDict()
        xml = DictToXml(myDict)
        self._writeFile(path,xml.getXml())
    
    
    
    def getObjectACP(self,bucket_name,object_name):
        object_path = self._getObjectPath(bucket_name,object_name)
        path = os.path.join(object_path,"acp.xml")
        if not os.path.exists(path):
            return None
        data = self._readFile(path)

        myDict = XmlToDict(data).getDict()
      
        list = myDict['ACP']['Grant']
        
        grantList = []
        
        if not type(list) is types.DictType:
            for grantDict in list:
                grant = Grant()
                grant.setByDict(grantDict)
                grantList.append(grant)
        else:
                grant = Grant()
                grant.setByDict(list) 
                grantList.append(grant)
        
        acp = ACP()
        acp.setByGrants(grantList)
        return acp

    
 
    def setObjectACP(self, bucket_name , object_name, acp):
        object_path = self._getObjectPath(bucket_name, object_name)
        path = os.path.join(object_path,"acp.xml")
        myDict = {}
        myDict['ACP'] = {}
        myDict['ACP']['Grant'] = []
        for grant in acp.getGrantList():
            myDict['ACP']['Grant'].append(grant.getDict())

        xml = DictToXml(myDict)
        self._writeFile(path,xml.getXml())

 
        
    def deleteObject(self, bucket_name,object_name):
        bucket_path = self._getObjectPath(bucket_name,object_name)
        path = os.path.join(bucket_path,"object.xml")
        os.unlink(path)
        
 
    #
    # INTERNALS
    ######################################################################
    

              
    def _getBucketPath(self,bucket_name):
        path = os.path.abspath(os.path.join(self._BUCKET_PATH,
                                           bucket_name))
        if not path.startswith(self._BUCKET_PATH) :
            raise web.HTTPError(404)
        
        return path
    
    
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
    
    
    def _getObjectPath(self,bucket_name,object_name):
        
        if self._BUCKET_DEPTH < 1:
            path = os.path.abspath(os.path.join(
                   self._BUCKET_PATH, bucket_name, object_name))
        else:
            #Multi Level storage
            hash = hashlib.md5(object_name).hexdigest()
            path = os.path.abspath(os.path.join(
                  self._BUCKET_PATH, bucket_name))
            for i in range(self.BUCKET_DEPTH):
                path = os.path.join(path, hash[:2 * (i + 1)])
        
        if not path.startswith(self._BUCKET_PATH) :
            raise web.HTTPError(404)
        
        return path
    
    
    def _readFile(self,path):
      
        FD = open(path, "r")
        data = FD.read()
        FD.close()
        return data
       
     
    
    def _writeFile(self,path,data):    
        FD = open(path, "wrb")
        FD.write(data)
        FD.close()
     
 

     
 

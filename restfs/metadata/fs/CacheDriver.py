import os 
import logging
import time
import os.path
import tornado.options
#import restfs.backends.fs.fsUtil as fsUtil
from tornado import web
from tornado.options import define, options
#from  restfs.backends.fs.FsObject import FsObject

class CacheDriver(object):
   
    _bucket_acl   = {}
    _policy       = {}
    _location     = {}
    _versioning   = {}
    _logging      = {}
    _notification = {}
    
    def __init__(self):

        self._BUCKET_ACL_SIZE = 1000
        self._POLICY_SIZE = 1000
        self._LOCATION_SIZE = 1000
        self._VERSIONING_SIZE = 1000
        self._LOGGING_SIZE = 1000
        self._NOTIFICATION_SIZE = 1000
        
        self._TTL_BUCKET_ACL = 60
        self._TTL_POLICY = 60
        self._TTL_LOCATION = 60
        self._TTL_VERSIONING = 60 
        self._TTL_LOGGING = 60
        self._TTL_NOTIFICATION = 60     
      
    def setCache(self,type,key,value):

        if type == "acl":
            if len(self._bucket_acl) > self._BUCKET_ACL_SIZE:
                self._bucket_acl.popitem()
            time = time.time()
            self._bucket_acl[key] = {'value':value, 'cdate':time}

        elif type == "policy":
            if len(self._policy) > self._POLICYL_SIZE:
                self._policy.popitem()
            time = time.time()
            self._policy[key] = {'value':value, 'cdate':time}
            
        elif type == "location":
            if len(self._bucket_acl) > self._LOCATION_SIZE:
                self._location.popitem()
            time = time.time()
            self._location[key] = {'value':value, 'cdate':time}

        elif type == "versioning":
            if len(self._versioning) > self._VERSIONING_SIZE:
                self._versioning.popitem()
            time = time.time()
            self._versioning[key] = {'value':value, 'cdate':time}

        elif type == "logging":
            if len(self._logging) > self._LOGGING_SIZE:
                self._logging.popitem()
            time = time.time()
            self._logging[key] = {'value':value, 'cdate':time}

        elif type == "notification":
            if len(self._notification) > self._NOTIFICATION_SIZE:
                self._notification.popitem()
            time = time.time()
            self._notification[key] = {'value':value, 'cdate':time}
        else :
            print('type {0} not found in cache'.format(type))
           
    #key = bucket_name 
    def getCacheItem(self,key):
        
        if key in self._bucket_acl:
            if int(time.time()-self._bucket_acl[key]['cdate']) <= self.TTL_BUCKET_ACL:
                return self._bucket_acl[key]
            del self._bucket_acl[key]
            
        elif key in self._policy:
            if int(time.time()-self._policy[key]['cdate']) <= self.TTL_POLICY:
                return self._policy[key]
            del self._policy[key]
            
        elif key in self._location:
            if int(time.time()-self._location[key]['cdate']) <= self.TTL_LOCATION:
                return self._location[key]
            del self._location[key]
            
        elif key in self._versioning:
            if int(time.time()-self._versioning[key]['cdate']) <= self.TTL_VERSIONING:
                return self._versioning[key]
            del self._versioning[key]
            
        elif key in self._logging:
            if int(time.time()-self._logging[key]['cdate']) <= self.TTL_LOGGING:
                return self._logging[key]
            del self._logging[key]
            
        elif key in self._notification:
            if int(time.time()-self._notification[key]['cdate']) <= self.TTL_NOTIFICATION:
                return self._notification[key]
            del self._notification[key]
        return None       
        
     
    def delCacheItem(self,key):
        if key in self._bucket_acl:
            del self._bucket_acl[key]
        elif key in self._policy:
            del self._policy[key]
        elif key in self._location:
            del self._location[key]
        elif key in self._versioning:
            del self._versioning[key]
        elif key in self._logging:
            del self._logging[key]
        elif key in self._notification:
            del self._notification[key]
        else:
            print('element {0} not in cache'.format(key))
            
            
    def cleanCache(self):
        self._bucket_acl   = {}
        self._policy       = {}
        self._location     = {}
        self._versioning   = {}
        self._logging      = {}
        self._notification = {}       
                     


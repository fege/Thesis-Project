'''
Created on 27/ott/2011

@author: max
'''
import unittest
import time
from redis import Redis

import datetime
import time


class ObjectProperties(object):
    
    def __init__(self,bucket_name="", object_name="", uid="" , last_modify="",size=0,md5="", storage_class="STANDARD", content_type=None, xattr=[], max_read = 1000, read_counter = 0):
        self.id            = ""
        self.bucket_name   = bucket_name
        self.object_name   = object_name
        self.uid           = uid
        self.last_modify   = last_modify
        self.size          = size
        self.md5           = md5 
        self.compress      = None
        self.storage_class = storage_class
        self.xattr         = xattr
        self.content_type  = content_type
        self.content_language = None
        self.expires       = None
        self.content_encoding = None
        self.vers = ""
        #ours
        self.max_read     = max_read
        self.read_counter = read_counter
        
        self.creation_date  = time.time()
        
        
    def getXattr(self):
        return self.xattr
    
    def setXattr(self, xattr):
        self.xattr = xattr
        
    def appendXattr(self,xattr):
        self.xattr.append(xattr)

    def setByDict(self,myDict):      
        for key in myDict:
            setattr(self,key,myDict[key])

        #self.last_modified = str(datetime.datetime.fromtimestamp(time.time()))
        #FIXME remove in final version
        if self.last_modify:
            self.last_modify = float(self.last_modify)
        if self.creation_date:
            self.creation_date = float(self.creation_date)
        if self.xattr == "None":
            self.xattr =  None
            
    def getDict(self):
        myDict={}
        myDict['id']            = self.id 
        myDict['bucket_name']   = self.bucket_name  
        myDict['object_name']   = self.object_name   
        myDict['last_modify']   = self.last_modify 
        myDict['size']          = self.size          
        myDict['md5']           = self.md5    
        myDict['uid']           = self.uid    
        myDict['compress']      = self.compress      
        myDict['storage_class'] = self.storage_class 
        myDict['xattr']     = self.xattr 
        myDict['content_type']   =  self.content_type
        myDict['content_language']   =  self.content_language
        myDict['expires']   =  self.expires
        myDict['content_encoding']   =  self.content_encoding  
        myDict['creation_date']   =  self.creation_date
        #ours
        myDict["max_read"] = self.max_read
        myDict["read_counter"] = int(self.read_counter) + 1


        
        return myDict

class TestObjectProperties():
    
    def __init__(self):
        self.redis = Redis('169.254.19.240')
        self.object = ObjectProperties()


    def testGet(self,key):
        Redis.get(self.redis,key)

    def testSet(self,key):
        Redis.set(self.redis,key, self.object)
    
    def testDelete(self,key):
        Redis.delete(self.redis,key)



if __name__ == "__main__":
    contatore = 0
    app = 0
    differenza = 0
    tempoInizio = time.time()
    t = TestObjectProperties()
    for x in range (2000001):
        if app == 1000000 or app == 2000000:
            contatore = 1
        if contatore == 1:
            differenza = time.time() - tempoInizio
            print 'tempo dal principio %s'%differenza
        t.testSet(x)
        app += 1
        contatore = 0
    
    
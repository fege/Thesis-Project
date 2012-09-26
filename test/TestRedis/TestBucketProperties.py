'''
Created on 27/ott/2011

@author: max
'''
import unittest
import time
import redis
import sys

import multiprocessing
from json import loads, dumps
import datetime

class BucketProperties (object):
    
    uid           = ""
    
    #Location
    region        = None
    objectCounter = 0
    
    #Notification
    notification  = 'disable'
    
    # ACP separete object
    acp           = None 
    logging       = None
    logging_prefix = ""
    
    #Versioning 
    versioning    = None # Must be Null if never activated
    versioning_mfa       = 'disable'
    
    # Website
    websiteEnabled  = False
    website_index   = ""
    website_err_key = ""
    website_endPoint = ""
    
    #Policy
    policy        = 'disable'
    
    
    def __init__(self,uid = "", reg="EU",read_counter = 0,max_objects = 100 ,max_space = 1000,\
                  dump=False, status='AVAIBLE', type='', defrag=False, check=False,\
                  max_file_size = 100):
        self.uid    = uid
        #self.acl    = acl 
        self.region = reg
        self.objectCounter = 0
        
        self.logging       = None
        self.logging_target = ""
        self.logging_prefix = ""
        self.space_used = 0 
        #ours
        self.read_counter = read_counter
        self.max_objects = max_objects
        self.creation_date = datetime.datetime.now()
        self.max_space     = max_space
        self.last_modified = ''
        self.last_dump     = ''
        self.dump          = dump
        self.check         = check
        self.last_check    = ''
        self.status        = status
        self.type          = type
        self.max_file_size = max_file_size

    
    def setByDict(self,myDict):
        for key in myDict:
            setattr(self,key,myDict[key])
        self.objectCounter = int(self.objectCounter) 
        if self.space_used:
            self.space_used = int(self.space_used)
        
        
    def getDict(self):
        myDict = {}
        myDict["uid"] = self.uid
        #myDict["acl"] = self.acl
        myDict["acp"] = self.acp #forse da far saltare
        myDict["region"] = self.region
        myDict["notification"] = self.notification
        myDict["logging"] = self.logging
        myDict["logging_target"] = self.logging_target
        myDict["logging_prefix"] = self.logging_prefix
        myDict["versioning"] = self.versioning #saltare
        myDict["versioning_mfa"] = self.versioning_mfa
        myDict["policy"] = self.policy
        myDict["objectCounter"] = self.objectCounter
        myDict["space_used"] = self.space_used
        #ours
        myDict["read_counter"] = int(self.read_counter) + 1
        myDict["max_objects"] = self.max_objects
        myDict["creation_date"] = str(self.creation_date)
        myDict["last_modified"] = self.last_modified
        myDict["max_space"] = self.max_space
        myDict["last_dump"] = self.last_dump
        myDict["last_check"] = self.last_check
        myDict["status"] = self.status
        myDict["type"] = self.type
        myDict["max_file_size"] = self.max_file_size
        
        return myDict
try:
   import cPickle as pickle
except:
   import pickle


class TestBucketProperties():
   
    def __init__(self):
       
        
        self.bucket = BucketProperties()
        self.bucket.uid = 'manfred'
        self.bucket.read_counter = 5
        self.bucket.type = 'ciao'
        self.bucket.logging_target = 'sfsfsd'
        self.bucket.logging_prefix = 'lsafsfas'
        self.bucket.last_modified = 'today'
        self.bucket.last_dump = 'today'
        self.bucket.last_check = 'today'
    

    def setData(self,obj):
        self.bucket = pickle.loads(obj)

    def getData(self):       
        return pickle.dumps(self.bucket)
    
    def getDict(self):
        return self.bucket.getDict()
    
    def setByDict(self,myDict):
        self.bucket.acp = myDict
    
class TestDictionary():
     
    def __init__(self):
        self.myDict= {"uid":100, "my":" XXXXXXXXXXXXXXXXXXXXXX  "}

    def setData(self,obj):
        self.myDict= obj

    def getData(self):       
        return self.myDict
    

        
        

def workerADD(id,keyNum, t):
        r = redis.StrictRedis(host='169.254.19.240', port=6379, db=0)
        r.flushall()
        
        tempoInizio = time.time()
        key = keyNum*id*1000
   
        
        for n in range (0,keyNum):
            for x in range (1000):
                obj = None
                if t == "o":
                    obj = TestBucketProperties()
                    r.set(key, obj.getData())
                elif t == "v":
                    obj = TestBucketProperties()
                    r.hmset(key, obj.getDict())
                elif t == "d":
                    obj = TestDictionary()
                    r.hmset(key, obj.getData())
                    
                elif t == "j":
                    obj = TestDictionary()
                    r.set(key, dumps(obj.getData()))

                else:
                    obj = TestDictionary()
                    r.set(key, obj.getData())
            
                key = key +1
                
            
            differenza = time.time() - tempoInizio
            print 'id,%s,%s,%s' % (id,differenza, key)
            
            
def workerGET(id,keyNum,t):
        r = redis.StrictRedis(host='169.254.19.240', port=6379, db=0)
        tempoInizio = time.time()
        key = keyNum*id*1000
        tempoInizio = time.time()
        for n in range (0,keyNum):
            for x in range (1000):
                obj = None
                if t == "o":
                    obj = TestBucketProperties()
                    obj.setData(r.get(key))
                elif t == "v":
                    obj = TestBucketProperties()
                    obj.setByDict(r.hgetall(key))
                elif t == "d":
                    obj = TestDictionary()
                    obj.setData(r.hgetall(key))
                elif t == "j":
                    obj = TestDictionary()
                    obj.setData(loads(r.get(key)))
                else:
                    obj = TestDictionary()
                    obj.setData(r.get(key))
                key = key +1
            
            differenza = time.time() - tempoInizio
            print 'id,%s,%s,%s' % (id,differenza, key)


def workerDEL(id,keyNum):
        r = redis.StrictRedis(host='169.254.19.240', port=6379, db=0)
        
        t = TestBucketProperties()
        tempoInizio = time.time()
        key = keyNum*id*1000
        
        for n in range (0,keyNum):
            for x in range (1000):
                r.delete(key)
                key = key +1
            
            differenza = time.time() - tempoInizio
            print 'id,%s,%s,%s' % (id,differenza, key)


if __name__ == "__main__":
    
    if len(sys.argv) == 0:
        print "XXX number_of_proc number_of_entry_Key_per_Thread(1=1k,2=2k) object type"
        print "o= object serilize, s=string , d =dictionary, v=object->to->dict"
        exit()
    obj = None
 
    proc = int(sys.argv[1])
    keyNum = int(sys.argv[2])
    t = sys.argv[3]
    
   

    jobs = []
    print "---------------------------ADD--------------------------------"
    for i in range(proc):
        p = multiprocessing.Process(target=workerADD, args=(i,keyNum,t,))
        jobs.append(p)
        p.start()
    for j in jobs:
        j.join()
        
        
    jobs = []
    print "---------------------------READ--------------------------------"
    for i in range(proc):
        p = multiprocessing.Process(target=workerGET, args=(i,keyNum,t,))
        ## Vedi esempio thread federico
        ##
        ## fare una cosa con il modulo Queue
        ##
        ##
        ##
        jobs.append(p)
        p.start()
    for j in jobs:
        j.join()
        
        
        
    print "----------------------------DELETE----------------------------"
    jobs = []
    for i in range(proc):
        p = multiprocessing.Process(target=workerDEL, args=(i,keyNum,))
        jobs.append(p)
        p.start()
    for j in jobs:
        j.join()
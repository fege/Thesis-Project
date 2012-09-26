import restfs.factories.BucketFactory as BucketFactory
import datetime

class BucketProperties (object):
    
    uid           = ""
    #Location
    region        = None
    objectCounter = 0
    
    #Notification
    notification  = BucketFactory.NOTIFY_DISABLE
    
    # ACP separete object
    acp           = None 
    logging       = None
    logging_prefix = ""
    
    #Versioning 
    versioning    = None # Must be Null if never activated
    versioning_mfa       = BucketFactory.VER_MFA_DISABLE
    
    # Website
    websiteEnabled  = False
    website_index   = ""
    website_err_key = ""
    website_endPoint = ""
    
    #Policy
    policy        = BucketFactory.POLICY_DISABLE
    
    
    def __init__(self,uid = "", bucket_name="", reg="EU",read_counter = 0,max_objects = 100 ,max_space = 1000,\
                  dump=False, status='AVAIBLE', type='', defrag=False, check=False,\
                  max_file_size = 100, gid = ''):
        self.uid    = uid
        self.gid    = gid
        self.bucket_name = bucket_name
        #self.acl    = acl 
        self.region = reg
        self.objectCounter = 0
        self.blocks_number = 0
        
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
        myDict["gid"] = self.gid
        myDict["bucket_name"] = self.bucket_name
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
        myDict["creation_date"] = self.creation_date
        myDict["last_modified"] = self.last_modified
        myDict["max_space"] = self.max_space
        myDict["last_dump"] = self.last_dump
        myDict["last_check"] = self.last_check
        myDict["status"] = self.status
        myDict["type"] = self.type
        myDict["max_file_size"] = self.max_file_size
        
        return myDict
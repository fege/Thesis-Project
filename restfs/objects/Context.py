class Context(object):
    def __init__(self, uid, server, processId=-1, session='', groupId=[],token=''):
        self.uid           = uid
        self.server        = server
        self.gid       = groupId
        self.session       = session
        self.security     = ""
        self.remotePeer    = ""
        self.proxy         = ""
        self.token         = ""
   
        ''' def __init__(self, user, bucket_name, session='', groupId=[]):
        self.user          = user
        self.bucket_name   = bucket_name
        self.session       = session
        self.groupId       = groupId
    
    def setByDict(self, user, bucket_name, session, groupId=[]): 
        self.user          = user
        self.bucket_name   = bucket_name
        self.session       = session
        self.groupId       = groupId  
            
            
    def getDict(self):
        myDict={}
        myDict['user']          = self.user 
        myDict['bucket_name']   = self.bucket_name 
        myDict['session']       = self.session  
        myDict['groupId']       = self.groupId
        
        return myDict'''
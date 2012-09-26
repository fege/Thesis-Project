class Grant(object):
    
    
    def __init__(self,type = None, uid=None, perm={}):
        self.entity_type = type
        self.uid         = uid
        self.permission  = perm
        self.allow       = True
        
    def setByDict(self,myDict):
        for key in myDict:
            setattr(self,key,myDict[key])
      
        
    def getDict(self):
        myDict = {}
        myDict["entity_type"] = self.entity_type
        myDict["uid"] = self.uid
        myDict["permission"] = self.permission
        
        return myDict
    
    def __repr__(self):
        return "<Grant(%s, %s, %s)>" % (self.entity_type, self.uid, self.permission)
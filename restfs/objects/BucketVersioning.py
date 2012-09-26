class BucketVersioning(object):
    
    def __init__(self):
        self.status = ''
        self.MfaDelete = ''

    
    def getDict(self):
        myDict = {}
        myDict["status"] = self.status
        myDict["MfaDelete"] = self.MfaDelete
        return myDict
        
    def setByDict(self,myDict): 
        for key in myDict:
            setattr(self,key,myDict[key])
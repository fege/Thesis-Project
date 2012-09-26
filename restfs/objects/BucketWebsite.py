class BucketWebsite(object):
    # versioning['status'] = Stato
    
    
    def __init__(self):
        self.indexSuffix = ''
        self.errorKey = ''
            
    
    def getDict(self):
        myDict = {}
        myDict["indexSuffix"] = self.indexSuffix
        myDict["errorKey"] = self.errorKey
        return myDict
        
    def setByDict(self,myDict):      
        for key in myDict:
            setattr(self,key,myDict[key])
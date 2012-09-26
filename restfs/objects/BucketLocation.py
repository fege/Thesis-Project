import restfs.factories.BucketFactory as BucketFactory

class BucketLocation (object):
    
    location       = ""
    alghortim = BucketFactory.DISTRIBUTION_BASE
    
    def __init__(self, loc="EU"):
        self.location = loc
        self.alghortim = BucketFactory.DISTRIBUTION_BASE
        
    def getDict(self):
        myDict = {}
        myDict["location"] = self.location
        
        return myDict()
        
    def set(self,location):      
        self.location = location

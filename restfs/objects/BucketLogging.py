

class BucketLogging(object):

    userGrant = {}
    status = "ENABLE"
    
    def __init__(self, target, prefix):
        self.userGrant = {}
        self.target = target
        self.prefix = prefix
    
    def setByGrants(self,grantList):
        
        for grant in grantList:
            self.userGrant[grant.uid] = {}
            self.userGrant[grant.uid][grant.permission] = grant
            
            
    def getGrantList(self):
        grantList = []
        for value in self.userGrant.itervalues():
            for grant in value.itervalues():
                grantList.append(grant)
        
        return grantList

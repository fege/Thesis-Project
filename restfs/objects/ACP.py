class ACP(object):
    # userGrant['iduseer']['perm'] = Grant
    userGrant = {}
    grantList = []
    
    def __init__(self):
        self.userGrant = {}
        self.grantList = []
    
    def setByGrants(self,grantList):
        self.grantList = grantList    
        for grant in grantList:
            self.userGrant[grant.uid] = {}
            self.userGrant[grant.uid] = grant.permission
            
            
            
    def getGrantList(self):
        
        return self.grantList
    
    def grantee(self,userUid,op):
        
        groups = [userUid,'@anonymous','@authenticated']
        #name.append(user.getGroups())
       
        #Global Permission
        for name in groups:
            perm = self._checkPerm(name,op)
            if perm:
                return True
            elif perm == False:
                return False
            
        return False
        
    
    
    def _checkPerm(self,name, op):
        # Single permission
        if not self.userGrant.has_key(name):
            return None
                
        return self.userGrant[name].get(op,None)
        
      
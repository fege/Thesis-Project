class User(object):
        id          = 0
        uid           = "anonymous"
        display_name = "anonymous"
        email        = ""
        _PASSWORD    = ""
        max_buckets  = ""
        max_objects  = ""
    
       
        def __init__(self,id,uid,display_name,email,password,max_buckets,max_objects):
            self.uid          = uid
            self.id           = int(id)
            self.display_name = display_name.strip()
            self.email        = email.strip()
            self._PASSWORD    = password.strip()
            self.max_buckets  = max_buckets
            self.max_objects  = max_objects
            
        
        def getPassword(self):
            return self._PASSWORD
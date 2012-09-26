import logging
from restfs.utils import s3util
from tornado.options import  options

class AuthManager(object):
    
    _LOGGER = logging.getLogger('AUTH_MANAGER')
    
    def __init__(self):
        #Bucket AUTH User
        auth_plugin = "restfs.auth.%s.AuthDriver" % options.auth_driver
        auth_mod = __import__(auth_plugin, globals(), locals(), ['AuthDriver'])
        AuthDriver = getattr(auth_mod, 'AuthDriver')
        self.authUser = AuthDriver()
        
          
    def getUser(self,user_name):
        return self.authUser.getUser(user_name)
        
              
              
    def checkS3Auth(self,user_id,signature,string_to_sign):
        res  = None
        user = self.getUser(user_id)
        print user.getPassword(),signature
        if user:
            print s3util.signString(user.getPassword(),string_to_sign)
            if s3util.signString(user.getPassword(),string_to_sign) == signature:
                res = user
            
        return res
             
        

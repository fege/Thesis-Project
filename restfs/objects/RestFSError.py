from tornado import web

class RestFSError(web.HTTPError):
    
    def __init__(self, key, msg, http, stack=None):
        web.HTTPError.__init__(self,http,msg)
        self.stack = stack
        self.key  = key
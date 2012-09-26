

class JsonRPCError(Exception):
    _version = "2.0"
    
    def __init__(self, code=-32000, message='Server error', rpcid=None):
        self.errorCode   = code
        self.errorString = message
        self.rpcid       = rpcid

    def getPayload(self):
        return {'code':self.errorCode, 'message':self.errorString}

    def response(self, rpcid=None, version=None):
        if not version:
            version = self._version
        if rpcid:
            self.rpcid = rpcid

    
    def __str__(self):
        return repr(self.errorString)
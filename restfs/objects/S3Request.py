from restfs.factories.HeaderFactory   import AWS

class S3Request(object):
    #0 Anonymous
    id   = None
    signature = None
  
    _AUTH_S3_HEADERS   = None
    _HTTP_METHOD = None
    RESOURCE     = None
    SUBRESOURCE  = None
    BUCKET       = None
    OBJECT       = None
    ID_REQUEST   = None


    def __init__(self, id , signature, method, s3headers, path, bucket, object, subResource, id_request):
        self.ID_REQUEST
        self._HTTP_METHOD = method
        self._AUTH_S3_HEADERS   = s3headers
        self.RESOURCE     = path
        self.SUBRESOURCE  = subResource
        self.id           = id
        self.signature    = signature
        self.BUCKET       = bucket
        self.OBJECT       = object
        self.ID_REQUEST   = id_request
        
    def setUserAuth(self, id, signature):
        self.id_user    = id 
        self.signature  = signature 
           
    def getStringToSign(self):
        s3HeaderKeys = self._AUTH_S3_HEADERS.keys()
        s3HeaderKeys.sort()

        buf = "%s\n" % self._HTTP_METHOD
        for key in s3HeaderKeys:
            if key.startswith(AWS.HEADER_PREFIX):
                buf += "%s:%s\n" % (key, self._AUTH_S3_HEADERS[key])
            else:
                buf += "%s\n" % self._AUTH_S3_HEADERS[key]   
        buf += "%s" % self.RESOURCE 
        
        #s3fs problem
        if self.RESOURCE.count("/") < 2 and len(self.RESOURCE) > 1:
            buf += "/"
        
        subResourceKeys = self.SUBRESOURCE.keys()
        subResourceKeys.sort()
        
        sub = "?"
        for key in subResourceKeys:
            if sub == "?":
                sub += "%s"  % self.SUBRESOURCE[key] 
            else:
                sub += "&%s" % self.SUBRESOURCE[key]
            
        if sub != "?":
            buf += sub
        
        return buf
    
    
    def getMetaData(self):
        meta = dict()
        for key in self._AUTH_S3_HEADERS:
           
            if  key.startswith(AWS.METADATA_PREFIX):
                meta[key] = self._AUTH_S3_HEADERS[key]
        
        return meta
    
    def getACL(self):
        return self._AUTH_S3_HEADERS.get(AWS.ACL_PREFIX,None)
    
  
    def getStorage(self):
        return self._AUTH_S3_HEADERS[AWS.STORAGE_PREFIX]  
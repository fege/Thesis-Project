class S3Response(object):
    
    HTTP_CODE = None
    RES_EL    = None
    RES_TYPE  = None
    
    def __init__(self, code, el, res):
        self.HTTP_CODE = code
        self.RES_EL    = el
        self.RES_TYPE  = res
        
		
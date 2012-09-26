

err={}

err['LimitBucketsReached']={"code":"LimitBucketsReached",  \
                         "message":"You have reached the limit of buckets you are permitted to create. Please try to delete one before and retry this operation.",\
                         "http":403,}
err['BucketNotEmpty']={"code":"BucketNotEmpty",  \
                         "message":"The bucket you tried to delete is not empty.",\
                         "http":409,}
err['BucketAlreadyExists']={"code":"BucketAlreadyExists",  \
                         "message":"The requested bucket name is not available. The bucket namespace is shared by all users of the system. Please select a different name and try again.",\
                         "http":409,}
err['IllegalVersioningConfigurationException']={"code":"IllegalVersioningConfigurationException",  \
                         "message":"Indicates that the Versioning configuration specified in the request is invalid.",\
                         "http":400,}
err['InvalidBucketName']={"code":"InvalidBucketName",  \
                         "message":"You have specified an invalid name for the bucket. Please try to insert another name.",\
                         "http":400,}
err['BucketNotFound']={"code":"BucketNotFound",  \
                         "message":"The name of the bucket you have entered was not found.",\
                         "http":404,}
err['InvalidPolicyDocument']={"code":"InvalidPolicyDocument",  \
                         "message":"The content of the form does not meet the conditions specified in the policy document.",\
                         "http":400,}
err['MalformedACLError']={"code":"MalformedACLError",  \
                         "message":"The XML you provided was not well-formed or did not validate against our published schema.",\
                         "http":400,}
err['InvalidVersion']={"code":"InvalidVersion",  \
                         "message":"The Version specified does not match an existing version.",\
                         "http":404,}
err['RegionNotFound']={"code":"RegionNotFound",  \
                         "message":"The region name that you have specified for your bucket does not exist. Please try to insert another one.",\
                         "http":404,}
err['ObjectNotFound']={"code":"ObjectNotFound",  \
                         "message":"The object name that you have specified does not exist or he is a directory and not a root. Please try to insert another one.",\
                         "http":404,}
err['PathNotExist']={"code":"PathNotExist",  \
                         "message":"Sorry but this path does not exist. Please check the path you have entered and try again.",\
                         "http":404,}
err['InternalServerError']={"code":"InternalServerError",  \
                         "message":"Sorry but something goes wrong. The server failed to fulfill a request.",\
                         "http":500,}
err['AuthorizationDeny']={"code":"AuthorizationDeny",  \
                         "message":"You are not authorized to do this action.",\
                         "http":401,}
err['LoginFailed']={"code":"LoginFailed",  \
                         "message":"You login is failed, maybe you have entered an incorrect username or password, please try again.",\
                         "http":401,}
err['InvalidURI']={"code":"InvalidURI",  \
                         "message":"Sorry but the URI specified is invalid and couldn't be parsed.",\
                         "http":400,}        
err['MalformedACLError']={"code":"MalformedACLError",  \
                         "message":"The XML you provided was not well-formed or did not validate against our published schema.",\
                         "http":400,}        

err['NotImplemented']={"code":"NotImplemented",  \
                         "message":"A header you provided implies functionality that is not implemented.",\
                         "http":501,}

     
    




class AWS(object):
    
    DEFAULT_HOST    = 'restfs.beolink.org'
    METADATA_PREFIX = 'x-amz-meta-'
    ACL_PREFIX      = 'x-amz-acl'
    STORAGE_PREFIX  = 'x-amz-storage-class'
    HEADER_PREFIX   = 'x-amz-'
    SUB_RESOURCE    = ["acl", "location", "logging", "notification", "partNumber", "policy", "requestPayment", "torrent", "uploadId", "uploads", "versionId", "versioning", "versions"]
    AUTH_S3_HEADER  = ['content-md5', 'content-type', 'date']
    HEADER_S3       = ['content-language','expires','cache-control','content-disposition', 'content-encoding']
    
 

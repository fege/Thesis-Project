import logging
import restfs.factories.S3ACPFactory as S3
import restfs.factories.RestFSACPFactory as RestFS
import restfs.factories.ErrorCodeFacotry  as errCode
from restfs.objects.RestFSError import RestFSError

class S3BaseService():
    
    _LOGGER = logging.getLogger('STORAGE_BASE_SERVICE')
    def __init__(self,resourceMng, metadataMng, storageMng):
        self.res      = resourceMng
        self.meta     = metadataMng
        self.storage  = storageMng
    
    def _checkBucketExist(self,bucket_name):
        self._LOGGER.info("Check if the bucket exists")
        prop = self.meta.getBucketProperty(bucket_name)
        if not prop :
            self._LOGGER.warning('The bucket does not exist')
            raise RestFSError(errCode.err['BucketNotFound']['code'],\
                              errCode.err['BucketNotFound']['message'],\
                              errCode.err['BucketNotFound']['http'])
        return prop
    
    def _checkBucketOwner(self,user, prop):
        self._LOGGER.info("Check if the user is the bucket owner")
        if prop.uid != user.uid  :
            self._LOGGER.warning('The user is not the bucket owner')
            raise RestFSError(errCode.err['AuthorizationDeny']['code'],\
                              errCode.err['AuthorizationDeny']['message'],\
                              errCode.err['AuthorizationDeny']['http']) 
        return 
    
    
    def _checkBucketPerm(self,bucket_name, user, prop, op):
        self._LOGGER.info("Check if the user has permission on the bucket")
        self._LOGGER.debug('Permission-type : %s' % op) 
        if user.uid == prop.uid:
            return True
        #Permission deny no acp  
        elif prop.acp:
            acp =  self.meta.getBucketACP(bucket_name)
            if acp.grantee(user,op):
                return True
            
        self._LOGGER.warning('The user cannot do this operation the bucket')
        raise RestFSError(errCode.err['AuthorizationDeny']['code'],\
                              errCode.err['AuthorizationDeny']['message'],\
                              errCode.err['AuthorizationDeny']['http'])         
            
    
    def _checkObjectExist(self, idObject):
        self._LOGGER.info("Check if the object exists")
        obj = self.meta.getObjectProperties(idObject)
        if not obj :
            self._LOGGER.warning('The object does not exist')
            raise RestFSError(errCode.err['BucketNotFound']['code'],\
                              errCode.err['BucketNotFound']['message'],\
                              errCode.err['BucketNotFound']['http'])
        return obj
    
    
    def _checkOwner(self,user, prop):
        self._LOGGER.info("Check if the user is the object owner")
        if prop.uid != user.uid  :
            self._LOGGER.warning('The user is not the object owner')
            raise RestFSError(errCode.err['AuthorizationDeny']['code'],\
                              errCode.err['AuthorizationDeny']['message'],\
                              errCode.err['AuthorizationDeny']['http']) 
        return 
    

        
    def _checkObjectPerm(self,idObject, user, prop, op):
        self._LOGGER.info("Check if the user has permission on the bucket")
        self._LOGGER.debug('Permission-type : %s' % op) 
    
        if user.uid == prop.uid:
            return True
        
        acp =  self.metaMng.getObjectACP(idObject)
        if acp.grantee(user,op):
            return True
       
        #Permission deny no acp  
        self._LOGGER.warning('The user cannot do this operation the bucket')
        raise RestFSError(errCode.err['AuthorizationDeny']['code'],\
                              errCode.err['AuthorizationDeny']['message'],\
                              errCode.err['AuthorizationDeny']['http']) 


    
       ##########################################################################
        # MAPPER FOR ACL
        ##########################################################################       
    
    def amzToPerm(self,amz):
        
        perm = {}
        
        if amz == S3.AMZ_PRIVATE:
            
            perm = {RestFS.ADD_FILE:RestFS.ALLOW,RestFS.ADD_SUBDIR:RestFS.ALLOW, RestFS.ADMIN:RestFS.ALLOW, RestFS.APPEND_DATA:RestFS.ALLOW,\
            RestFS.DELETE_CHILD:RestFS.ALLOW,RestFS.DELETE_FILE:RestFS.ALLOW, RestFS.EXECUTE:RestFS.ALLOW, RestFS.FLAG:RestFS.ALLOW,\
            RestFS.LIST_DIR:RestFS.ALLOW, RestFS.READ_ACL:RestFS.ALLOW, RestFS.READ_ATTR:RestFS.ALLOW, RestFS.READ_DATA:RestFS.ALLOW, \
            RestFS.READ_XATTR:RestFS.ALLOW, RestFS.WRITE_ACL:RestFS.ALLOW, RestFS.WRITE_ATTR:RestFS.ALLOW, RestFS.WRITE_DATA:RestFS.ALLOW, \
            RestFS.WRITE_OWNER:RestFS.ALLOW, RestFS.WRITE_XATTR:RestFS.ALLOW}
        
        elif amz == S3.AMZ_BUCKET_OWNER_FULL_CONTROL:
    
            perm = {RestFS.ADD_FILE:RestFS.ALLOW,RestFS.ADD_SUBDIR:RestFS.ALLOW, RestFS.ADMIN:RestFS.ALLOW, RestFS.APPEND_DATA:RestFS.ALLOW,\
            RestFS.DELETE_CHILD:RestFS.ALLOW,RestFS.DELETE_FILE:RestFS.ALLOW, RestFS.EXECUTE:RestFS.ALLOW, RestFS.FLAG:RestFS.ALLOW,\
            RestFS.LIST_DIR:RestFS.ALLOW, RestFS.READ_ACL:RestFS.ALLOW, RestFS.READ_ATTR:RestFS.ALLOW, RestFS.READ_DATA:RestFS.ALLOW, \
            RestFS.READ_XATTR:RestFS.ALLOW, RestFS.WRITE_ACL:RestFS.ALLOW, RestFS.WRITE_ATTR:RestFS.ALLOW, RestFS.WRITE_DATA:RestFS.ALLOW, \
            RestFS.WRITE_OWNER:RestFS.ALLOW, RestFS.WRITE_XATTR:RestFS.ALLOW}
            
        elif amz == S3.AMZ_PUBLIC_READ:
            perm = { RestFS.EXECUTE:RestFS.ALLOW, RestFS.LIST_DIR:RestFS.ALLOW, RestFS.READ_ACL:RestFS.ALLOW, \
                                RestFS.READ_ATTR:RestFS.ALLOW, RestFS.READ_DATA:RestFS.ALLOW, RestFS.READ_XATTR:RestFS.ALLOW }
            
        elif amz == S3.AMZ_AUTHENTICATED_READ:
    
            perm = { RestFS.EXECUTE:RestFS.ALLOW, RestFS.LIST_DIR:RestFS.ALLOW, RestFS.READ_ACL:RestFS.ALLOW, \
                                RestFS.READ_ATTR:RestFS.ALLOW, RestFS.READ_DATA:RestFS.ALLOW, RestFS.READ_XATTR:RestFS.ALLOW }
            
                
        elif amz == S3.AMZ_BUCKET_OWNER_READ:
    
            perm = { RestFS.EXECUTE:RestFS.ALLOW, RestFS.LIST_DIR:RestFS.ALLOW, RestFS.READ_ACL:RestFS.ALLOW, \
                                RestFS.READ_ATTR:RestFS.ALLOW, RestFS.READ_DATA:RestFS.ALLOW, RestFS.READ_XATTR:RestFS.ALLOW, \
                                RestFS.ADMIN:RestFS.ALLOW}
            
        elif amz == S3.AMZ_PUBLIC_READ_WRITE:
            perm = { RestFS.EXECUTE:RestFS.ALLOW, RestFS.LIST_DIR:RestFS.ALLOW, RestFS.READ_ACL:RestFS.ALLOW, \
                                RestFS.READ_ATTR:RestFS.ALLOW, RestFS.READ_DATA:RestFS.ALLOW, RestFS.READ_XATTR:RestFS.ALLOW, \
                                RestFS.WRITE_ATTR:RestFS.ALLOW, RestFS.WRITE_ACL:RestFS.ALLOW, RestFS.WRITE_DATA:RestFS.ALLOW, \
                                RestFS.WRITE_XATTR:RestFS.ALLOW}
        else:
                raise RestFSError(errCode.err['MalformedACLError']['code'],\
                                  errCode.err['MalformedACLError']['message'],\
                                  errCode.err['MalformedACLError']['http'])
                
        return perm
    
    def amzToID(self,amz,user):
        id = None
        
        if amz == S3.AMZ_PRIVATE:
            id = user
        
        elif amz == S3.AMZ_PUBLIC_READ or amz == S3.AMZ_PUBLIC_READ_WRITE:
            id = RestFS.USER_ANONYMOUS
        
        elif amz == S3.AMZ_AUTHENTICATED_READ:
            id = RestFS.USER_AUTHENTICATED
        
        elif amz == S3.AMZ_BUCKET_OWNER_FULL_CONTROL or amz == S3.AMZ_BUCKET_OWNER_READ:
            id = user
        
        else:
                raise RestFSError(errCode.err['MalformedACLError']['code'],\
                                  errCode.err['MalformedACLError']['message'],\
                                  errCode.err['MalformedACLError']['http'])
        
        return id
    
    def permToS3Acl(self,permission):
    
        acl = None
        if permission.has_key(RestFS.ADMIN):      
            if permission[RestFS.ADMIN] == RestFS.ALLOW :
                acl = S3.ACL_FULL_CONTROL
        elif permission.has_key(RestFS.WRITE_ACL):
            if permission[RestFS.WRITE_ACL] == RestFS.ALLOW :
                acl = S3.ACL_WRITE_ACP
        elif permission.has_key(RestFS.READ_ACL) :
            if permission[RestFS.READ_ACL] == RestFS.ALLOW :
                acl = S3.ACL_READ_ACP
        elif permission.has_key(RestFS.WRITE_DATA) :
            if permission[RestFS.WRITE_DATA] == RestFS.ALLOW :
                acl = S3.ACL_WRITE
        elif permission.has_key(RestFS.READ_DATA) :
            if permission[RestFS.READ_DATA] == RestFS.ALLOW :
                acl = S3.ACL_READ
            
        return acl
    
    
    
    
    def s3AclToPerm(self,s3Acl):
    
        perm = {}
        
        if  s3Acl == S3.ACL_FULL_CONTROL:
            perm = {RestFS.ADD_FILE:RestFS.ALLOW,RestFS.ADD_SUBDIR:RestFS.ALLOW, RestFS.ADMIN:RestFS.ALLOW, RestFS.APPEND_DATA:RestFS.ALLOW,\
                            RestFS.DELETE_CHILD:RestFS.ALLOW,RestFS.DELETE_FILE:RestFS.ALLOW, RestFS.EXECUTE:RestFS.ALLOW, RestFS.FLAG:RestFS.ALLOW,\
                            RestFS.LIST_DIR:RestFS.ALLOW, RestFS.READ_ACL:RestFS.ALLOW, RestFS.READ_ATTR:RestFS.ALLOW, RestFS.READ_DATA:RestFS.ALLOW, \
                            RestFS.READ_XATTR:RestFS.ALLOW, RestFS.WRITE_ACL:RestFS.ALLOW, RestFS.WRITE_ATTR:RestFS.ALLOW, RestFS.WRITE_DATA:RestFS.ALLOW, \
                            RestFS.WRITE_OWNER:RestFS.ALLOW, RestFS.WRITE_XATTR:RestFS.ALLOW}
        
        elif s3Acl == S3.ACL_WRITE_ACP:
            perm = { RestFS.EXECUTE:RestFS.ALLOW, RestFS.LIST_DIR:RestFS.ALLOW, RestFS.READ_ACL:RestFS.ALLOW, \
                            RestFS.READ_ATTR:RestFS.ALLOW, RestFS.READ_DATA:RestFS.ALLOW, RestFS.READ_XATTR:RestFS.ALLOW, \
                            RestFS.WRITE_ATTR:RestFS.ALLOW,RestFS.WRITE_ACL:RestFS.ALLOW, RestFS.WRITE_DATA:RestFS.ALLOW, \
                            RestFS.WRITE_XATTR:RestFS.ALLOW}
        
        elif s3Acl == S3.ACL_READ_ACP:
            perm = { RestFS.EXECUTE:RestFS.ALLOW, RestFS.LIST_DIR:RestFS.ALLOW, RestFS.READ_ACL:RestFS.ALLOW, \
                            RestFS.READ_ATTR:RestFS.ALLOW, RestFS.READ_DATA:RestFS.ALLOW, RestFS.READ_XATTR:RestFS.ALLOW}
            
        elif s3Acl == S3.ACL_READ:
            perm = { RestFS.EXECUTE:RestFS.ALLOW, RestFS.LIST_DIR:RestFS.ALLOW, \
                            RestFS.READ_ATTR:RestFS.ALLOW, RestFS.READ_DATA:RestFS.ALLOW, RestFS.READ_XATTR:RestFS.ALLOW}
            
        elif s3Acl == S3.ACL_WRITE:
            perm = { RestFS.EXECUTE:RestFS.ALLOW, RestFS.LIST_DIR:RestFS.ALLOW, \
                            RestFS.READ_ATTR:RestFS.ALLOW, RestFS.READ_DATA:RestFS.ALLOW, RestFS.READ_XATTR:RestFS.ALLOW, \
                            RestFS.WRITE_ATTR:RestFS.ALLOW, RestFS.WRITE_DATA:RestFS.ALLOW, \
                            RestFS.WRITE_XATTR:RestFS.ALLOW}
    
        else:
            raise RestFSError(errCode.err['MalformedACLError']['code'],\
                              errCode.err['MalformedACLError']['message'],\
                              errCode.err['MalformedACLError']['http'])
    
        return perm


        

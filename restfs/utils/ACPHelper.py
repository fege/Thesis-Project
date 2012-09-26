import types
import restfs.factories.S3ACPFactory as S3
import restfs.factories.RestFSACPFactory as RestFS
import restfs.factories.ErrorCodeFacotry  as errCode
from restfs.objects.RestFSError import RestFSError
from restfs.objects.Grant import Grant


    ##########################################################################
    # MAPPER FOR ACL
    ##########################################################################       
    
def amzToPerm(amz):
    
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

def amzToID(amz,user):
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

def permToS3Acl(permission):

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




def s3AclToPerm(s3Acl):

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


def granteeToGrant(myList):
        grantList = []
        if not type(myList) is types.DictType:
            for grantDict in myList:
                if not grantDict['Grantee'].has_key('ID'):
                    raise RestFSError(errCode.err['NotImplemented']['code'],\
                          errCode.err['NotImplemented']['message'],\
                          errCode.err['NotImplemented']['http'])            
                grant = Grant()
                grant.uid = grantDict['Grantee']['ID']
                grant.permission = s3AclToPerm(grantDict['Permission'])
                grantList.append(grant)
        else:
            
                grant = Grant()
                grant.uid = myList['Grantee']['ID']
                grant.permission = s3AclToPerm(myList['Permission'])
                grantList.append(grant)
                
        return  grantList
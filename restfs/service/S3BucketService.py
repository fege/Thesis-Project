import logging
import restfs.factories.BucketFactory as BF
import restfs.factories.ErrorCodeFacotry  as errCode
import restfs.utils.ACPHelper as ACPHelper

from restfs.objects.User import User
from restfs.objects.ACP import ACP
from restfs.objects.Grant import Grant
from restfs.objects.Context import Context
from restfs.objects.BucketLogging import BucketLogging
from restfs.objects.BucketVersioning import BucketVersioning
from restfs.objects.BucketWebsite import BucketWebsite
from restfs.objects.RestFSError import RestFSError
from restfs.service.S3BaseService import S3BaseService


class S3BucketService(S3BaseService):
    
    _LOGGER = logging.getLogger('STORAGE_BUCKET_SERVICE')
            
    #
    # Create Backet
    # 1) distribution -> ...
    ##########################################################################    
    # PUT Operation
    ##########################################################################

    def putBucket(self,bucket_name, user, session, amz, location):
        # service operation for search 
        self._LOGGER.info("Creating the bucket")
        self._LOGGER.debug('Bucket-name : %s' % bucket_name) 
        
        context = ''
        if user.id == 0 :
            self._LOGGER.warning('The user have not privileges to create the bucket')
            raise RestFSError(errCode.err['AuthorizationDeny']['code'],\
                              errCode.err['AuthorizationDeny']['message'],\
                              errCode.err['AuthorizationDeny']['http'])
        res = self.res.findBucket(bucket_name)
        
        if res > 0:
            self._LOGGER.warning('The bucket you have tried to create already exists')
            raise RestFSError(errCode.err['BucketAlreadyExists']['code'],\
                              errCode.err['BucketAlreadyExists']['message'],\
                              errCode.err['BucketAlreadyExists']['http']) 

        # How many bucket can create ? 
        num = self.res.getCountBucketByOwner(user.uid)
        self._LOGGER.debug('Bucket-number created : %s' % num) 
        mymax = User.max_buckets
        self._LOGGER.debug('Bucket-number max the user can create : %s' % mymax) 


        if num > mymax:
            self._LOGGER.warning('The user has reached the limit of the buckets to create')
            raise RestFSError(errCode.err['LimitBucketsReached']['code'],\
                              errCode.err['LimitBucketsReached']['message'],\
                              errCode.err['LimitBucketsReached']['http'])    

        #check policy
        #FIX

        if res > 0:
            self._LOGGER.warning('The policy document specified is invalid')
            raise RestFSError(errCode.err['InvalidPolicyDocument']['code'],\
                              errCode.err['InvalidPolicyDocument']['message'],\
                              errCode.err['InvalidPolicyDocument']['http'])

        #check location
        loc = self.res.getRegionList()
        if not location:
            location = "EU"
        elif location not in loc:
            self._LOGGER.warning('The region/location the user specified was not found')
            raise RestFSError(errCode.err['RegionNotFound']['code'],\
                              errCode.err['RegionNotFound']['message'],\
                              errCode.err['RegionNotFound']['http'])

        
        #Find cluster
    
        # Set Administration privileges .. 
        # Make registration of the new Bucket
        # Resource manager operation
        self.res.addBucket(bucket_name,user.uid)
               
        # Convert S3 Permission to RestFs Permission
        acp = ACP()
        grant = Grant()
        grant.permission = ACPHelper.amzToPerm(amz)
        grant.uid = user.uid
        grantList = [grant]
        acp.setByGrants(grantList)
        #FIX ME
        gid = ''
      
        self.meta.createBucket(bucket_name,acp,location,user.uid, gid,context)
        
        #Storage operation
        self.storage.createBucket(bucket_name,context)
        
        #Close the transaction, put bucket online
        self.res.setBucketStatus(bucket_name,2,context)
        
        

    def putBucketACL(self, bucket_name , user, session, grantList):
        self._LOGGER.info("PUT bucket-Acl")
        
        context = ''
        
        acp = ACP()
        acp.setByGrants(grantList)
        
        self.meta.setBucketACL(bucket_name, acp, context)
        return
    
    def putBucketVersioning(self, bucket_name , user, session, status, mfa):
        self._LOGGER.info("PUT bucket-Versioning")

        context = ''
        
        versioning = BucketVersioning()
        versioning.status    = status
        versioning.MfaDelete = mfa
        
        self.meta.setBucketProperty(bucket_name,BF.PROPERTIES_VERSIONING , versioning, context)
        
        return
    
    def putBucketWebsite(self, bucket_name , user, session,  index, err):
        self._LOGGER.info("PUT bucket-Website")
 
        context = ''
        
        website = BucketWebsite()
        website.indexSuffix = index
        website.errorKey = err
        
        self.meta.setBucketProperty(bucket_name, BF.PROPERTIES_WEBSITE, website, context)

        return 
    
    
    def putBucketLocation(self, bucket_name , user, session,  location):
        self._LOGGER.info("PUT bucket-Location")
 
        context = ''
        
        self.meta.setBucketProperty(bucket_name, BF.PROPERTIES_LOCATION, location, context)

        return 

    def putBucketLogging(self, bucket_name , user, session, grantList, target, prefix ):
        self._LOGGER.info("PUT bucket-Logging")

        context = ''
         
        log = BucketLogging(target,prefix)
        log.setByGrants(grantList)
        
        self.meta.setBucketProperty(bucket_name,BF.PROPERTIES_LOGGING ,log, context)

        return
        
        
        
    ##########################################################################
    # GET OPERATION
    ##########################################################################
    def getBucketList(self, user, session):
        #FIXME Lookup User and Group List 
        context = ''
        return self.res.getBucketListByOwner(user.uid)
    
             
    def getBucketACL(self, bucket_name, user, session):
        self._LOGGER.info("GET the bucket-acl")
       
        #if not prop.acp:
        #    return []  
        context = ''
        gid = ''
        acp =  self.meta.getBucketACL(bucket_name,user.uid, gid, context)
        self._LOGGER.debug('Bucket acp grantlist : %s' % acp.getGrantList()) 
        return acp.getGrantList()
    
    def getBucketLogging(self,bucket_name, user, session):
        self._LOGGER.info("GET the bucket-logging")
        
        context = ''
        logging =  self.meta.getBucketProperty(bucket_name,BF.PROPERTIES_LOGGING,context)
        if not logging:
            return [] 
        return logging.getGrantList()
               
    def getBucketVersioning(self, bucket_name, user, session):
        self._LOGGER.info("GET bucket-Versioning")
     
        context = ''
       
        versioning =  self.meta.getBucketProperty(bucket_name,BF.PROPERTIES_VERSIONING,context)
        self._LOGGER.debug('Bucket versioning : %s' % versioning) 
        return versioning
    
    def getBucketWebsite(self, bucket_name, user , session):
        self._LOGGER.info("GET bucket-Website")
        
        context = ''
        
        website =  self.meta.getBucketProperty(bucket_name,BF.PROPERTIES_WEBSITE,context)
        self._LOGGER.debug('Bucket website : %s' % website) 
        return website

    
    def getBucketLocation(self,bucket_name, user, session):
        self._LOGGER.info("GET bucket-Location")
        context = ''
        
        location = self.meta.getBucketProperty(bucket_name,BF.PROPERTIES_LOCATION,context)
        self._LOGGER.debug('Bucket location : %s' % location) 
        return location
        
    
  
    ##########################################################################
    # DELETE OPERATION
    ##########################################################################
  
    def removeBucket(self, bucket_name, user, session):
        self._LOGGER.info("REMOVING the bucket")
        
        context = ''
        
        #Metadata operation
        self.meta.removeBucket(bucket_name,context)
        #what the hell is this ?
        self.storage.removeBucket(bucket_name,context)
        #Remove from resource
        self.res.removeBucket(bucket_name,context)
        
    def deleteBucketWebsite(self, bucket_name, user, session):
        self._LOGGER.info("REMOVING the bucket-website")
        
        context = ''
        self.meta.delBucketProperty(bucket_name,BF.PROPERTIES_WEBSITE , context)
           
    #non funziona
    def disableBucketLogging(self,bucket_name, user, session):
        self._LOGGER.info("DISABLE bucket logging operation")
        
        context = ''
        logging = self.meta.getBucketProperty(bucket_name,BF.PROPERTIES_LOGGING,context)
        logging.status = BF.LOG_DISABLE
        self.meta.setBucketProperty(bucket_name,BF.PROPERTIES_LOGGING,logging,context)
        
        return 

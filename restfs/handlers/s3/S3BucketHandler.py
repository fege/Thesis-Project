
import logging
import datetime



from restfs.handlers.s3.S3Handler import S3Handler
import restfs.factories.S3ACPFactory as S3ACP
import restfs.utils.ACPHelper as ACPHelper
import restfs.factories.ErrorCodeFacotry as errCode
from restfs.objects.RestFSError import RestFSError
from  restfs.utils.XmlUtils import XmlToDict
import restfs.factories.ObjectFactory as OF 



class S3BucketHandler(S3Handler):
    
    _LOGGER = logging.getLogger('BUCKET_HANDLER')

    def get(self):
        self._LOGGER.info("GET Bucket Request") 
        res = None
        #METADATA function
        if not self.s3Request.SUBRESOURCE:
            res = self._getObjectList()
        else:
            if self.s3Request.SUBRESOURCE.get('location',None):
                res =  self._getBucketLocation()
            elif self.s3Request.SUBRESOURCE.get('acl', None):
                res =  self._getBucketAcl()
            elif self.s3Request.SUBRESOURCE.get('logging',None):
                res =  self._getBucketLogging()   
            elif self.s3Request.SUBRESOURCE.get('versioning',None):
                res =  self._getBucketVersioning()  
            elif self.s3Request.SUBRESOURCE.get('website',None):
                res =  self._getBucketWebsite()
            else:
                self._notImplemented()
       
                  
        self.s3Render(res,self.s3Request.ID_REQUEST)
       
       

    def put(self):
        self._LOGGER.info('PUT bucket request')

        res = None
        headers = {}
        myDict  = {}
        xml = self.getBody()
        if xml:
            myDict = XmlToDict(self.getBody()).getDict()
        
        #Get HEADER NAME FROM CONSTANT
        if not self.s3Request.SUBRESOURCE:
            headers = self._putBucket(myDict)
        else:
            if self.s3Request.SUBRESOURCE.get('acl', None):
                res = self._putBucketAcl(myDict)
            elif self.s3Request.SUBRESOURCE.get('logging',None):
                res = self._putBucketLogging(myDict)    
            elif self.s3Request.SUBRESOURCE.get('versioning',None):
                res = self._putBucketVersioning(myDict)   
            elif self.s3Request.SUBRESOURCE.get('website',None):
                res = self._putBucketWebsite(myDict)
            else:
                self._notImplemented()
            
        self._LOGGER.debug('Headers : %s' % headers)
        self.s3Render(res,self.s3Request.ID_REQUEST,headers)
       
         



    def delete(self):
        
        self._LOGGER.info('DELETE bucket request')
        bucket_name = self.s3Request.BUCKET
        self._LOGGER.debug('Bucket Name : %s' % bucket_name)
        
        if self.s3Request.SUBRESOURCE:
            if self.s3Request.SUBRESOURCE.get('website', None):
                self.application.bucketSrv.deleteBucketWebsite(bucket_name,self.getUser(),self.s3Request.ID_REQUEST)
            else :
                self._notImplmeneted()
        else :
            self.application.bucketSrv.removeBucket(bucket_name,self.getUser(),self.s3Request.ID_REQUEST)

        
        self.s3Render(None,self.s3Request.ID_REQUEST,code=204)
        
        
        
    ##########################################################################
    # SUb function for GET
    ##########################################################################
    
    
    def _getObjectList(self):
        
        prefix = self.get_argument("prefix", "")
        marker = self.get_argument("marker", "")
        max_keys = int(self.get_argument("max-keys", 50000))
        ###
        ###terse = int(self.get_argument("terse", 0))
        ###
        isTruncated = int(self.get_argument("IsTruncated",0))
        
        #FIXME try cach

        self._LOGGER.debug('Bucket Name : %s' % self.s3Request.BUCKET)
        self._LOGGER.debug('prefix : %s' % prefix)
        self._LOGGER.debug('marker : %s' % marker)
        self._LOGGER.debug('max keys : %s' % max_keys)
        self._LOGGER.debug('isTruncated : %s' % isTruncated)

        resObjectQuery = self.application.objectSrv.getObjectList(self.s3Request.BUCKET,self.getUser(), self.s3Request.ID_REQUEST, prefix,marker,max_keys,isTruncated)
        my_delimiter = '/'
        
        res = {"ListBucketResult":{"Name":resObjectQuery.bucket_name,
                                   "Prefix":resObjectQuery.prefix,
                                   "Marker":resObjectQuery.marker,
                                   "MaxKeys":resObjectQuery.max_keys,
                                   "Delimiter":my_delimiter, 
                                   "IsTruncated":resObjectQuery.isTruncated } 
               
               }
        res["ListBucketResult"]["CommonPrefixes"] = []
        res["ListBucketResult"]["Contents"] = []
        
        for obj in resObjectQuery.getObjectList():
            #el = {}
            if obj.object_type == OF.TYPE_FILE:
                el = {}
                el["Key"] = resObjectQuery.prefix +"/"+obj.object_name
                el["LastModified"] =  datetime.datetime.utcfromtimestamp(obj.last_modify) 
                el["ETag"] = obj.md5
                el["Size"] = obj.size
                #el["object_type"] = obj.object_type
                el["Owner"] = {"ID":obj.uid,"DispalyName":''}
                el["StorageClass"] =  obj.storage_class
                res["ListBucketResult"]["Contents"].append(el)
                
            elif obj.object_type == OF.TYPE_DIR or obj.object_type == OF.TYPE_ROOT:
                el = {}
                el['Prefix'] = resObjectQuery.prefix + obj.object_name +'/'
                res["ListBucketResult"]['CommonPrefixes'].append(el)
                
        return res
    
    def _getBucketAcl(self):
        
        self._LOGGER.info('Getting the acl of the bucket')

        grantList = self.application.bucketSrv.getBucketACL(self.s3Request.BUCKET,self.getUser(),self.s3Request.ID_REQUEST)
         
        if len(grantList) == 0 :
            return {"AccessControlPolicy": {
                                     "Owner"  :  {"ID": self.user.uid,
                                                  "DisplayName": ""}
                                                  }
                      }
            
        #FIXME MOVE IN Service object->Dict convertion
        outList = []
        for grant in grantList:
            grants = ""
            grants +='<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">'
            grants += "<ID>%s</ID>" % grant.uid
            grants += "<DisplayName></DisplayName>"
            grants +='</Grantee>'
            grants += "<Permission>%s</Permission>" % ACPHelper.permToS3Acl(grant.permission)
            outList.append(grants)                     
          
        self._LOGGER.debug('Outlist : %s' % outList) 
        return {"AccessControlPolicy": {
             "Owner"  :  {"ID": self.user.uid,
                          "DisplayName": ""},
             "AccessControlList": {
                                  "Grant": outList}
             }
         }
        


    def _getBucketVersioning(self): 
        
        self._LOGGER.info('Getting the version of the bucket')
        
        vers = self.application.bucketSrv.getBucketVersioning(self.s3Request.BUCKET,self.getUser(),self.s3Request.ID_REQUEST)
        self._LOGGER.debug('Versioning : %s' % vers) 
        res = {}
        res['VersioningConfiguration'] ={}
        if vers:
            res['VersioningConfiguration']['Status'] = vers.status
           
            if vers.MfaDelete:
                res['VersioningConfiguration']['MfaDelete'] = vers.MfaDelete 
             
        return res 
        
        
    def _getBucketWebsite(self):

        self._LOGGER.info('Getting the website of the bucket')

        web = self.application.bucketSrv.getBucketWebsite(self.s3Request.BUCKET,self.getUser(),self.s3Request.ID_REQUEST)
        self._LOGGER.debug('Website : %s' % web)         
        res = {}
        res['WebsiteConfiguration'] ={}
        if web:
            res['WebsiteConfiguration']['IndexSuffix'] = web.indexSuffix
           
            if web.keyError:
                res['WebsiteConfiguration']['ErrorKey'] = web.keyError 
             
        return res         
        
        
    
    def _getBucketLocation(self):
        
        self._LOGGER.info('Getting the location of the bucket')

        location = self.application.bucketSrv.getBucketLocation(self.s3Request.BUCKET,self.getUser(),self.s3Request.ID_REQUEST)
        self._LOGGER.debug('Location : %s' % location)         
        res = {}
        res['LocationConstraint'] ={}
        if location :
            res = {"LocationConstraint": location}  
        return res 
  
  
    def _getBucketLogging(self):

        self._LOGGER.info('Getting the logging of the bucket')
        #FIXME move object to dict in service
        loggingList = self.application.bucketSrv.getBucketLogging(self.s3Request.BUCKET,self.getUser(),self.s3Request.ID_REQUEST)
        self._LOGGER.debug('LoggingList : %s' % loggingList)         
        if loggingList == []:
            return {'BucketLoggingStatus xmlns="http://doc.s3.amazonaws.com/2006-03-01': None }

        
        outList = []
        if loggingList :
            for grant in loggingList:
                grants = ""
                grants +='<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser>'
                grants += "<ID>%s</ID>" % grant.uid
                grants +='</Grantee>'
                grants += "<Permission>%s</Permission>" % ACPHelper.permToS3Acl(grant.permission)
                outList.append(grants)                     
        self._LOGGER.debug('Outlist : %s' % loggingList) 
          
      
        return {"BucketLoggingStatus ": {
             "LoggingEnabled"  :  {"TargetBucket": self.fsRequest.BUCKET,
                                   "TargetPrefix": 'valore'},
                                   "TargetGrants": {
                                                    "Grant": outList}
             }
         }
    
     
    # SUb function for PUT
    ########################################################################## 
    def _putBucket(self,myDict):
        amz = None
        if self.request.headers.has_key('x-amz-acl'):
            amz = self.request.headers['x-amz-acl']
        else:
            amz = S3ACP.AMZ_PRIVATE
             

        lc = None
        
        if myDict.has_key("CreateBucketConfiguration"):
            lc  = myDict['CreateBucketConfiguration']['LocationConstraint']
        
        self.application.bucketSrv.putBucket(self.s3Request.BUCKET,self.getUser(), self.s3Request.ID_REQUEST , amz,lc)
        headers = {}
        headers['Location'] = "/"+self.s3Request.BUCKET
        
        return headers 
       
    def _putBucketAcl(self,myDict):
        
        self._LOGGER.info('Putting the acl in the bucket')
        myList = myDict['AccessControlPolicy']['AccessControlList']['Grant'] 
        grantList = ACPHelper.granteeToGrant(myList)     
        self.application.bucketSrv.putBucketACL(self.s3Request.BUCKET,self.getUser(), self.s3Request.ID_REQUEST, grantList)
        
        return
    
    def _putBucketLogging(self,myDict):

        self._LOGGER.info('Putting the logging in the bucket')

        if not myDict.has_key('LoggingEnabled'):
            
            self.application.bucketSrv.disableBucketLogging(self.s3Request.BUCKET,self.getUser(),self.s3Request.ID_REQUEST)
            return
        
        bucket_dest   = myDict['LoggingEnabled']['TargetBucket']
        bucket_prefix = myDict['LoggingEnabled']['TargetPrefix']
        
        myList = myDict['LoggingEnabled']['TargetGrants']['Grant']
                           
        self.application.bucketSrv.putBucketLogging(self.fsRequest.BUCKET,self.getUser(), self.s3Request.ID_REQUEST,  myList, bucket_dest, bucket_prefix)
        
        return
    
    def _putBucketVersioning(self,myDict):

        self._LOGGER.info('Putting the version in the bucket')    

        myList = myDict['VersioningConfiguration']
       
        self.application.bucketSrv.putBucketVersioning(self.s3Request.BUCKET,self.getUser(), self.s3Request.ID_REQUEST , myList['Status'], myList.get('MfaDelete', None))
        return
    
    # FIXME Rewrite with Xattr !!!!!!
    def _putBucketWebsite(self,myDict):

        self._LOGGER.info('Putting the website in the bucket')    
        
        myList = myDict['WebsiteConfiguration']
       
        self.application.bucketSrv.putBucketWebsite(self.s3Request.BUCKET,self.getUser(), self.s3Request.ID_REQUEST , myList["IndexSuffix"] ,myList["ErrorKey"]  )
        return
    
    
    #
    # NOT Implemented !  
    ###########################################################################
    
    def _notImplemented(self):
        self._LOGGER.warning('Operation currently not implemented')
        raise RestFSError(errCode.err['NotImplemented']['code'],\
                          errCode.err['NotImplemented']['message'],\
                          errCode.err['NotImplemented']['http']) 
        return 
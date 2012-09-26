
import datetime
import logging
import types
import urllib
import time


from restfs.handlers.s3.S3Handler import S3Handler
from restfs.objects.Grant import Grant
import restfs.factories.S3ACPFactory as S3ACP
import restfs.utils.ACPHelper as ACPHelper
from restfs.objects.ACP import ACP
from restfs.objects.Context import Context
import restfs.factories.ErrorCodeFacotry as errCode
from restfs.objects.RestFSError import RestFSError
from  restfs.utils.XmlUtils import XmlToDict

class S3ObjectHandler(S3Handler):
    
    _LOGGER = logging.getLogger('OBJECT_HANDLER')
    


    def get(self):
        self._LOGGER.info("GET Object Request") 
        # FIXME HEADER!!!!!!
        res = None

        if not self.s3Request.SUBRESOURCE:
            res = self._getObject()
            return
        else:
            if self.s3Request.SUBRESOURCE.get('acl', None):
                res =  self._getObjectAcl()

      
        self.s3Render(res,self.s3Request.ID_REQUEST)


    def head(self):
        
        self._LOGGER.info("HEAD Object Request") 
     
        self._LOGGER.debug('Bucket-name : %s' % self.s3Request.BUCKET ) 
        self._LOGGER.debug('Object-name : %s' % self.s3Request.OBJECT) 
       

        obj = self.application.objectSrv.getObjectProperties(self.s3Request.BUCKET ,self.s3Request.OBJECT,self.getUser(),self.s3Request.ID_REQUEST)
        
        headers = {} 
        if obj.content_type:
            headers["Content-Type"] = obj.content_type
        headers["Content-Length"]   = obj.size
        headers["Last-Modified"]    = datetime.datetime.utcfromtimestamp(obj.last_modify)
        headers["ETag"] = obj.md5
        #only AMZ xattr
        if obj.xattr:
            headers.update(obj.xattr)
           
        self._LOGGER.debug('Headers : %s' % headers) 
        self.s3Render(None, self.s3Request.ID_REQUEST, headers)


    
  
    def put(self):

        self._LOGGER.info("Put Object Request") 
        headers = None
        response = None
        if not self.s3Request.SUBRESOURCE:
            if self.s3Request._AUTH_S3_HEADERS.has_key('x-amz-copy-source'):
                self._LOGGER.info("Copy Object Request")
                self._LOGGER.debug("Copy source : %s" %self.s3Request._AUTH_S3_HEADERS['x-amz-copy-source'])
                obj = self._copyObject()
                headers = {}
                headers["ETag"] = obj.md5
                headers["x-amz-copy-source-version-id"] = obj.md5
                headers["x-amz-version-id"] = obj.md5
                
                response  = {"CopyObjectResult": 
                                {
                                 'LastModified': datetime.datetime.utcfromtimestamp(obj.last_modify), 
                                 'ETag': obj.md5
                                 }
                            }
            else:
                headers = self._putObject()
            
            
        else:     
            if self.s3Request.SUBRESOURCE.get('acl', None):
                headers = self._putObjectAcl()
           
        
                
        self._LOGGER.debug("Headers : %s" %headers)
        self.s3Render(response,self.s3Request.ID_REQUEST, headers)
        
 
    def delete(self):
        self._LOGGER.info("Delete Object Request") 

        self._LOGGER.debug('Bucket-name : %s' % self.s3Request.BUCKET) 
        self._LOGGER.debug('Object-name : %s' % self.s3Request.OBJECT)
                   
        self.application.objectSrv.deleteObject(self.s3Request.BUCKET,self.s3Request.OBJECT,self.getUser(), self.s3Request.ID_REQUEST)
        
        self._LOGGER.info("Object Deleted") 
        
        self.s3Render(None,self.s3Request.ID_REQUEST, None, 204)
        
        #self.set_status(204)
        #self.finish()
       

    ##########################################################################
    # SUb function for GET
    ##########################################################################
  
    def _getObjectAcl(self):
        
        self._LOGGER.info("getting the object acl") 

        grantList = self.application.objectSrv.getObjectACL(self.s3Request.BUCKET,self.s3Request.OBJECT, self.getUser(), self.s3Request.ID_REQUEST)
          
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
    
    
   
    def _getObject(self):
        
        self._LOGGER.info("getting the object") 

        self._LOGGER.debug('Bucket-name : %s' % self.s3Request.BUCKET) 
        self._LOGGER.debug('Object-name : %s' % self.s3Request.OBJECT)

        #FIXME Content type from object
        obj = self.application.objectSrv.getObjectProperties(self.s3Request.BUCKET,self.s3Request.OBJECT, self.getUser(), self.s3Request.ID_REQUEST)
                
        if obj.content_type:
            self.set_header("Content-Type", obj.content_type)
            
        if obj.last_modify:
            self.set_header("Last-Modified", datetime.datetime.utcfromtimestamp(obj.last_modify))
        else:
            self.set_header("Last-Modified", datetime.datetime.utcfromtimestamp(time.time()))
        self.set_header("ETag" ,obj.md5)
        #only AMZ xattr
        
        if obj.getXattr() :
            for key,value in  obj.getXattr().iteritems():
                self.set_header(key, value)
                #self.set_header('content-md5',md5)
                # SET HANDLER for Async ! '''
    
        self.finish(self.application.objectSrv.getObjectData(obj, self.getUser(), self.s3Request.ID_REQUEST))    
        return None
    
    ##########################################################################
    # SUb function for GET
    ##########################################################################
    
    def _putObject(self):
        
        self._LOGGER.info("creating the object") 

        self._LOGGER.debug('Bucket-name : %s' % self.s3Request.BUCKET) 
        self._LOGGER.debug('Object-name : %s' % self.s3Request.OBJECT)
       
        storage_class = "STANDARD"
        if self.s3Request._AUTH_S3_HEADERS.has_key('x-amz-storage-class'):
            storage_class = self.s3Request._AUTH_S3_HEADERS['x-amz-storage-class']
            
        object_acl = S3ACP.AMZ_PRIVATE   
        #if self.s3Request._AUTH_S3_HEADERS.has_key('x-amz-acl'):
        #    object_acl = self.s3Request._AUTH_S3_HEADERS['x-amz-acl']
        self._LOGGER.debug('Object acl : %s' % object_acl)
        
        content_type = None   
        if self.s3Request._AUTH_S3_HEADERS.has_key('content-type'):
            content_type = self.s3Request._AUTH_S3_HEADERS['content-type']
        self._LOGGER.debug('Object content type : %s' % content_type)
        
        meta = self.s3Request.getMetaData()
       
        acp = ACP()
        grant = Grant() 
        #FIXME user amzToID
        grant.uid = self.getUser().uid  
        grant.permission = ACPHelper.amzToPerm(object_acl)
        grantList = [grant]
        acp.setByGrants(grantList)

            
        obj = self.application.objectSrv.addObject(self.s3Request.BUCKET,self.s3Request.OBJECT, self.getUser(), self.s3Request.ID_REQUEST,storage_class , acp, content_type, meta, self.getBody())
        headers = {}
      

      
      
        headers["ETag"] =  obj.md5
        
        return headers
      

    def _copyObject(self):
        self._LOGGER.info("coping the object") 

        self._LOGGER.debug('Bucket-name : %s' % self.s3Request.BUCKET) 
        self._LOGGER.debug('Object-name : %s' % self.s3Request.OBJECT)

        storage_class = "STANDARD"
        if self.s3Request._AUTH_S3_HEADERS.has_key('x-amz-storage-class'):
            storage_class = self.s3Request._AUTH_S3_HEADERS['x-amz-storage-class']
            
        object_acl = S3ACP.AMZ_PRIVATE   
        if self.s3Request._AUTH_S3_HEADERS.has_key('x-amz-acl'):
            object_acl = self.s3Request._AUTH_S3_HEADERS['x-amz-acl']
        self._LOGGER.debug('Object acl : %s' % object_acl)
        
        content_type = None    
        if self.s3Request._AUTH_S3_HEADERS.has_key('Content-Type'):
            content_type = self.s3Request._AUTH_S3_HEADERS['Content-Type']
        self._LOGGER.debug('Object content type : %s' % content_type)
            
        meta_replace = "COPY"    
        if self.s3Request._AUTH_S3_HEADERS.has_key('x-amz-metadata-directive'):
            meta_replace = self.s3Request._AUTH_S3_HEADERS['x-amz-metadata-directive']

        meta = None 
        if meta_replace == "REPLACE":
                    meta = self.s3Request.getMetaData()
       
       
        copy_path = urllib.unquote(self.s3Request._AUTH_S3_HEADERS['x-amz-copy-source'])
        sourcePosition = copy_path.find('/')
        bucket_path = copy_path[:sourcePosition]
        object_path = copy_path[sourcePosition+1:]
            
        grant = Grant() 
        grant.uid = self.getUser().uid  
        grant.permission = ACPHelper.amzToPerm(object_acl)
        grantList = [grant]

        obj = self.application.objectSrv.copyObject(self.s3Request.BUCKET,self.s3Request.OBJECT, bucket_path,object_path ,self.getUser(), self.s3Request.ID_REQUEST, storage_class , grantList, content_type, meta)
        
        return obj



    def _putObjectAcl(self):
        
        self._LOGGER.info("putting the object acl") 
    
        myDict = XmlToDict(self.getBody()).getDict()
      
        myList = myDict['AccessControlPolicy']['AccessControlList']['Grant']
        
        
        grantList = []
        if not type(myList) is types.DictType:
            for grantDict in myList:
                if not grantDict['Grantee'].has_key('ID'):
                        self._LOGGER.warning('Putting different ACL from the usal CanonicalID, currently not implemented')
                        raise RestFSError(errCode.err['NotImplemented']['code'],\
                          errCode.err['NotImplemented']['message'],\
                          errCode.err['NotImplemented']['http'])
                grant = Grant()
                grant.uid = grantDict['Grantee']['ID']
                grant.permission = ACPHelper.s3AclToPerm(grantDict['Permission'])
                grantList.append(grant)
        else:
                grant = Grant()
                grant.uid = myList['Grantee']['ID']
                grant.permission = ACPHelper.s3AclToPerm(myList['Permission'])
                grantList.append(grant)
                
        self.application.objectSrv.putObjectACL(self.s3Request.BUCKET,self.s3Request.OBJECT,self.s3Request.ID_REQUEST, self.getUser(),grantList)
        
        return None
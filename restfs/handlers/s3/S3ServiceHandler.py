
import logging
import tornado.web

from restfs.handlers.s3.S3Handler import S3Handler


class S3ServiceHandler(S3Handler):
    _LOGGER = logging.getLogger('SERVICE_HANDLER')
    

    @tornado.web.asynchronous 
    def get(self):
        self._get()
        
        
    def _get(self):
        self._LOGGER.info("Service Request")

        myBucketList   = self.application.bucketSrv.getBucketList(self.getUser(),self.s3Request.ID_REQUEST)                                       

        res = {"ListAllMyBucketsResult": 
                        {
                            "Owner"  :  {
                                           "ID": self.user.uid,
                                           "DisplayName": self.user.display_name
                                        }
                        }
               }
        

        if myBucketList and len(myBucketList) > 0:
            
            buckets = []
            for bucket in myBucketList:
                buckets.append({
                    "Name": bucket.bucket_name,
                    "CreationDate": bucket.cdate
                    })
            
            
            res["ListAllMyBucketsResult"]["Buckets"] =  {"Bucket": buckets}
                      
        
        
        self.s3Render(res,self.s3Request.ID_REQUEST)

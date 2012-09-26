import math
import restfs.factories.ObjectFactory as OF
from restfs.objects.ACP import ACP
import cStringIO
import logging
import md5
from restfs.objects.ObjectQueryResult import ObjectQueryResult
from restfs.objects.RestFSError import RestFSError
from restfs.objects.Context import Context
from restfs.service.S3BaseService import S3BaseService
import restfs.factories.ErrorCodeFacotry as errCode


class S3ObjectService(S3BaseService):
    
    _LOGGER = logging.getLogger('STORAGE_OBJECT_SERVICE')
    
    #
    # Create Backet
    # 1) distribution -> ...
    ##########################################################################    
    # PUT Operation
    ##########################################################################
    
    def addObject(self, bucket_name, object_name, user, session, storage_class, object_acl ,content_type, xattr,  data_handler):
        import time
        start = time.time()
        #context oggetto
        #user id buckname idgrouputente [+ gruppi] sessione
        
        self._LOGGER.info("ADD Object") 
        #FIXME
        context = ''
        mode = ''
        gid = ''
        # Operation Type based on object existance
        idObj = self.meta.access(bucket_name, object_name, context)  #FIX TH PATH
        
        self._LOGGER.debug('Id Object : %s' % idObj)
        self._LOGGER.debug('S3 Object Name : %s' % object_name)
        
        if idObj == OF.EACCESS:
            self._LOGGER.warning("The user cannot access to the object")
            raise RestFSError(errCode.err['AuthorizationDeny']['code'],\
                          errCode.err['AuthorizationDeny']['message'],\
                          errCode.err['AuthorizationDeny']['http'])  
            return         
        
        # Create a new Object (object not found)
        if idObj == OF.ENOENT:  
            object_type = OF.TYPE_FILE
            if object_name[-1] == '/' :
                #the object is a directory
                object_type = OF.TYPE_DIR
                   
            idObj = self.meta.createObject(bucket_name, object_name, object_type, mode, user.id, gid, context)
            
            if idObj == OF.EACCESS:
                self._LOGGER.warning("The user cannot create the object")
                raise RestFSError(errCode.err['AuthorizationDeny']['code'],\
                              errCode.err['AuthorizationDeny']['message'],\
                              errCode.err['AuthorizationDeny']['http'])  
                return
            elif idObj == OF.ENOENT:
                self._LOGGER.warning("The object doesn't exist")
                raise RestFSError(errCode.err['ObjectNotFound']['code'],\
                          errCode.err['ObjectNotFound']['message'],\
                          errCode.err['ObjectNotFound']['http'])
                
                
        #Get Object Properties     
        obj = self.meta.getObjectProperty(idObj,OF.PROPERTY_OBJECT,user.id, gid, context)
        self.meta.setObjectProperty(idObj,OF.PROPERTY_ACL,object_acl,user.id, gid, context)
        
        
        #check if i can change it
        #self._checkObjectPerm(obj.id , context.user,obj,BF.ACL_WRITE)  
        # Object Dimension
        file_size = len(data_handler)
        block_counter       = int(math.ceil(file_size/obj.block_size))
        block_counter_old   = obj.block_counter
        segment_counter     = int(math.ceil(file_size/(obj.segment_size*obj.block_size)))
        segment_counter_old = obj.segment_counter
        
        self._LOGGER.debug('Object Dimension : %s' % file_size)
        
        # Init struct for save data
        segmentId = 0
        seg = self.meta.getObjectSegment(obj.id,segmentId,user.uid,gid, context)
        segBlock = self.meta.getObjectSegment(obj.id, str(segmentId)+'-Bid', user.uid,gid, context)
        myMd5 = md5.new()
        block_pos = 0
        
        myStringIO = cStringIO.StringIO(data_handler)
        
        for blockId in range(1,block_counter+1) :
            
            if block_pos > obj.segment_size:
                
                self.meta.setObjectSegment(obj.id,segmentId, seg, user.uid,gid, context)
                # Create a new segment numBlock BlockId
                self.meta.setObjectSegment(obj.id, str(segmentId)+'-Bid', segBlock, user.uid,gid, context)   
                block_pos = 0
                segmentId += 1
                seg  = self.meta.getObjectSegment(obj.id,segmentId , user.uid,gid, context)
                segBlock = self.meta.getObjectSegment(obj.id, str(segmentId)+'-Bid', user.uid,gid, context)
                
            block_pos += 1
            new_block = myStringIO.read(int(obj.block_size))
            bhash = self.meta.generateHash(new_block)
            bkey  = self.meta.getBlockKey(bucket_name,obj.id,segmentId,blockId)
            self._LOGGER.debug('Block Key : %s' % bkey)
            self._LOGGER.debug('Block Id : %s' % blockId)
                        
            if seg.has_key(blockId):
                hash_old = seg[blockId]
                bkey     = segBlock[blockId]
            else:
                hash_old = None
                bkey = self.meta.getBlockKey(bucket_name,obj.id,segmentId,blockId)
            
            myMd5.update(new_block)
            if bhash.hexdigest() != hash_old:
                self.storage.writeBlock(bucket_name, obj.id, bkey, new_block, bhash)
                #myMd5.update(new_block) 
                seg[blockId] = bhash.hexdigest()
                segBlock[blockId] = bkey
                  
            self._LOGGER.debug('Block Position : %s' % block_pos) 
            self._LOGGER.debug('Segment Id : %s' % segmentId) 
            self._LOGGER.debug('Old Hash : %s' % hash_old)
            self._LOGGER.debug('New Hash : %s' % bhash.hexdigest())     
                
        #save the last segment       
        self.meta.setObjectSegment(obj.id, segmentId, seg, user.uid,gid, context) 
        # Create a new segment numBlock BlockId    
        self.meta.setObjectSegment(obj.id, str(segmentId)+'-Bid', segBlock, user.uid,gid, context)
        
        if block_counter_old > block_counter:
            
            for i in range(blockId,blockId + (len(seg)-block_pos)):
                
                self.storage.removeBlock(bucket_name, obj.id, segBlock[i+1])
                del segBlock[i+1]
                del seg[i+1]
                
            self.meta.setObjectSegment(obj.id, segmentId, seg, user.uid,gid, context) 
            self.meta.setObjectSegment(obj.id, str(segmentId)+'-Bid', segBlock, user.uid,gid, context)
               
        #Check if i have to delete segments
        if  segment_counter < segment_counter_old:

            for segId in range (segment_counter+1,segment_counter_old+1):
                segBlock = self.meta.getObjectSegment(obj.id, str(segId)+'-Bid', user.uid,gid, context)
                self.meta.delObjectSegment(obj.id,segId, user.uid,gid, context)
                self.storage.remove(bucket_name, obj.id, segBlock)
            
        obj.block_counter   = block_counter
        obj.segment_counter = segment_counter
        obj.size            = file_size
        obj.num_segments    = segmentId
        #FIXME do a incremental job .. 
        obj.md5             =  myMd5.hexdigest()
        
        self.meta.setObjectProperty(obj.id, OF.PROPERTY_OBJECT, obj, user.id, gid, context)
        self.meta.refreshBucketProperties(bucket_name, obj.id, obj.size, block_counter)
        
        
        self._LOGGER.debug('Tot num blocks : %s' % block_counter)
        self._LOGGER.debug('Num segments for object : %s' % segment_counter)
        self._LOGGER.debug('MD5 : %s' % obj.md5)
        fine = time.time()-start
        print "TEMPO IMPIEGATO ", fine
        self._LOGGER.debug('tempo impiegato: %s ' % fine)
        self._LOGGER.debug('dimensione file: %s' % obj.size)
        return obj
          
    def copyObject(self,bucket_name, object_name, bucket_orig, object_orig, user, session, storage_class, grantList ,content_type, xattr):
        

        self._LOGGER.info("COPY Object") 
        context = ''

        # pick up the original
        objOriginal = self.getObjectProperties(bucket_orig, object_orig, user, session)
        objOriginalAcl = self.getObjectACL(bucket_orig, object_orig, user, session)
        objOriginalData = self.getObjectData(objOriginal, user, session)
        

        obj = self.addObject(bucket_name, object_name, user, session, storage_class, objOriginalAcl, content_type, xattr, objOriginalData)


        #FIX METADATAS, REALLY TO FIX ?
        
        # Special case .. for change attribute
        if bucket_orig == bucket_name and object_orig == object_name:
            if xattr:
                objOriginal.xattr = xattr
            self.meta.setObjectProperty(objOriginal.id, OF.PROPERTY_OBJECT, objOriginal, context)
            return objOriginal
        
        
        return obj    
        
        
    
    # ChangeME with new managers
    def putObjectACL(self, bucket_name ,object_name, user, session, grantList):
        self._LOGGER.info("PUT Object ACL")
        context = Context(user, '', session, [])
        
        # ACCESS OR LOOKUP ? 
        idObject = self.meta.access(bucket_name, object_name, context)
       
        acp = ACP()
        acp.setByGrants(grantList)
        self.meta.setObjectACL(idObject, acp, context)
        return
       

        
    ##########################################################################
    # GET OPERATION
    ##########################################################################
    
    def getObjectList(self,bucket_name, user, session, prefix, marker, max_keys,terse=0):
        context = ''
        gid=''
        
        res = ObjectQueryResult(bucket_name, prefix, marker,max_keys, False)
        path = "/"+prefix
        if not path.endswith("/"):
            path +="/"

        idObject = self.meta.lookup(bucket_name, path)
        
        if not idObject:
            raise RestFSError(errCode.err['ObjectNotFound']['code'],\
                          errCode.err['ObjectNotFound']['message'],\
                          errCode.err['ObjectNotFound']['http'])
            
         
        # We want all information 
        myList = self.meta.getObjectList(idObject,user.id, gid, context,meta=True)
        res.setObjectList(myList)
        return res

    
    

    def getObjectProperties(self, bucket_name,  object_name, user, session, ):
        self._LOGGER.info("GET Object Properties")
        context = ''
        gid = ''
        
        
        idObject = self.meta.access(bucket_name, object_name, context)

        if idObject == OF.EACCESS:
            self._LOGGER.warning("The user cannot access to the object")
            raise RestFSError(errCode.err['AuthorizationDeny']['code'],\
                          errCode.err['AuthorizationDeny']['message'],\
                          errCode.err['AuthorizationDeny']['http'])  
            return         
        
         
        elif idObject == OF.ENOENT:
                self._LOGGER.warning("The object doesn't exist")
                raise RestFSError(errCode.err['ObjectNotFound']['code'],\
                          errCode.err['ObjectNotFound']['message'],\
                          errCode.err['ObjectNotFound']['http'])
            
        #Get Object     
        return self.meta.getObjectProperty(idObject, OF.PROPERTY_OBJECT, user.id, gid, context)  
    
 
    def getObjectData(self, obj, user, session):
        self._LOGGER.info("GET Object")
        #I have already checked the access and the permissions
        #Get Object
        self._LOGGER.debug('Id Object : %s' % obj.id)
        context = ''
        gid = ''

        block_pos = 0
        segmentId = 0
        segBlock = self.meta.getObjectSegment(obj.id,str(segmentId)+'-Bid', user.uid,gid, context)
        streamData = '' # IOqulcosa 
        
        #for sinchronize it FIX
        #getObjectSegment(obj.id, str(segId)+'-Bid', context)
        
        #if not self.metaMng.open(obj.id, context)
        
        for blockId in range(1,obj.block_counter+1) :
            if block_pos > obj.segment_size:
                block_pos = 0
                segmentId += 1
                segBlock  = self.meta.getObjectSegment(obj.id,str(segmentId)+'-Bid', user.uid,gid, context)
            
            block_pos += 1
            #new_block = myStringIO.read(obj.block_size)
            #real_block_id = self.meta.getBlockKey(seg[blockId],blockId,\
            #                                         segmentId,obj.id,obj.bucket_name)
            data_block = self.storage.readBlock(obj.bucket_name, obj.id,segBlock[blockId])
            streamData += data_block
            self._LOGGER.debug('Block ID : %s' % blockId)
            self._LOGGER.debug('Real Block ID : %s' % segBlock[blockId])
            self._LOGGER.debug('Segment ID : %s' % segmentId)
    
        #for sinchronize it FIX
        #self.metaMng.close(obj.id, context)
        return streamData
       
          
    def getObjectACL(self,bucket_name, object_name ,user, session):
        self._LOGGER.info("GET Object ACL")
        context = ''
        gid = ''

        
        idObject = self.meta.lookup(bucket_name, object_name)
        #self._checkObjectPerm(object_name, context.user,prop,BF.ACL_READ_ACP)
        
        acp =  self.meta.getObjectProperty(idObject, OF.PROPERTY_ACL, user.id, gid, context)
        
        if not acp:
            return []
        return acp.getGrantList()

    
  
    ##########################################################################
    # DELETE OPERATION
    ##########################################################################
  
    def deleteObject(self,bucket_name, object_name, user, session):
        self._LOGGER.info("DELETE Object")
        context = ''
        gid = ''
            
        
        idObject = self.meta.lookup(bucket_name, object_name)       
        self._LOGGER.debug('User UID : %s' % user.uid)       
        #blocksNumber = int(math.ceil(obj.size/obj.block_size))
#        self.meta.refreshDelBucketProperties(bucket_name, obj.id, obj.size, blocksNumber)
        
        #Storage operation

        obj = self.meta.getObjectProperty(idObject, OF.PROPERTY_OBJECT, user.uid, gid, context) 
        if obj.object_type == OF.TYPE_FILE:
            self.storage.removeObject(bucket_name, idObject ,context)
        
        #Metadata operation
        self.meta.removeObject(idObject,user.uid, gid, context)

        

    
    

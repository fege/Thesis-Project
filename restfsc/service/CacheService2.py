from restfsc.manager.ConnectionManager import ConnectionManger
from restfs.objects.JsonRPCPayload import JsonRPCPayload
import restfs.factories.ObjectFactory as OF
import tornado.options
import logging
import restfsc.net.ping as Ping
from tornado.options import define, options
from restfsc.net.WebSocket import ABNF
from restfsc.manager.CacheManager import CacheManager
import math, cStringIO, md5, time, hashlib
from restfs.objects.RestFSError import RestFSError
import restfs.factories.ErrorCodeFacotry as errCode
from restfs.utils.bson import Binary

class CacheService(object):

    _LOGGER = logging.getLogger('CACHE_SERVICE')
    
    def __init__(self):
        # Read What kind of cache ... 
        self.conn_meta    =  ConnectionManger('metadata')
        self.conn_data    =  ConnectionManger('data')
        self.conn_service =  ConnectionManger('service')
        self.cacheMng     = CacheManager()
        
        #STATS
        self.srv_num_get        = 0
        self.srv_num_get_failed = 0
        self.srv_num_set       = 0
        #######
        ###CRITICAL    50
        ###ERROR    40
        ###WARNING    30
        ###INFO    20
        ###DEBUG    10
        
    #### metadata operations ####
    def findCluster(self,bucket_name):
        self._LOGGER.info("FIND Cluster %s" % bucket_name)
        if self._LOGGER.getEffectiveLevel() == 10:
            self._ourDebug(bucket_name)
        msg = JsonRPCPayload('findCluster',[bucket_name])
        self._sendService(msg)
        ip_list = msg.get('result')
        ping_list = []
        for ip_address in ip_list:
            RTT = Ping.doOne(ip_address)
            ping_list.append((ip_address,RTT))
        ping_list = sorted(ping_list, key=lambda i:i[-1])
        return ping_list      
    
    def lookup(self,bucket, path):
        self._LOGGER.info("Lookup operation %s" % path) 
        msg = JsonRPCPayload('lookup',[bucket, path])
        self._send(msg)
        return msg.get('result')      

    
    def createObject(self, bucket, path, obj_type, mode, uid, gid , context):
        print '####################'
        print 'CACHE CREATE OBJ'
        print bucket, path, obj_type, mode, uid, gid , context
        print '####################'
        self._LOGGER.info("Creating object %s")
        if self._LOGGER.getEffectiveLevel() == 10:
            self._ourDebug(bucket, path, obj_type, mode, uid, gid , context)
        msg = JsonRPCPayload('createObject',[ bucket, path, obj_type, mode, uid, gid, context])
        self._send(msg)
        return msg.get('result')     
    
    
    def getObjectList(self,bucket,idObj,uid,gid,context):
        self._LOGGER.info("Getting object list %s")
        if self._LOGGER.getEffectiveLevel() == 10:
            self._ourDebug(bucket, idObj,uid, gid , context)    
        msg = JsonRPCPayload('getObjectList',[idObj,uid,gid,context])
        self._send(msg)
        
        return msg.get('result')
    
    
    def removeObject(self, bucket, idObj, uid, gid, context):
        self._LOGGER.info("Removing object %s")
        if self._LOGGER.getEffectiveLevel() == 10:
            self._ourDebug(bucket, idObj,uid, gid , context)    
        msg = JsonRPCPayload('removeObject',[idObj, uid, gid,context])
        self._send(msg)
        self.cacheMng.delMetaObj(idObj)
        self.srv_num_set += 1
        return msg.get('result')
    
    # ATTRIBUTES
    ########################
    def getProperties(self,bucket, idObj,field,uid,gid,context):
        self._LOGGER.info("Getting object properties ")
        if self._LOGGER.getEffectiveLevel() == 10:
            self._ourDebug(bucket, idObj,field,uid, gid , context)   
        item, meta = self.cacheMng.getMetaItem(idObj, field)
       
        if item and meta['vers']==-1:
            self._LOGGER.info("If item is subscribed ") 
            return item
        
        if item and self._isValid(item, meta):
            self._LOGGER.info("The version is cache-coerent") 
            self._LOGGER.debug("meta  %s" % meta)
            return item
        
        if not self._isValid(item, meta):
            self.srv_num_get_failed += 1
            msg = JsonRPCPayload('getObjectVersion',[idObj,field,uid,gid,context])
            self._send(msg)

            if meta:
                if meta['vers'] == msg.get('result'):
                    self._LOGGER.info("The version is coerent with cache version") 
                    return item 
                
        self.srv_num_get += 1           
        msg = JsonRPCPayload('getAttributes',[idObj,field,uid,gid,context])
        self._send(msg)
        item = msg.get('result')
        self.cacheMng.setMetaItem(idObj,field,item,item['vers'])
        self.srv_num_set += 1
        return item
 
    def setAttributes(self,bucket, idObj,obj,uid,gid,context):
        self._LOGGER.info("Setting object attributes ")
        if self._LOGGER.getEffectiveLevel() == 10:
            self._ourDebug(bucket, idObj,uid, gid , context)            
        msg = JsonRPCPayload('setAttributes',[idObj,obj,uid,gid,context])
        self._send(msg)
        return msg.get('result')
    
    def setAttributesFromNew(self,bucket, idObj,obj,uid,gid,context):
        self._LOGGER.info("Setting object attributes ")
        if self._LOGGER.getEffectiveLevel() == 10:
            self._ourDebug(bucket, idObj,uid, gid , context)            
        msg = JsonRPCPayload('setAttributesFromNew',[idObj,obj,uid,gid,context])
        self._send(msg)
        return msg.get('result')      
    
    def setObjectMode(self, bucket, idObj, mode, uid, gid, context):
        self._LOGGER.info("Setting object mode ")
        if self._LOGGER.getEffectiveLevel() == 10:
            self._ourDebug(bucket, idObj,mode,uid, gid , context)        
        msg = JsonRPCPayload('setObjectMode',[idObj,mode,uid,gid,context])
        self._send(msg)
        return msg.get('result') 
        
    def setObjectOwner(self, bucket, idObj, owner, group, uid, gid, context):
        self._LOGGER.info("Setting object mode ")
        if self._LOGGER.getEffectiveLevel() == 10:
            self._ourDebug(bucket, idObj,owner, group,uid, gid , context)  
        msg = JsonRPCPayload('setObjectOwner',[idObj,owner,group,uid,gid,context])
        self._send(msg)
        return msg.get('result') 
    
    def setObjectUtime(self, bucket, idObj, utime, uid, gid, context):
        self._LOGGER.info("Setting object mode ")
        if self._LOGGER.getEffectiveLevel() == 10:
            self._ourDebug(bucket, idObj,utime,uid, gid , context)          
        msg = JsonRPCPayload('setObjectUtime',[idObj,utime,uid,gid,context])
        self._send(msg)
        return msg.get('result')
    
    # X ATTRIBUTES
    ########################    
    def getObjectXattr(self,bucket, idObj, name, uid, gid,context):
        self._LOGGER.info("Getting object Xattr ")
        if self._LOGGER.getEffectiveLevel() == 10:
            self._ourDebug(bucket, idObj,name,uid, gid , context)  
        msg = JsonRPCPayload('getObjectXattr',[idObj, name, uid, gid,context])
        self._send(msg)
        return msg.get('result')
       
    def setObjectXattr(self, bucket, idObj, name, value, uid, gid, context):
        self._LOGGER.info("Setting object Xattr ")
        if self._LOGGER.getEffectiveLevel() == 10:
            self._ourDebug(bucket, idObj,name, value,uid, gid , context)  
        msg = JsonRPCPayload('setObjectXattr',[idObj,name, value,  uid, gid,context])
        self._send(msg)
        return msg.get('result')
       
    def delObjectXattr(self, bucket, idObj, name, uid, gid, context):
        self._LOGGER.info("Deleting object Xattr ") 
        if self._LOGGER.getEffectiveLevel() == 10:
            self._ourDebug(bucket, idObj,name, uid, gid , context)  
        msg = JsonRPCPayload('delObjectXattr',[idObj, name, uid, gid,context])
        self._send(msg)
        return msg.get('result')
    
    def listObjectXattr(self, bucket, idObj, uid, gid, context):
        self._LOGGER.info("Listing object Xattr ")
        if self._LOGGER.getEffectiveLevel() == 10:
            self._ourDebug(bucket, idObj,uid, gid , context)   
        msg = JsonRPCPayload('listObjectXattr',[idObj, uid, gid,context])
        self._send(msg)
        return msg.get('result')   
    
    
    # Segment
    #######################
    def getObjectSegment(self, bucket,  idObj, idSeg, uid, gid, context):
        self._LOGGER.info("Getting object Segment ")
        if self._LOGGER.getEffectiveLevel() == 10:
            self._ourDebug(bucket, idObj,uid, gid , context)     
        '''field = 'segment-'+idSeg
        '########### filed del mio getObjectSegment', field
        item, meta = self.cacheMng.getMetaItem(idObj, field)
        '########### guardo se ho in cache meta e item',meta,item
       
        if item and meta['vers']==-1:
            return item
        
        if item and self._isValid(item, meta):
            return item
        
        if not self._isValid(item, meta):
            msg = JsonRPCPayload('getObjectVersion',[idObj,field,uid,gid,context])
            self._send(msg)

            if meta:
                if meta['vers'] == msg.get('result'):
                    print 'item',item
                    return item            

        '########### getto object segment'''
        msg = JsonRPCPayload('getObjectSegment',[idObj,idSeg, uid, gid,context])
        self._send(msg)
        item = msg.get('result')
        '''self.cacheMng.setMetaItem(idObj,field,item,item['vers'])'''
        return item 
    
    def delObjectSegment(self, bucket,  idObj, idSeg, uid, gid, context):
        self._LOGGER.info("Deleting object Segment ")
        if self._LOGGER.getEffectiveLevel() == 10:
            self._ourDebug(bucket, idObj,idSeg,uid, gid , context)
        msg = JsonRPCPayload('delObjectSegment',[idObj,idSeg, uid, gid,context])
        self._send(msg)
        return msg.get('result')      
    
    def setObjectSegment(self, bucket,  idObj, idSeg, seg, uid, gid, context):
        self._LOGGER.info("Setting object Segment ")
        if self._LOGGER.getEffectiveLevel() == 10:
            self._ourDebug(bucket, idObj,idSeg,seg,uid, gid , context)  
        msg = JsonRPCPayload('setObjectSegment',[idObj,str(idSeg), seg, uid, gid,context])
        self._send(msg)
        return msg.get('result')  
    
    
    #### storage operations ####
    #### Block operations   ####

    def writeBlock(self,bucket, idObj, key , block, bhash, uid=None, gid=None, context=None):
        self._LOGGER.info("Writing Block ")
        if self._LOGGER.getEffectiveLevel() == 10:
            self._ourDebug(bucket, idObj,key,block,bhash,uid, gid , context)  
        if isinstance(block,unicode):
            block = str(block)
        
        msg = JsonRPCPayload('writeBlock',[bucket,idObj, key, Binary(block,1), bhash, uid, gid,context])
        self._sendData(msg)
        item = msg.get('result')
        if not item:
            self._LOGGER.info("Writing Block in cache") 
            self.cacheMng.writeBlock(bucket, idObj, key, bhash, block)
            self.srv_num_set += 1
        return item  
    
    def removeBlock(self,bucket, idObj, key, uid=None, gid=None, context=None):
        self._LOGGER.info("Removing Block ")
        if self._LOGGER.getEffectiveLevel() == 10:
            self._ourDebug(bucket, idObj,key,uid, gid , context)
        msg = JsonRPCPayload('removeBlock',[bucket,idObj, key, uid, gid,context])
        self._sendData(msg)
        return msg.get('result')
        
    def removeSegment(self,bucket, idObj, key, uid=None, gid=None, context=None):
        self._LOGGER.info("Removing Segment ")
        if self._LOGGER.getEffectiveLevel() == 10:
            self._ourDebug(bucket, idObj,key,uid, gid , context) 
        msg = JsonRPCPayload('remove',[bucket,idObj, key, uid, gid,context])
        self._sendData(msg)
        return msg.get('result')  

    def readBlock(self, bucket, idObj, key, bhash, uid=None, gid=None, context=None):
        self._LOGGER.info("Reading Block ")
        if self._LOGGER.getEffectiveLevel() == 10:
            self._ourDebug(bucket, idObj,key,bhash,uid, gid , context) 
        item = self.cacheMng.readBlock(bucket, idObj, key, bhash)
        if item:
            self.srv_num_get += 1
            self._LOGGER.info("Founded in cache, returning it ") 
            return item
        self.srv_num_get_failed += 1
        self._LOGGER.info("NOT Founded in cache, writing it ") 
        msg = JsonRPCPayload('readBlock',[bucket,idObj, key, uid, gid,context])
        item = self._sendDataBinary(msg)
        self.srv_num_set += 1
        self.cacheMng.writeBlock(bucket, idObj, key, bhash, item)
        return item
    
    def writeOffset(self, bucket, object_name, data_handler, offset,  uid, gid, context):

        mode = ""
        idObj = self.lookup(bucket, object_name)  #the object must exixst      
        #Get Object Properties     
        obj = self.getProperties(bucket, idObj, OF.PROPERTY_OBJECT, uid, gid, context)
        idObj = obj['id']

        #self.setObjectAcl(bucket, idObj, OF.PROPERTY_ACL, object_acl, uid, gid, context)
       # da modificare in setAttributes
        
        offset = int(offset)
        #check if i can change it
        #self._checkObjectPerm(obj.id , context.user,obj,BF.ACL_WRITE)  
        # Object Dimension
        

        chunk_size         = len(data_handler)
        block_to_end       = int(math.ceil((offset + chunk_size)/obj['block_size']))
        block_to_start     = int(math.ceil(offset/obj['block_size']))
        chunk_ToTouch      = offset - (block_to_start*obj['block_size'])
        chunk_notToTouch   = int(((block_to_start*obj['block_size']) + chunk_ToTouch) - (block_to_start-1)*obj['block_size'])
        segment_to_start   = int(math.ceil(offset/(obj['segment_size']*obj['block_size'])))-1
        segmentId = segment_to_start
        seg_to_start = self.getObjectSegment(bucket, idObj, segmentId, uid, gid, context)
        segBlock_to_start = self.getObjectSegment(bucket, idObj, str(segmentId)+'-Bid', uid, gid, context)
        myMd5 = md5.new()
        block_pos = block_to_start-1
        myStringIO = cStringIO.StringIO(data_handler)
        
        for blockId in range(block_to_start,block_to_end+1) :
            
            if block_pos > obj['segment_size']:
                
                self.setObjectSegment(bucket, idObj, segmentId, seg_to_start, uid, gid, context)
                # Create a new segment numBlock BlockId
                self.setObjectSegment(bucket, idObj, str(segmentId)+'-Bid', segBlock_to_start, uid, gid, context)
                block_pos = 0
                segmentId += 1
                seg_to_start = self.getObjectSegment(bucket, idObj, segmentId, uid, gid, context)
                segBlock_to_start = self.getObjectSegment(bucket, idObj, str(segmentId)+'-Bid', uid, gid, context)
                
            block_pos += 1

            if chunk_notToTouch > 0:
                block_to_modify = segBlock_to_start[str(blockId)]
                old_block = self.readBlock(bucket, idObj, block_to_modify, seg_to_start[str(blockId)], uid, gid, context)
                old_bytes = cStringIO.StringIO(old_block)
                old_block = old_bytes.read(chunk_notToTouch)
                new_block = old_block + (myStringIO.read(int(obj['block_size'])))
                chunk_notToTouch = 0
            
            else:
                new_block = myStringIO.read(int(obj['block_size']))
                
            bhash = self._generateHash(new_block)
            
            if seg_to_start.has_key(blockId):
                bkey     = segBlock_to_start[blockId]
            else:
                bkey  = self._getBlockKey(bucket, idObj, segmentId, blockId)            
            
            self.writeBlock(bucket, idObj, bkey, new_block, bhash, uid, gid, context)
            myMd5.update(new_block) 
            seg_to_start[blockId] = bhash
            segBlock_to_start[blockId] = bkey
                
        #save the last segment 
        self.setObjectSegment(bucket, idObj, segmentId, seg_to_start, uid, gid, context)      
        # Create a new segment numBlock BlockId   
        self.setObjectSegment(bucket, idObj, str(segmentId)+'-Bid', segBlock_to_start, uid, gid, context)
        
        #FIX AGGIORNARE METADATAS
        #FIX METTER IL TRUNCATE DALLA MIA FINE


    def loadOffset (self, bucket, idObj, offset, uid=None, gid=None, context=None):
        
        offset = int(offset)
        objDict = self.getProperties(bucket, idObj, OF.PROPERTY_OBJECT, uid, gid, context)
        block_pos = int(math.ceil(offset/objDict['block_size']))
        segmentId = int(math.ceil(offset/(objDict['segment_size']*objDict['block_size'])))-1
        segBlock = self.getObjectSegment(bucket, idObj, str(segmentId)+'-Bid', uid, gid, context)
        seg =      self.getObjectSegment(bucket, idObj, segmentId, uid, gid, context)
        streamData = '' # IOqulcosa 
        #if not self.metaMng.open(obj.id, context)
        for blockId in range(block_pos,objDict['block_counter']) :
            if block_pos > objDict['segment_size']:
                block_pos = 0
                segmentId += 1
                segBlock = self.getObjectSegment(bucket, idObj, str(segmentId)+'-Bid', uid, gid, context)
                seg =      self.getObjectSegment(bucket, idObj, segmentId, uid, gid, context)
            data_block = self.readBlock(bucket, idObj, segBlock[str(blockId)], seg[str(blockId)], uid, gid, context)
            block_pos += 1
            if data_block:
                streamData += data_block
    
        #for sinchronize it FIX
        #self.metaMng.close(obj.id, context)
        return streamData    
        
        
        
        

    #### "global" object operaitons
    
    def write(self, bucket, object_name, uid, gid, context, storage_class, object_acl ,content_type, xattr,  data_handler):
        #context oggetto
        #user id buckname idgrouputente [+ gruppi] sessione
        mode = ""
        # Operation Type based on object existance
        idObj = self.lookup(bucket, object_name)  #FIX TH PATH
        if idObj == None:
            idObj = OF.ENOENT       
        
        
        # Create a new Object (object not found)
        if idObj == OF.ENOENT:  
            object_type = OF.TYPE_FILE
            if object_name[-1] == '/' :
                #the object is a directory
                object_type = OF.TYPE_DIR

            idObj = self.createObject(bucket, object_name, object_type, '', uid, gid, context) 

            
            if idObj == OF.EACCESS:
                raise RestFSError(errCode.err['AuthorizationDeny']['code'],\
                              errCode.err['AuthorizationDeny']['message'],\
                              errCode.err['AuthorizationDeny']['http'])  
                return
            elif idObj == OF.ENOENT:
                raise RestFSError(errCode.err['ObjectNotFound']['code'],\
                          errCode.err['ObjectNotFound']['message'],\
                          errCode.err['ObjectNotFound']['http'])
                
        #Get Object Properties     
        obj = self.getProperties(bucket, idObj, OF.PROPERTY_OBJECT, uid, gid, context)
        idObj = obj['id']

        #self.setObjectAcl(bucket, idObj, OF.PROPERTY_ACL, object_acl, uid, gid, context)
        #da modificare in setAttributes
        
        #check if i can change it
        #self._checkObjectPerm(obj.id , context.user,obj,BF.ACL_WRITE)  
        # Object Dimension
        file_size = len(data_handler)
        block_counter       = int(math.ceil(file_size/obj['block_size']))
        block_counter_old   = obj['block_counter']
        segment_counter     = int(math.ceil(file_size/(obj['segment_size']*obj['block_size'])))
        segment_counter_old = obj['segment_counter']
        
        # Init struct for save data
        segmentId = 0
        seg = self.getObjectSegment(bucket, idObj, segmentId, uid, gid, context)
        segBlock = self.getObjectSegment(bucket, idObj, str(segmentId)+'-Bid', uid, gid, context)
        myMd5 = md5.new()
        block_pos = 0
        myStringIO = cStringIO.StringIO(data_handler)
        
        for blockId in range(1,block_counter+1) :
            
            if block_pos > obj['segment_size']:
                
                self.setObjectSegment(bucket, idObj, segmentId, seg, uid, gid, context)
                # Create a new segment numBlock BlockId
                self.setObjectSegment(bucket, idObj, str(segmentId)+'-Bid', segBlock, uid, gid, context)
                block_pos = 0
                segmentId += 1
                seg = self.getObjectSegment(bucket, idObj, segmentId, uid, gid, context)
                segBlock = self.getObjectSegment(bucket, idObj, str(segmentId)+'-Bid', uid, gid, context)
                
            block_pos += 1
            new_block = myStringIO.read(int(obj['block_size']))
            bhash = self._generateHash(new_block)
            bkey  = self._getBlockKey(bucket, idObj, segmentId, blockId)

            if seg.has_key(blockId):
                hash_old = seg[blockId]
                bkey     = segBlock[blockId]
            else:
                hash_old = None
                bkey = self._getBlockKey(bucket, idObj, segmentId, blockId)
            
            if bhash != hash_old:
                self.writeBlock(bucket, idObj, bkey, new_block, bhash, uid, gid, context)
                myMd5.update(new_block) 
                seg[blockId] = bhash
                segBlock[blockId] = bkey
                
        #save the last segment 
        self.setObjectSegment(bucket, idObj, segmentId, seg, uid, gid, context)      
        # Create a new segment numBlock BlockId   
        self.setObjectSegment(bucket, idObj, str(segmentId)+'-Bid', segBlock, uid, gid, context)	   
        
        if block_counter_old > block_counter:
            
            for i in range(blockId,blockId + (len(seg)-block_pos)):
                
                self.removeBlock(bucket, idObj, segBlock[i+1], uid, gid, context)
                del segBlock[i+1]
                del seg[i+1]
                
            self.setObjectSegment(bucket, idObj, segmentId, seg, uid, gid, context) 
            self.setObjectSegment(bucket, idObj, str(segmentId)+'-Bid', segBlock, uid, gid, context)
               
        #Check if i have to delete segments
        if  segment_counter < segment_counter_old:

            for segId in range (segment_counter+1,segment_counter_old+1):
                segBlock = self.getObjectSegment(bucket, idObj, str(segId)+'-Bid', uid, gid, context)
                self.delObjectSegment(bucket, idObj, segId, uid, gid, context)
                self.removeSegment(bucket, idObj, segBlock, uid, gid, context)

        obj['block_counter']   = block_counter       
        obj['segment_counter'] = segment_counter
        obj['size']            = file_size
        obj['num_segments']    = segmentId
        #FIXME do a incremental job .. 
        obj['md5']            =  myMd5.hexdigest()
        
        self.setAttributes(bucket, idObj,obj, uid, gid, context)
        return obj

  
    
    def load(self, bucket, idObj, uid=None, gid=None, context=None):
        
        objDict = self.getProperties(bucket, idObj, OF.PROPERTY_OBJECT, uid, gid, context)
        block_pos = 0
        segmentId = 0
        seg = self.getObjectSegment(bucket,idObj,segmentId, uid ,gid, context)
        segBlock = self.getObjectSegment(bucket,idObj,str(segmentId)+'-Bid', uid ,gid, context)
        streamData = '' # IOqulcosa 
        #if not self.metaMng.open(obj.id, context)
        for blockId in range(1,objDict['block_counter']+1) :
            if block_pos > objDict['segment_size']:
                block_pos = 0
                segmentId += 1
                segBlock = self.getObjectSegment(bucket,idObj,str(segmentId)+'-Bid', uid ,gid, context)
            block_pos += 1
            data_block = self.readBlock(bucket, idObj, segBlock[str(blockId)], seg[str(blockId)])
            if data_block:
                streamData += data_block
    
        #for sinchronize it FIX
        #self.metaMng.close(obj.id, context)
        return streamData
    
    def open(self, bucket, idObj, flags, uid=None, gid=None, context=None):
        stream_data = self.load(bucket, idObj, uid, gid, context)
        objDict = self.getProperties(bucket, idObj, OF.PROPERTY_OBJECT, uid, gid, context)
        myFile = open('/tmp/'+objDict['object_name'],'w')
        myFile.write(stream_data)
        myFile.close()
        return None
    
    def remove(self,bucket, idObj, uid=None, gid=None, context=None):
        msg = JsonRPCPayload('removeObject',[bucket,idObj, uid, gid,context])
        self._sendData(msg)
        return msg.get('result')
    
    def readLocalFile(self,path):
        file = open(path,"r")
        streamData = file.read()
        file.close()
        pathList = path.split('/')
        file_name = pathList[-1] 
        return streamData, file_name

    #
    # Internal Sendid call 
    #######################
    
    def _isValid(self, item, meta):
        self._LOGGER.info("Checking if item is valid ")
        self._LOGGER.debug("item %s",item) 
        self._LOGGER.debug("meta %s",meta) 
 
        if not item or meta:
            return False
        if item['vers']==meta['vers']:
            return True
        return False
     
    ####Internal Debug
    #### 
    def _ourDebug(self,*param):
        for i in param:
            self._LOGGER.debug("PARAMS %s" % i)  
             
    
# Get the key of the block 
    def _getBlockKey(self,bucket_name,idObject,segmentId,blockId):
        idBlock = str(time.time())+'.'+str(idObject)+'.'+str(segmentId)+'.'+str(blockId)
        return bucket_name+'.'+str(hashlib.sha1(idBlock).hexdigest())

      
    def _generateHash(self, block_data):
        return hashlib.sha1(block_data).hexdigest()
    
    
    def _send(self,msg):
        
        data = self.conn_meta.send(msg.getPacket())
        
        msg.load(data)
        
    def _sendData(self,msg):
        data = self.conn_data.send(msg.getPacket())
        msg.load(data)


    def _sendService(self,msg):
        data = self.conn_service.send(msg.getPacket())
        msg.load(data)
        #CHANGED FOR BINARY FILES
        # FIXME
    def _sendDataBinary(self,msg):
        data = self.conn_data.send(msg.getPacket())
        #msg.load(data)
        return data
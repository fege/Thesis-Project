import hashlib
import logging
import time
import restfs.factories.ObjectFactory as OF
import restfs.factories.BucketFactory as BF
import restfs.factories.ErrorCodeFacotry as errCode
from tornado.options import options
from restfs.objects.RestFSError import RestFSError
from restfs.objects.ObjectProperties import ObjectProperties
from restfs.objects.BucketProperties import BucketProperties

def rpcmethod(func):
        """ Decorator to expose Node methods as remote procedure calls
       
        Apply this decorator to methods in the Node class (or a subclass) in order
        to make them remotely callable via the DHT's RPC mechanism.
        """
        func.rpcmethod = True
        return func



class MetadataManager (object):
    
    _LOGGER = logging.getLogger('METADATA_MANAGER')
    def __init__(self):
        
        #Bucket METADATA
        meta_plugin = "restfs.metadata.%s.MetadataBucketDriver" % options.metadata_driver
        meta_mod = __import__(meta_plugin, globals(), locals(), ['MetadataBucketDriver'])
        MetadataBucketDriver = getattr(meta_mod, 'MetadataBucketDriver')
        self.metaBuck = MetadataBucketDriver()
        
        #Object METADATA
        meta_plugin = "restfs.metadata.%s.MetadataObjectDriver" % options.metadata_driver
        meta_mod = __import__(meta_plugin, globals(), locals(), ['MetadataObjectDriver'])
        MetadataObjectDriver = getattr(meta_mod, 'MetadataObjectDriver')
        self.metaObj = MetadataObjectDriver()
        
        # CACHE
         
    # This is the same as the access(2) system call. It returns -ENOENT if the path doesn't exist,
    # -EACCESS if the requested permission isn't available, or 0 for success. Note that it can be called on files, 
    # directories, or any other object that appears in the filesystem. 
    def access(self, bucket_name, path, context):

        self._LOGGER.info("Access operation")        

        idObject = self.lookup(bucket_name, path)
        
        if idObject == None:
            return OF.ENOENT
        
        #self._checkObjectPerm(objId ,context.user ,self.s3AclToPerm(BF.ACL_READ))
                
        return idObject  
            
    # Return file attributes. For the given pathname, this should fill in the elements of the "stat" structure. 
    # If a field is meaningless or semi-meaningless (e.g., st_ino) then it should be set to 0 or given a "reasonable" value.
    #
    # Similar to Dir Read
    @rpcmethod
    def getObjectList(self,idObject, uid, gid, context, meta=None, pos=None, size=None, filter=None):
        # bucket,
        obj = self._getObject(idObject)
        if not obj:
            return None
        
        if (obj.object_type != OF.TYPE_DIR and obj.object_type != OF.TYPE_ROOT):
            return None
        
        myDict = self.metaObj.getSegment(idObject,0)
        
        myList = myDict.keys()
        
        if 'vers' in myList:
            myList.remove('vers')  
        
        if filter:
            pass
        
        if pos:
            myList = myList[pos:]
        
        if size:
            myList = myList[:size]
        
        if meta:
            newList = []
            for key in myList:
                obj = self._getObject(myDict[key])
                newList.append(obj)
            myList = newList
            
        return myList
        

    @rpcmethod
    def createObject (self, bucket_name, path,  object_type, mode, uid, gid, context):
        self._LOGGER.info("Create object") 
        if path == '/':
            return OF.EACCESS
        if path[0] != '/':
            return OF.EACCESS
        if path[-1] == '/':
            path= path[:-1]
            
        s_pos = path.rfind('/')
        # Check exist
        objId = self.lookup( bucket_name, path) 
        if objId:
            return OF.EACCESS
         
        #Split path
        self._LOGGER.info("Splitting path object")  
        parent_path  = path[:s_pos]
        object_name  = path[s_pos+1:]
        self._LOGGER.debug('parent_path : %s' % parent_path) 
        self._LOGGER.debug('object_name : %s' % object_name) 

        
        # FIXME CHECK NAME
        #........
        
        #Check permission
        idFather = self.access(bucket_name, parent_path, context )
        if idFather == OF.EACCESS or idFather == OF.ENOENT:
            return idFather
        fatherObj = self.metaObj.getProperty(idFather,OF.PROPERTY_OBJECT)
        if fatherObj.object_type == OF.TYPE_FILE or fatherObj.object_type == OF.TYPE_MOUNT:
            self._LOGGER.warning('The parent object is not a Directory or a Root')
            return OF.EACCESS
        idObj = self._createObjectKey(bucket_name, object_name,idFather)           
        obj = ObjectProperties()
        obj.id          = idObj
        obj.uid         = uid
        obj.object_type = object_type
        obj.bucket_name = bucket_name
        obj.object_name = object_name
        obj.id_parent   = idFather
        self.metaObj.create(obj.id,obj)  
        
        # Link To parent increase counter .. 
        
        seg = self.metaObj.getSegment(idFather, 0)
        seg[obj.object_name] = obj.id
        self.metaObj.setSegment(obj.id_parent,0,seg)
        return idObj

 
    def open(self,objId, context):
        pass
    
    def close(self,objId, context):
        pass
    
    
    # Rename the file, directory, or other object "from" to the target "to". 
    # Note that the source and target don't have to be in the same directory, 
    # so it may be necessary to move the source to an entirely new directory. 
    # See rename(2) for full details.
    def rename(self,objectID,to_path,context):
        pass
    
    # Change properties of the given object. 
    # See chmod(2) for details,
    # ALL ATTIBUTES ? 

        

    # Truncate or extend the given file so that it is precisely size bytes long. 
    # See truncate(2) for details. This call is required for read/write filesystems, 
    # because recreating a file will first truncate it.
    def truncate(self,objectID,size,context):      
        pass
    

    # Perform a POSIX file-locking operation. See details below.
    def lock(self, objectID, lock_type, context):
        pass
    
    # Perform a POSIX file-locking operation. See details below.
    def unlock(self, objectID, lock_type, context):
        pass

    # 
    def subscribe(self, objectID, type, context, timeout=100):
        pass
    
    #
    def unsubscribe(self, objectID, type, context):
        pass

    # 

    @rpcmethod
    def removeObject(self, idObject, uid, gid, context):
        self._LOGGER.info("Deleting object")
        # Check if exist ...     
        obj = self.metaObj.getProperty(idObject,OF.PROPERTY_OBJECT)
        seg = self.metaObj.getSegment(obj.id_parent, 0)        
        self.metaObj.remove(idObject) 
                   
        # deleting idObj in segment 0 of the father
        del seg[obj.object_name]
        self.metaObj.setSegment(obj.id_parent, 0, seg)
 

    # NOT DEFINED AT THE MOMENT !!
    def statfs(self):
        pass
    
    def mknod(self):
        pass
    
    def ioctl(self):
        pass
    
    def flush(self):
        pass
            
    @rpcmethod
    def lookup(self, bucket_name, path):
        self._LOGGER.info("Lookup operation")                        
        self._LOGGER.debug('Object path : %s' % path) 
        self._LOGGER.debug('Bucket-name : %s' % bucket_name) 
        
        #pathList = path.split('/')
        key =  bucket_name+".ROOT"

        # Special case it is the root 
        if path == "/":
            return key
        
        if path.endswith('/'):
            path= path[:-1]
        
        pathList = path.split('/')
        
        if pathList[0] == '':
            pathList = pathList[1:]
        self._LOGGER.debug('Path List : %s' % pathList) 
        
        
        for el in pathList:
            
            obj = self.metaObj.getProperty(key,OF.PROPERTY_OBJECT)
            
            if (obj== None) or (obj.object_type != OF.TYPE_DIR and obj.object_type != OF.TYPE_ROOT):
                return None
                        
            seg = self.metaObj.getSegment(key, 0)
                        
            #check if we have segment 0
            if len(seg) < 1:
                return None
            
            if not seg.has_key(el):
                return None

            key = seg[el]
            self._LOGGER.debug('Key Father : %s' % key) 
            
        return key  
    
    
    
    # Properties 
    ##############################################################
    @rpcmethod
    def getObjectXattr(self,idObject,name,uid, gid,context):
        #FIXME Permission
        #FIXME Return TYPE, dict or list
        return self.metaObj.getProperty(idObject,name)
       
    @rpcmethod
    def setObjectXattr(self, idObject, name, value,uid, gid, context):
        #FIXME Permission
        #FIXME Return TYPE, dict or list
        self.metaObj.setProperty(idObject, name,value)
    
    @rpcmethod   
    def delObjectXattr(self,idObject, name, value, uid, gid, context):
        #FIXME Permission / Name... 
        #FIXME Return TYPE, dict or list
        self.metaObj.delProperty(idObject, name)
    
    @rpcmethod
    def listObjectXattr(self, idObject, uid, gid, context):
        #FIXME PErmission
        propList = self.metaObj.getKeyList.getPropertiesList(idObject)
        list = []
        for i in propList:
            if i.startswith(OF.PROPERTY_USER):
                list.append(i)
        return list
    
        # Specific for ACL
    @rpcmethod
    def getObjectProperty(self,idObject,field, uid, gid, context):
        #FIXME Permission
        #FIXME DICT
        obj = self.metaObj.getProperty( idObject, field)
        return obj
    
    @rpcmethod
    def getObjectVersion(self,idObject, field, uid, gid, context):
        #FIXME Permission
        #FIXME DICT
        obj = self.getAttributes(idObject, field, uid, gid, context)
        print 'obj da cui prendo la versione', obj
        print 'versione' ,obj['vers']
        return  obj['vers']
    
    
    @rpcmethod
    def getAttributes(self,idObject,field, uid, gid, context):
        #FIXME Permission
        #FIXME DICT
        obj = self.metaObj.getProperty( idObject, field)
        
        myDict={}
        myDict['bucket_name']   = obj.bucket_name  
        myDict['object_name']   = obj.object_name
        myDict['id']            = obj.id
        # myDict['uid']           = obj.uid
        # myDict['gid']           = obj.gid
        myDict['mode']           = int(obj.mode)
        myDict['last_modify']   = obj.last_modify 
        myDict['size']          = obj.size          
        myDict['md5']           = obj.md5    
        myDict['uid']           = obj.uid
        myDict['id_parent']     = obj.id_parent    
        myDict['compress']      = obj.compress      
        myDict['storage_class'] = obj.storage_class 
        myDict['content_type']   =  obj.content_type
        myDict['content_language']   =  obj.content_language
        myDict['read_counter']   =  obj.read_counter
        myDict['content_encoding']   =  obj.content_encoding  
        myDict['creation_date']   =  obj.creation_date
        myDict['object_type']     =  obj.object_type
        myDict['segment_size']    =  obj.segment_size  
        myDict['block_size']      =  obj.block_size
        myDict['block_counter']   =  obj.block_counter
        myDict['segment_counter'] =  obj.segment_counter
        myDict['vers']            =  obj.vers

        #ours
        myDict["max_read"] = obj.max_read
               
        return myDict
    
    @rpcmethod
    def setAttributes(self, idObject, objDict, uid,gid, context):
        
        obj = self.metaObj.getProperty(idObject, OF.PROPERTY_OBJECT)

        for key in objDict:
            obj.__setattr__(key,objDict[key])

        self.metaObj.setProperty(idObject, OF.PROPERTY_OBJECT, obj)

    @rpcmethod
    def setAttributesFromNew(self, idObject, objDict, uid,gid, context):
        
        obj = ObjectProperties()

        for key in objDict:
            obj.__setattr__(key,objDict[key])

        self.metaObj.setProperty(idObject, OF.PROPERTY_OBJECT, obj)
        
        
       
    @rpcmethod
    def setObjectProperty(self,idObject, field, value, uid, gid, context):
        #FIXME Permission
        self.metaObj.setProperty(idObject, field, value)
    
    
    @rpcmethod
    def setObjectMode(self,idObject, mode, uid, gid, context):
        #FIXME Permission
        obj = self.metaObj.getProperty( idObject, OF.PROPERTY_OBJECT)
        obj.mode = mode
        self.metaObj.setProperty(idObject, OF.PROPERTY_OBJECT, obj)
    
    @rpcmethod
    def setObjectOwner(self,idObject, owner, group, uid, gid, context):
        #FIXME Permission
        obj = self.metaObj.getProperty( idObject, OF.PROPERTY_OBJECT)
        obj.uid = owner
        obj.gid = group 
        self.metaObj.setProperty(idObject, OF.PROPERTY_OBJECT, obj)
    
    @rpcmethod
    def setObjectUtime(self,idObject, utime, uid, gid, context):
        #FIXME Permission
        obj = self.metaObj.getProperty( idObject, OF.PROPERTY_OBJECT)
        obj.last_modify = utime
        self.metaObj.setProperty(idObject, OF.PROPERTY_OBJECT, obj)   
    
    # Specific for ACL
    def getObjectACL(self,idObject,uid, gid, context):
        #FIXME Permission
        return self.metaObj.getProperty( idObject, OF.PROPERTY_ACL)
       
    
    def setObjectACL(self,idObject, value, context):
        #FIXME Permission
        self.metaObj.setProperty(idObject, OF.PROPERTY_ACL, value)
        
    
    # Perform a POSIX file-locking operation. See details below.
    @rpcmethod
    def getObjectSegment(self, objectID, segmentId,  uid, gid, context):
        self._LOGGER.info("Getting object-segment, if any")         
        return self.metaObj.getSegment(objectID,segmentId)
    
    @rpcmethod    
    def delObjectSegment(self, objectID, segmentId,  uid, gid, context):
        self._LOGGER.info("Deleting object-segment, if any")        
        return self.metaObj.delSegment(objectID,segmentId)

    @rpcmethod
    def setObjectSegment(self, objId,segmentId, seg,  uid, gid, context):
        self._LOGGER.info("Setting segment")        
        return self.metaObj.setSegment(objId,segmentId,seg)
    
       
    
    #########################################################################
    # BUCKET RELEATED
    #########################################################################
    @rpcmethod
    def createBucket(self, bucket_name, acp, location, uid, gid, context):
        #FIXME Check Permission !!!(one kind?)
        prop = BucketProperties(uid, gid, bucket_name, location)
        root = ObjectProperties(bucket_name+".ROOT", bucket_name, '/', uid, gid, object_type=OF.TYPE_ROOT)
        self._LOGGER.info("Create bucket metadatas")        
        self.metaBuck.create(bucket_name, prop, location)
        #for the root
        self.metaObj.create(root.id,root,acp)
        
        
    @rpcmethod
    def removeBucket(self,bucket_name, context):
        #FIXME Check Permission !!!(one kind?)
        self._LOGGER.info("Delete bucket metadatas")
        keys = self.metaObj.getKeyList(bucket_name+".")
        if keys:
            for key in keys:
                self.metaObj.remove(key)
        self.metaBuck.delete(bucket_name)
        
        
    def setBucketProperty(self, bucket_name, prop_name, prop , context):
        #FIXME CHECK Permission
        
        #FIXME Check Prop Name and Validation
        self._LOGGER.info("Setting bucket properties")        
        self.metaBuck.setProperty(bucket_name,prop_name, prop)  
        
    def getBucketProperty(self, bucket_name, prop_name, context):
        #FIXME CHECK Permission
        
        #FIXME Check Prop Name and Validation
        self._LOGGER.info("Getting bucket properties")        
        return self.metaBuck.getProperty(bucket_name,prop_name)  
    
    def delBucketProperty(self, bucket_name, prop_name, context):
        #FIXME CHECK Permission
        
        #FIXME Check Prop Name and Validation
        self._LOGGER.info("Getting bucket properties")        
        return self.metaBuck.delProperty(bucket_name,prop_name)  
    
    
    # More Alias than true function
    def setBucketACL(self, bucket_name, acp , context):
        self._LOGGER.info("Setting bucket properties") 
        idObject = bucket_name+".ROOT"
        self.setObjectACL(idObject, acp, context)
        #FIXME Check Prop Name and Validation
       
        
    def getBucketACL(self, bucket_name,uid, gid, context):
        self._LOGGER.info("Getting bucket properties")      
        idObject = bucket_name+".ROOT"
        return self.getObjectACL(idObject, uid, gid, context)  
        
          
     
    #FIXME  STATISTIC 
    def refreshBucketProperties(self,bucket_name, objI, objSize, numBlocks):  
        self._LOGGER.info("Refreshing bucket properties")              
        buck = self.metaBuck.getProperty(bucket_name, BF.PROPERTIES_BUCKET)
        """
        buck.objectCounter      = buck.objectCounter + 1
        buck.space_used         = buck.space_used + objSize
        buck.blocks_number      = buck.blocks_number + numBlocks 
        self.metaBuck.setProperties(bucket_name,BF.PROPERTIES_BUCKET,buck)
        # FIXME 
        self._LOGGER.debug('Bucket Object-counter : %s' % buck.objectCounter) 
        self._LOGGER.debug('Bucket Space-used : %s' % buck.space_used)
        self._LOGGER.debug('Bucket Blocks-number : %s' % buck.blocks_number)      
        """
        
        
    def refreshDelBucketProperties(self,bucket_name, objI, objSize, numBlocks):
        self._LOGGER.info("Refreshing bucket properties")                        
        buck = self.metaBuck.getProperty(bucket_name,BF.PROPERTIES_BUCKET)
        buck.objectCounter      = buck.objectCounter - 1
        buck.space_used         = buck.space_used - objSize
        buck.blocks_number      = buck.blocks_number - numBlocks
        self.metaBuck.setProperty(bucket_name,BF.PROPERTIES_BUCKET,buck)
        # FIXME
        self._LOGGER.debug('Bucket Object-counter : %s' % buck.objectCounter) 
        self._LOGGER.debug('Bucket Space-used : %s' % buck.space_used)
        self._LOGGER.debug('Bucket Blocks-number : %s' % buck.blocks_number)   
           
    # Generate the hash of the block
    def generateHash(self, block_data):
        self._LOGGER.info("SET Block Hash") 
        return hashlib.sha1(block_data)
    
    # Get the key of the block 
    def getBlockKey(self,bucket_name,idObject,segmentId,blockId):
        self._LOGGER.info("GET Block Key") 
        idBlock = str(time.time())+'.'+str(idObject)+'.'+str(segmentId)+'.'+str(blockId)
        return bucket_name+'.'+str(hashlib.sha1(idBlock).hexdigest())
    
    
         
    #########################################################################
    # INTERNAL 
    #########################################################################
     
    # Get the key of the block 
    def _getBlockKey(self,idBlock,idSegment,idObject):
        self._LOGGER.info("GET Block Key")
        bKey = str(time.time()) +"."+ str(idBlock) + "." + str(idSegment) + "."+ str(idObject)
        return self.generateHash(bKey).hexdigest()
    
    def _createObjectKey(self,bucket_name,object_name, idFather):
        self._LOGGER.info("Creating object key") 
        idObj  = str(time.time())+'.'+idFather+'.'+object_name
        return bucket_name+'.'+str(hashlib.sha1(idObj).hexdigest())
    
    def _getChecksum(self,block):
        self._LOGGER.info("Getting checksum of the block")                        
        return hashlib.sha1(block)
    
    def _getObject(self,idObject):        
        return self.metaObj.getProperty(idObject,OF.PROPERTY_OBJECT)
        
    def _setObject(self,objProperties):
        pass
    
    def _lookup(self, bucket, path):
        pass
    
    def _mkRootDir(self, path, permission):
        pass
    
        
    def _checkObjectPerm(self,idObject, user, op):
        
        self._LOGGER.info("Check if the user has permission on the bucket")
        self._LOGGER.debug('Permission-type : %s' % op)
    
        acp =  self.getObjectAcp(idObject)
        obj =  self.getObjectProperties(idObject)

        if user.uid == obj.uid:
            return True
        
        if acp.grantee(user,op):
            return True
       
        #Permission deny no acp  
        self._LOGGER.warning('The user cannot do this operation the bucket')
        raise RestFSError(errCode.err['AuthorizationDeny']['code'],\
                              errCode.err['AuthorizationDeny']['message'],\
                              errCode.err['AuthorizationDeny']['http']) 

    
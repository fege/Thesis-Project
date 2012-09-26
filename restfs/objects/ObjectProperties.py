import time
import restfs.factories.ObjectFactory as OF

class ObjectProperties(object):
    
    def __init__(self, id="", bucket_name="", object_name="", uid="" , id_parent="", last_modify=time.time(), size=0, md5="", \
                  storage_class="STANDARD", content_type=None, xattr=[], max_read = 1000, read_counter = 0, \
                  object_type = 'file', segment_size=512, block_size=16000.0, segment_counter=0, previous=(0,0), num_segments=0,\
                gid="", mode=OF.CHMOD_TYPE_ALL+OF.CHMOD_TYPE_ALL+OF.CHMOD_TYPE_ALL, block_counter = None, vers=1):

        #Generic Properties
        self.bucket_name      = bucket_name
        self.object_name      = object_name
        self.id               = id
        self.id_parent        = id_parent
        self.object_type      = object_type
        self.uid              = uid
        self.gid              = gid
        self.mode             = mode
        self.size             = size
        self.md5              = md5 
        self.compress         = None

        #Content properties
        self.content_type     = content_type
        self.content_language = None
        self.content_encoding = None
        
        #Time and Version
        self.last_modify      = last_modify
        self.creation_date    = time.time()
        self.vers             = self.creation_date

        #Storage properties
        self.storage_class    = storage_class
        self.segment_size     = segment_size
        self.block_size       = block_size
        self.segment_counter  = segment_counter
        self.block_counter    = block_counter
        
        #previous is the last versione of the object, stored with this format (idObject, idParent) / Snapshot
        self.previous         = previous
        
        #Statistics
        self.max_read     = max_read
        self.read_counter = read_counter
        #self.revision = 'puntatore !'
        self.xattr        = xattr
        self.vers         = vers
        
    def getXattr(self):
        return self.xattr
    
    def setXattr(self, xattr):
        self.xattr = xattr
        
    def appendXattr(self,xattr):
        self.xattr.append(xattr)

    def setByDict(self,myDict):      
        for key in myDict:
            setattr(self,key,myDict[key])

        #self.last_modified = str(datetime.datetime.fromtimestamp(time.time()))
        #FIXME remove in final version
        if self.last_modify:
            self.last_modify = float(self.last_modify)
        if self.creation_date:
            self.creation_date = float(self.creation_date)
        if self.xattr == "None":
            self.xattr =  None
            
    def getDict(self):
        myDict={}
        myDict['bucket_name']   = self.bucket_name  
        myDict['object_name']   = self.object_name
        myDict['id']            = self.id
        myDict['uid']           = self.uid
        myDict['gid']           = self.gid
        myDict['mode']           = int(self.mode)
        myDict['last_modify']   = self.last_modify 
        myDict['size']          = self.size          
        myDict['md5']           = self.md5    
        myDict['uid']           = self.uid
        myDict['id_parent']     = self.id_parent    
        myDict['compress']       = self.compress      
        myDict['storage_class']  = self.storage_class 
        myDict['xattr']          = self.xattr 
        myDict['content_type']   =  self.content_type
        myDict['content_language']   =  self.content_language
        myDict['block_counter']   =  self.block_counter
        myDict['content_encoding']   =  self.content_encoding  
        myDict['creation_date']   =  self.creation_date
        myDict['object_type']    =  self.object_type
        myDict['segment_size']   =  self.segment_size  
        myDict['block_size']     =  self.block_size
        #ours
        myDict["max_read"]       = self.max_read
        myDict["read_counter"]   = int(self.read_counter) + 1
        myDict["vers"]           = self.vers


        
        return myDict
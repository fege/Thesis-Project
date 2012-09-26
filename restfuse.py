#!/usr/bin/env python

from collections import defaultdict
from errno import ENOENT 
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time
from tornado import httpserver
from tornado import ioloop
from tornado import web
import sys
import tornado

from fusepy.fuse24 import FUSE, FuseOSError, Operations, LoggingMixIn, fuse_get_context
from tornado.options import define, options
import restfs.factories.ObjectFactory as OF
from restfsc.manager.ConnectionManager import ConnectionManger
from restfsc.service.CacheService import CacheService
from restfs.objects.Grant import Grant
import restfs.factories.S3ACPFactory as S3ACP
import restfs.utils.ACPHelper as ACPHelper
from restfs.objects.ACP import ACP


define("conf"   ,         default="restfs.cnf"  , help="configuration file")
define("root_bucket",       default='yyyy', help="port of resource locator")


class RestFSClient(LoggingMixIn, Operations):
    
    def __init__(self):
        self.cache = CacheService()

        
        self.files = {}
        self.data  = defaultdict(str)
        self.fd    = 0
        now        = time()
        self.files['/'] = dict(st_mode=(S_IFDIR | 0755), st_ctime=now,
           st_mtime=now, st_atime=now, st_nlink=2)
        
    def access (self, path, mode):
        pass

    def chmod(self, path, mode):
        
        print '#################'
        print '##### CH MOD  ###'
        print '#################'

        uid, gid, pid = fuse_get_context()
        idObj = self.cache.lookup(options.root_bucket,path) 
        context = ""     
        prop = self.cache.getProperties(options.root_bucket, idObj, OF.PROPERTY_OBJECT, uid, gid, context)
        prop.mode = mode
        self.cache.setAttributes(options.root_bucket, idObj, OF.PROPERTY_OBJECT, prop, uid, gid, context)
        
        self.files[path]['st_mode'] &= 0770000
        self.files[path]['st_mode'] |= prop.mode
        return 0

    def chown(self, path, uid, gid):
        
        print '#################'
        print '##### CH OWN  ###'
        print '#################'
        #uid and gid correct? are the new or old one ?
        old_uid, old_gid, pid = fuse_get_context()
        idObj = self.cache.lookup(options.root_bucket,path) 
        context = ""     
        prop = self.cache.getProperties(options.root_bucket, idObj, OF.PROPERTY_OBJECT, old_uid, old_gid, context)
        if gid & uid:
            prop.mode = uid+gid+prop.mode[-1]
        elif uid:
            prop.mode = uid+prop.mode[1:]
        self.cache.setAttributes(options.root_bucket, idObj, OF.PROPERTY_OBJECT, prop, uid, gid, context)
        
        self.files[path]['st_uid'] = uid
        self.files[path]['st_gid'] = gid
    
    def create(self, path, mode):
        
        print '#################'
        print '##### CREATE  ###'
        print path
        print '#################'
        uid, gid, pid = fuse_get_context()
        context = ""
        st_mode=(S_IFREG | mode)
        self.cache.createObject(options.root_bucket,path, OF.TYPE_FILE, uid, gid, st_mode, context)
        
        print '#################'
        print OF.TYPE_FILE
        print '#################'
        
        self.files[path] = dict(st_mode=(S_IFREG | mode), st_nlink=1,
           st_size=0, st_ctime=time(), st_mtime=time(), st_atime=time())
        self.fd += 1
        #FIXME idObj need a number 
        return self.fd 
       
    
    def getattr(self, path, fh=None):
        print '#################'
        print '##### GET-ATT ###'
        print '#################'

        uid, gid, pid = fuse_get_context()
        context = ""
        
        ## IL PROBLEMA su MAC e il lookup non il raise, l'unica e fare come fa lui
        ## e quindi aver la lista e controllare li.. eliminando il lookup
        
        try :
            print 'PATH',path
            idObj = self.cache.lookup(options.root_bucket,path)
            if idObj == None or idObj == 'null':
                raise FuseOSError(ENOENT) 
            print 'FH', idObj
            '''prop = self.cache.getProperties(options.root_bucket, idObj, OF.PROPERTY_OBJECT, uid, gid, context)
            self.files[path] = dict(st_mode=(prop['mode']), st_nlink=prop['read_counter'],\
                st_size=prop['size'], st_ctime=prop['creation_date'], st_mtime=prop['last_modify'], st_atime=time())
            '''
            #prop = self.cache.getProperties(options.root_bucket, idObj, OF.PROPERTY_OBJECT, uid, gid, context)
        except :
            raise FuseOSError(ENOENT)    
        
        st = self.files[path]
        return st
    
    
    def getxattr(self, path, name, position=0):
        
        print '#################'
        print '##### GetXattr ###'
        print '#################'
        attrs = self.files[path].get('attrs', {})
        try:
            return attrs[name]
        except KeyError:
            return ''       # Should return ENOATTR
   
    def listxattr(self, path):
        
        print '#################'
        print '##### listXattr  ###'
        print '#################'
        attrs = self.files[path].get('attrs', {})
        return attrs.keys()
     
    def mkdir(self, path, mode):
        
        print '#################'
        print '##### MKDIR  ###'
        print '#################'
        uid, gid, pid = fuse_get_context()
        context = ""
        
        st_mode = (S_IFDIR | mode)
        self.cache.createObject(options.root_bucket,path, OF.TYPE_DIR, uid, gid, st_mode, context)
        
        idObj = self.cache.lookup(options.root_bucket, path)
        prop = self.cache.getProperties(options.root_bucket, idObj,OF.PROPERTY_OBJECT,uid,gid,context)
        self.files[path] = dict(st_mode=(S_IFDIR | mode), st_nlink=prop['read_counter'],\
                st_size=prop['size'], st_ctime=prop['creation_date'], st_mtime=prop['last_modify'], st_atime=time())
        self.files['/']['st_nlink'] += 1
        
        print sys.exc_info()
    
    def open(self, path, flags):
        
        print '#################'
        print '##### OPEN  ###'
        print flags
        print '#################'
        uid, gid, pid = fuse_get_context()
        context = ""
        idObj = self.cache.lookup(options.root_bucket, path)
        stream = self.cache.load(options.root_bucket, idObj, uid, gid, context)
        #self.cache.open(options.root_bucket, path, flags, uid, gid, context)
        self.fd += 1
        return self.fd
    
    
    def read(self, path, size, offset, fh):    
        
        print '#################'
        print '##### READ  ###'
        print '#################'
        uid, gid, pid = fuse_get_context()
        context = ""
        idObj = self.cache.lookup(options.root_bucket, path)
        stream = self.cache.load(options.root_bucket, idObj, uid, gid, context)
        return self.data[path][offset:offset + size]    
    
    def readdir(self, path, fh):
        
        print '#################'
        print '##### READ DIR  ###'
        print '#################'
        uid, gid, pid = fuse_get_context()
        # FIXME Translation "." e ".."
        context = ""
        idObjDir = self.cache.lookup(options.root_bucket,path)
        lista = self.cache.getObjectList(options.root_bucket, idObjDir, uid, gid, context)
        propDir = self.cache.getProperties(options.root_bucket, idObjDir,OF.PROPERTY_OBJECT,uid,gid,context)
        dizio = dict()
        if len(lista) != 0:
            for elem in lista :
                elem = '/'+str(elem)
                if path != '/':
                    pathn = path+elem
                    idObj = self.cache.lookup(options.root_bucket,pathn)
                else:
                    idObj = self.cache.lookup(options.root_bucket,path)
                prop = self.cache.getProperties(options.root_bucket, idObj,OF.PROPERTY_OBJECT,uid,gid,context)
                dizio[elem] = dict(st_mode=(prop['mode']), st_nlink=prop['read_counter'],\
                                   st_size=prop['size'], st_ctime=prop['creation_date'], \
                                   st_mtime=prop['last_modify'], st_atime=time())
                dizio['/.']=dict(st_mode=(propDir['mode']), st_nlink=propDir['read_counter'],\
                         st_size=propDir['size'], st_ctime=propDir['creation_date'], \
                         st_mtime=propDir['last_modify'], st_atime=time())
        print '#############'
        print dizio
        print '###########'
        return ['..'] +[x[1:] for x in dizio if x != '/']
       
    def readlink(self, path):
        
        print '#################'
        print '##### READ LINK ###'
        print '#################'
        return self.data[path]

    
    def removexattr(self, path, name):
        
        print '#################'
        print '##### REMOVE XATTr ###'
        print '#################'

        uid, gid, pid = fuse_get_context()
        context = ""
        
        idObj = self.cache.lookup(options.root_bucket,path)
        attrs = self.files[path].get('attrs', {})
        try:
            del attrs[name]
            self.cache.delProperty(idObj, OF.PROPERTY_USER+name, uid, gid, context)
        except KeyError:
            pass        # Should return ENOATTR
   
        
        #Key Error Should return ENOATTR
    
    def rename(self, old, new):
        
        print '#################'
        print '##### RENAME  ###'
        print '#################'
        uid, gid, pid = fuse_get_context()
        context = ""
        idObj_old = self.cache.lookup(options.root_bucket,old)
        prop_old = self.cache.getProperties(options.root_bucket, idObj_old, OF.PROPERTY_OBJECT, uid, gid, context)
        #self.cache.removeObject(options.root_bucket, idObj_old, uid, gid, context)
        #self.cache.remove(options.root_bucket,idObj,uid,gid,context)
        
        prop_old['object_name'] = new
        self.cache.removeObject(options.root_bucket, idObj_old, uid, gid, context)
        self.cache.setAttributesFromNew(options.root_bucket, idObj_old, prop_old, uid, gid, context)
        self.files[new] = self.files.pop(old)
        
    def rmdir(self, path):
        
        
        print '#################'
        print '##### RMDIR  ###'
        print '#################'
        uid, gid, pid = fuse_get_context()
        context = ""
        idObj = self.cache.lookup(options.root_bucket,path)
        #self.cache.remove(options.root_bucket,idObj,uid,gid,context)
        self.cache.removeObject(options.root_bucket, idObj, uid, gid, context)
        self.files.pop(path)
        self.files['/']['st_nlink'] -= 1
    

    def setxattr(self, path, name, value, options, position=0):
        
        print '#################'
        print '##### SETxattr  ###'
        print '#################'
        uid, gid, pid = fuse_get_context()
        context = ""
        # Options ? / Positions ?
        idObj = self.cache.lookup(options.root_bucket,path)
        self.cache.setProperty(idObj, OF.PROPERTY_USER+name, value, uid, gid, context)
        
        attrs = self.files[path].setdefault('attrs', {})
        attrs[name] = value
     
     
    def statfs(self, path):
        
        print '#################'
        print '##### statfs  ###'
        print '#################'
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)
    
    def symlink(self, target, source):
        
        print '#################'
        print '##### symlink  ###'
        print '#################'
        self.files[target] = dict(st_mode=(S_IFLNK | 0777), st_nlink=1,
           st_size=len(source))
        self.data[target] = source
   
    def truncate(self, path, length, fh=None):
        
        print '#################'
        print '##### truncate  ###'
        print '#################'
        self.data[path] = self.data[path][:length]
        self.files[path]['st_size'] = length
    
    def unlink(self, path):
        
        print '#################'
        print '##### unlink  ###'
        print '#################'
        uid, gid, pid = fuse_get_context()
        context = ""
        idObj = self.cache.lookup(options.root_bucket,path)
        self.cache.remove(options.root_bucket,idObj, uid, gid, context) 
        self.cache.removeObject(options.root_bucket,idObj, uid, gid, context)        
        
        self.files.pop(path)
    
    def utimens(self, path, times=None):
        print '#################'
        print '##### utimes  ###'
        print '#################'
        now = time()
        atime, mtime = times if times else (now, now)
        self.files[path]['st_atime'] = atime
        self.files[path]['st_mtime'] = mtime
   
    def write(self, path, data_handler, offset, fh):
        print '#################'
        print '##### WRITE  ###'
        print '#################'
        uid, gid, context = fuse_get_context()
        xattr = self.listxattr(path)
        object_acl = S3ACP.ACL_FULL_CONTROL
        acp = ACP()
        grant = Grant() 
        grant.uid = uid  
        grant.permission = ACPHelper.s3AclToPerm(object_acl)
        grantList = [grant]
        acp.setByGrants(grantList)
        storage_class="STANDARD"
        content_type=None
        self.cache.write(options.root_bucket, path, uid, gid, context, storage_class, object_acl ,content_type, xattr,  data_handler)
        
        self.data[path] = self.data[path][:offset] + data_handler
        self.files[path]['st_size'] = len(self.data[path])
        return len(data_handler)




if __name__ == "__main__":
    

    if len(argv) != 2:
        print 'usage: %s <mountpoint>' % argv[0]
        exit(1)

    tornado.options.parse_command_line()
    # Read File 
    tornado.options.parse_config_file(options.conf)
    
    # Connection Manager 
    
    # Cache manager

    fuse = FUSE(RestFSClient(), argv[1], foreground=True)

    

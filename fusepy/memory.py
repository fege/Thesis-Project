#!/usr/bin/env python

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time

from fuse24 import FUSE, FuseOSError, Operations, LoggingMixIn


class Memory(LoggingMixIn, Operations):
    """Example memory filesystem. Supports only one level of files."""
    
    def __init__(self):
        print '#################'
        print '##### INIT  ###'
        print '#################'
        self.files = {}
        self.data = defaultdict(str)
        self.fd = 0
        now = time()
        self.files['/'] = dict(st_mode=(S_IFDIR | 0755), st_ctime=now,
            st_mtime=now, st_atime=now, st_nlink=2)
        
    def chmod(self, path, mode):
        print '#################'
        print '##### CHMOD  ###'
        print '#################'
        self.files[path]['st_mode'] &= 0770000
        self.files[path]['st_mode'] |= mode
        return 0

    def chown(self, path, uid, gid):
        print '#################'
        print '##### CHOWN  ###'
        print '#################'
        self.files[path]['st_uid'] = uid
        self.files[path]['st_gid'] = gid
    
    def create(self, path, mode):
        print '#################'
        print '##### CREATE  ###'
        print '#################'
        self.files[path] = dict(st_mode=(S_IFREG | mode), st_nlink=1,
            st_size=0, st_ctime=time(), st_mtime=time(), st_atime=time())
        self.fd += 1
        return self.fd
    
    def getattr(self, path, fh=None):
        print '#################'
        print '##### GETATTR  ###'
        print '#################'
        if path not in self.files:
            raise FuseOSError(ENOENT)
        st = self.files[path]
        return st
    
    def getxattr(self, path, name, position=0):
        print '#################'
        print '##### GETXATTR ###'
        print '#################'
        attrs = self.files[path].get('attrs', {})
        try:
            return attrs[name]
        except KeyError:
            return ''       # Should return ENOATTR
    
    def listxattr(self, path):
        print '#################'
        print '##### LISTXATTR  ###'
        print '#################'
        attrs = self.files[path].get('attrs', {})
        return attrs.keys()
    
    def mkdir(self, path, mode):
        print '#################'
        print '##### MKDIR  ###'
        print '#################'
        self.files[path] = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                st_size=0, st_ctime=time(), st_mtime=time(), st_atime=time())
        self.files['/']['st_nlink'] += 1
    
    def open(self, path, flags):
        print '#################'
        print '##### OPEN  ###'
        print '#################'
        self.fd += 1
        return self.fd
    
    def read(self, path, size, offset, fh):
        print '#################'
        print '##### READ ###'
        print '#################'
        return self.data[path][offset:offset + size]
    
    def readdir(self, path, fh):
        print '#################'
        print '##### READ DIR  ###'
        print '#################'
        return ['.', '..'] + [x[1:] for x in self.files if x != '/']
    
    def readlink(self, path):
        print '#################'
        print '##### READ LINK  ###'
        print '#################'
        return self.data[path]
    
    def removexattr(self, path, name):
        print '#################'
        print '##### REMOVE XATTR  ###'
        print '#################'
        attrs = self.files[path].get('attrs', {})
        try:
            del attrs[name]
        except KeyError:
            pass        # Should return ENOATTR
    
    def rename(self, old, new):
        print '#################'
        print '##### RENAME  ###'
        print '#################'
        self.files[new] = self.files.pop(old)
    
    def rmdir(self, path):
        print '#################'
        print '##### RM DIR  ###'
        print '#################'
        self.files.pop(path)
        self.files['/']['st_nlink'] -= 1
    
    def setxattr(self, path, name, value, options, position=0):
        print '#################'
        print '##### SET XATTR  ###'
        print '#################'
        # Ignore options
        attrs = self.files[path].setdefault('attrs', {})
        attrs[name] = value
    
    def statfs(self, path):
        print '#################'
        print '##### STATFS  ###'
        print '#################'
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)
    
    def symlink(self, target, source):
        print '#################'
        print '##### SYMLINK  ###'
        print '#################'
        self.files[target] = dict(st_mode=(S_IFLNK | 0777), st_nlink=1,
            st_size=len(source))
        self.data[target] = source
    
    def truncate(self, path, length, fh=None):
        print '#################'
        print '##### TRUNCATE  ###'
        print '#################'
        self.data[path] = self.data[path][:length]
        self.files[path]['st_size'] = length
    
    def unlink(self, path):
        print '#################'
        print '##### UNLINK  ###'
        print '#################'
        self.files.pop(path)
    
    def utimens(self, path, times=None):
        print '#################'
        print '##### UTIMES  ###'
        print '#################'
        now = time()
        atime, mtime = times if times else (now, now)
        self.files[path]['st_atime'] = atime
        self.files[path]['st_mtime'] = mtime
    
    def write(self, path, data, offset, fh):
        print '#################'
        print '##### WRITE  ###'
        print '#################'
        self.data[path] = self.data[path][:offset] + data
        self.files[path]['st_size'] = len(self.data[path])
        return len(data)


if __name__ == "__main__":
    if len(argv) != 2:
        print 'usage: %s <mountpoint>' % argv[0]
        exit(1)
    fuse = FUSE(Memory(), argv[1], foreground=True)
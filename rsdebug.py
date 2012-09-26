import tornado
import sys
import logging.config
import types
import traceback
from tornado.options import define, options
from restfsc.service.CacheService import CacheService
from restfsc.manager.CacheManager import CacheManager

define("conf"   ,   default="fuse.cnf"  , help="configuration file")
define("rpc",       default=None, help="rpc call")
define("param",     default=None, help="paramaters")
define("bucket",    default='londra', help="bucket")
define("info",      default=None, help="Command line info")
restfs_version = "0.1"

_LOGGER = logging.getLogger('RSDEBUG')

def my_debug_help():
        print "rpc=findCluster, param=bucket"
        print "rpc=lookup, param=path"
        print "rpc=createObject, param=path, OF.TYPE_FILE, uid, gid, st_mode, context"
        print "rpc=getObjectList, param=idObj, uid, gid, context"
        print "rpc=removeObject, param=bucket, idObj, uid, gid, context"
        print "rpc=getAttributes, param=bucket, idObj, field, uid, gid, context"
        print "rpc=setObjectMode, param=bucket, idObj, mode, uid, gid, context"
        print "rpc=setObjectOwner, param=bucket, idObj, owner, group, uid, gid, context"
        print "rpc=setObjectUtime, param=bucket, idObj, utime, uid, gid, context"
        
        print "rpc=getObjectXattr, param=bucket, idObj, name, uid, gid, context"
        print "rpc=setObjectXattr, param=bucket, idObj, name, value, uid, gid, context"
        print "rpc=delObjectXattr, param= bucket,idObj, name, uid, gid, context"
        print "rpc=listObjectXattr, param=bucket, idObj, uid, gid, context"
        
        print "rpc=getObjectSegment, param= bucket, idObj, idSeg, uid, gid, context"
        print "rpc=delObjectSegment, param=bucket, idObj, idSeg, uid, gid, context"
        print "rpc=setObjectSegment, param=bucket, idObj, idSeg, seg, uid, gid, context"
        
        print "STORAGE OPERATIONS "
        print "rpc=writeBlock,  param= bucket, idObj, key, block, bhash, uid, gid, context"
        print "rpc=readBlock,  param= bucket, idObj, key, uid, gid, context"
        print "rpc=removeBlock,  param= bucket, idObj, key, uid, gid, context"
        
        print "rpc=remove,  param= bucket, idObj, uid, gid, context"
        print "rpc=load,  param= bucket, idObj, uid, gid, context"
        print "rpc=open,  param= bucket, idObj, flags,uid, gid, context"
        print "rpc=write, param = bucket, object_name, uid, gid, context, storage_class, object_acl, content_type, xattr, data_handler)"
        
        print "rpc=writeMetaItem param= idObj, field, value, version"
        print "rpc=getMetaItem     param= idObj, field"
        print "rpc=writeBlockCache param=bucket, idObj, idSeg, idBlock, block"
        print "rpc=readBlockCache  param=bucket, idObj, idSeg, idBlock"
        print "rpc=writeOffset  param =bucket, idObj, data_handler, offset,  uid, gid, context"
        print "rpc=loadOffset  param = bucket, idObj, offset, uid, gid, context"

        # Server preverred list 
        print "rpc=getMetaCache param=bucket, idObj"
        print "\n\n"


def myhelp():
        print "debug\n"
        print "rpc=setbucket parqm=bucket\n"
        print "help\n"
        print "exit\n"
        
def _findService(name):
    try:
        return getattr(cache, name)
             
    except AttributeError:
        return None


def contatempo(func): 
    import time
    def interna(*args,**kw): 
        start=time.time() 
        result=func(*args,**kw) 
        tempo=time.time()-start 
        _LOGGER.info(args[0])
        _LOGGER.debug(tempo)
        #print("Funzione: %s - tempo impiegato: %4.9f" % (args[0],tempo)) 
        return result 
    return interna
 
@contatempo
def _dispatch(method, params):
  
    func = _findService(method)
   
    if func is not None:
        try:
            if type(params) is types.ListType:
                if params[0] != '':
                    response = func(*params)
                else:
                    pass
            else:
                response = func(**params)
                
            return response
        except:
            traceback.print_exc()
            return ""
    else:
        return "Not Found"


def debug_mode():
        while True:
            params = []
            cmd = raw_input('restfs-debug:insert cmd name> ')
            if cmd == "exit":
                return 
            else:
                if cmd != "help":
                    bucket = raw_input('restfs-debug:insert bucket>')
                    if cmd != 'findCluster':
                        param  = raw_input('restfs-debug:insert params>') 
                        print param
                        params = param.split(',')
                    params.insert(0,bucket)
                    print _dispatch(cmd,params)
                    #_dispatch(cmd,params)
                else:
                    my_debug_help() 

def _configure_logging():
        """
        Configure logging.
        When no logfile argument is given we log to stdout.
        """
        logging.config.fileConfig('loggingClient.conf')
        logging.info('Logging started')


if __name__ == "__main__":
    
    _configure_logging()
    cache = CacheService()
    cacheMng = CacheManager()
    tornado.options.parse_command_line()
    # Read File 
    tornado.options.parse_config_file(options.conf)
    
    if not options.rpc and not options.param:
        int_exec = True
    
    if options.help == '':
        myhelp()
        sys.exit()
    
    if int_exec:
        print 'Welcome to RestFS Debug Command Line\n'
        print 'Version %s \n' % restfs_version
    
    while True:
        prop = None
        if int_exec:
            cmd = raw_input('restfs:insert cmd name> ')
            if cmd != "debug" and cmd != "exit" and cmd != "help":
                param=raw_input('restfs:insert params>')
                params = options.param.split(',')
            

        # operations

        if cmd =="setbucket":
            bucket = params[0]
        
        if cmd.lower() =="put":
            #createObject
            # put FILE_INPUT PATH_SERVER 
            #dove sono gli altri uid, gid, context, storage_class, object_acl ,content_type, xattr ???
            #readLocalFile e una funzione di cacheService ad hoc
            streamData, file_name = readLocalFile(params[1])
            #streamData e il mio nuovo data_handler
            print _dispatch('write', params, streamData)

        if cmd.lower() =="get":
            #getObject
            #get PATH_SERVER or ID ?
            #se do solo il path prima lookup e poi open
            #di default la open mette il file in tmp
            idObj = _dispatch('lookup',params)
            print _dispatch('open',params)

        if cmd.lower() =="cd":
            #get a directory or change bucket ?
            pass
        
        if cmd.lower() =="login":
            #auth
            pass
        
        if cmd.lower() =="info":
            #getOBjectProperties
            #info PATH_SERVER or ID ?
            idObj = _dispatch('lookup',params)
            print _dispatch('getProperties',params)
            pass
        
        if cmd.lower() =="mkdir":
            #create object type dir
            #mkdir PATH_SERVER
            if not params[1].endswith('/'):
                params[1] = params[1]+'/'
            print _dispatch('write', params)
        
        if cmd.lower() =="del":
            #delete
            #del PATH_SERVER
            idObj = _dispatch('lookup',params)
            print _dispatch('removeObject',params)
            print _dispatch('remove',params)
                    
        if cmd.lower() =="ls":
            #getObjectList
            #ls PATH_SERVER
            #si puo fare ls del bucket ? da decidere
            idObj = _dispatch('lookup',params)
            print _dispatch('getObjectList',params)

        if cmd.lower() =="pwd":
            #getCurrentPath
            #qua non capisco cosa vuole il manfred ... 
            #il path gia lo inserisci da input, che voglia opposto del lookup ?
            pass
        
        if cmd.lower() =="chmod":
            #change mode
            #chmod PATH NEW_MODE
            idObj = _dispatch('lookup',params)
            print _dispatch('setObjectMode',params)

        if cmd.lower() =="chown":
            #change owner
            #chmod PATH NEW_OWNER
            idObj = _dispatch('lookup',params)
            print _dispatch('setObjectOwner',params)
            
        if cmd.lower() =="getXattr":
            #get xattributes
            #getxattr PATH 
            idObj = _dispatch('lookup',params)
            print _dispatch('getObjectXattr',params)

        if cmd.lower() =="setXattr":
            #set xattr
            #setxattr PATH NEW_XATTR
            idObj = _dispatch('lookup',params)
            print _dispatch('setObjectXattr',params)
            
        #mancano i comandi sull acls        

        elif cmd == "exit" or not int_exec:
            sys.exit()

        elif cmd == "debug":
            debug_mode()           
        else:
            myhelp()
            
        if prop :
            print 'RESULT =',  prop    
        

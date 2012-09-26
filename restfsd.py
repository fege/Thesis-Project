#!/usr/bin/env python
import sys
import os.path
import signal
import tornado.options
import tornado.httpserver
import tornado.ioloop
import logging.config
import restfs.utils.daemon as daemon
from tornado import web
from tornado.options import define, options
from restfs.utils.pidfile import Pidfile
from restfs.handlers.s3.S3ServiceHandler import S3ServiceHandler
from restfs.handlers.s3.S3BucketHandler import S3BucketHandler
from restfs.handlers.s3.S3ObjectHandler import S3ObjectHandler
from restfs.handlers.websocket.WbSObjectHandler import WbSObjectHandler
from restfs.handlers.websocket.WbSDataHandler import WbSDataHandler
from restfs.handlers.websocket.WbServiceHandler import WbServiceHandler
from restfs.manager.AuthManager     import AuthManager
from restfs.manager.ResourceManager import ResourceManager
from restfs.manager.MetadataManager import MetadataManager
from restfs.manager.StorageManager  import StorageManager
from restfs.service.S3BucketService import S3BucketService
from restfs.service.S3ObjectService import S3ObjectService

__VERSION__ = "0.3.0"

#GLOBAL Options 
define("port",            default=80  , help="run on the given port", type=int)
define("id_system",       default="beolink1"  , help="System Identification",)
define("host"   ,         default="s3.amazonaws.com"  , help="Default host")
define("conf"   ,         default="restfs.cnf"  , help="configuration file")
define("daemon"   ,       default="False"  , help="Run as daemon")
define("bin_path"   ,     default="./"  , help="Installation Path")

#MANAGER Options 
define("auth_driver"    , default="file"  , help="Auth Driver")
define("resource_driver", default="redis" , help="Default service lookup")
define("metadata_driver", default="redis" , help="Metadata Storage",)
define("storage_driver" , default="fs2"   , help="Storage system",)
define("cache_driver",    default="cache" , help="Metadata Storage",)


#define("cache",           default="fs"  , help="Default service lookup")


#SSL Options 
define("ssl"         ,    default="False"  , help="enable ssl")
define("ssl_certfile",    default="./ssl/server.crt"  , help="enable ssl")
define("ssl_keyfile" ,    default="./ssl/server.key"  , help="enable ssl")

#S3 Options 
define("interface_s3" ,             default="False", help="S3 Interface")
define("interface_rpc",             default="False", help="Restfs Interface")


_pidfile = None 

class RestFSApp(tornado.web.Application):
    
    def __init__(self):
        
        
        handlers =  [ ]
        
        
        settings = dict(
            template_path=os.path.join(os.path.dirname(__file__), "templates"),
            xsrf_cookies=False,
            cookie_secret="43oETzKXQAGaYdkL5gEmGeJJFuYh7EQnp2XdTP1o/Vo=",
        )
        
        
        # Parameter
        self.id_system = options.id_system

        #SET UP MANAGER
        self.authMng     = AuthManager()
        self.resourceMng = ResourceManager()
        self.metadataMng = MetadataManager()
        self.storageMng  = StorageManager()
        
        #SET UP APLLICATION
        web.Application.__init__(self, handlers, **settings)
        
        # END POINT
        self.default_host = options.host 
        endpoint = r""
        for el in self.default_host.split('.'):
                if endpoint:
                    endpoint += r"\."+el
                else:
                    endpoint = el
        bckEndpoint = r".+\."+endpoint
        rootHandlers = []
        bckHandlers  = []
        #
        # WebSocket Interface
        #############################################################################
        if options.interface_rpc == "True":
            rootHandlers.append((r"/metadata", WbSObjectHandler))
            bckHandlers.append((r"/metadata", WbSObjectHandler))
            rootHandlers.append((r"/data", WbSDataHandler))
            bckHandlers.append((r"/data", WbSDataHandler))  
            rootHandlers.append((r"/service", WbServiceHandler))
            bckHandlers.append((r"/service", WbServiceHandler))           
            self.rpc_encode_type = "json"


        #S3 Service Handler
        #######################################################################
        if options.interface_s3 == "True":
            
            self.bucketSrv = S3BucketService(self.resourceMng, self.metadataMng, self.storageMng)
            self.objectSrv = S3ObjectService(self.resourceMng, self.metadataMng, self.storageMng)

            #endpoint     = r".+\.restfs\.beolink\.org"
            
            #URL BASE
            #  self.add_handlers(r"restfs\.beolink\.org", [
            rootHandlers.append((r"/", S3ServiceHandler))
            #rootHandlers.append((r"/[^/]+", S3ServiceHandler))
            rootHandlers.append((r"/[^/]+/", S3BucketHandler))
            rootHandlers.append((r"/[^/]+/.+", S3ObjectHandler))
            
            bckHandlers.append((r"^/.+", S3ObjectHandler))
            bckHandlers.append((r"^/$", S3BucketHandler))

           
        
        self.add_handlers(endpoint,rootHandlers)
        self.add_handlers(bckEndpoint,bckHandlers)        
        
        
        
        



def _setup_path():
        sys.path.append(options.bin_path)  
     
     
def _parse_opts():
        #Read Command Line 
        tornado.options.parse_command_line()
        # Read File 
        tornado.options.parse_config_file(options.conf)
        #Command line has precedence 
        tornado.options.parse_command_line()
   

def _configure_logging():
        """
        Configure logging.
        When no logfile argument is given we log to stdout.
        """
        logging.basicConfig(format='%(levelname)s:%(message)s',level=logging.DEBUG)
        logging.basicConfig(filename='logging.log',filemode='w')
        logging.config.fileConfig('logging.conf')
        logging.info('Logging started')

def _init_signals(self):
        """
        Setup kill signal handlers.
        """
        signals = ['TERM', 'HUP', 'QUIT', 'INT']

        for signame in signals:
            """
            SIGHUP and SIGQUIT are not available on Windows, so just don't register a handler for them
            if they don't exist.
            """
            sig = getattr(signal, 'SIG%s' % signame, None)
            
            if sig:
                signal.signal(sig, _receive_signal)    



            
def _receive_signal(self, signum, stack):
        _shutdown()    


def _shutdown(self):
        """
        Called on application shutdown.
        
        Stop  .
        """
       
        if options.daemon == "True":
            _pidfile.unlink()



def  main():
    # Read Command line
    # getopts is better ! 
    ############################################   
    _parse_opts()
    _setup_path()
    
        
    # Start Logging
    ############################################ 
    _configure_logging()
    
    
    #Daemonize
    ############################################

    if options.daemon == "True":
            daemon.daemonize()
            
            pid = os.getpid()
            _pidfile = Pidfile(options.pidfile)
            _pidfile.create(pid)

    
    #Run Application
    ############################################
    application = RestFSApp()

    if options.ssl == "True":
        http_server = tornado.httpserver.HTTPServer(application,ssl_options=dict(
            certfile=options.ssl_certfile,
            keyfile=options.ssl_keyfile))
    else:
        http_server = tornado.httpserver.HTTPServer(application)
        
    http_server.listen(options.port)
    logging.info('Server starting on port %s' % options.port)
    tornado.ioloop.IOLoop.instance().start()
    
    
    
    

if __name__ == "__main__":
    main()

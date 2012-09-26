import tornado.options
import os
import sys
from restfs.objects.JsonRPCPayload import JsonRPCPayload
from restfsc.net.WebSocket import WebSocket
from tornado.options import define, options
import restfsc.net.WebSocket as WBS
import restfsc.net.ping as Ping
from restfs.objects.Context import Context

import httplib
import json

import tornado.options
import os
import sys
from restfs.objects.JsonRPCPayload import JsonRPCPayload
from restfsc.net.WebSocket import WebSocket
from tornado.options import define, options
import restfsc.net.WebSocket as WBS
import restfsc.net.ping as Ping

class ConnectionManager(object):
    _META_SERVER_LIST = {} 
    #= None
    _WBS_PROTOCOL = None
    _SERVICE_ENDPOINT  = "service"
    _METADATA_ENDPOINT = "metadata"
    _DATA_ENDPOINT     = "data"
    _CONNECTION_MANAGER_TYPE      = ""
    CONNECTION_TYPE_DATA  = "DATA"
    CONNECTION_TYPE_METADATA = "METADATA"
    context = Context(1, '', '', [])
    
    def __init__(self, type = None,  bucket_name=None):
        if 'ssl' and 'auth_mode' and 'system_id' and 'system_password' and 'service_mode'\
        and 'service_ip' and 'service_port' not in options.keys():
            define("ssl",               default="True", help="ssl tunnel")
            define("auth_mode",         default=None, help="system authentication type")
            define("system_id",         default="", help="system id ")
            define("system_password",   default="", help="system password")
            define("service_mode",      default="ip", help="Lookup service mechanism")
            define("service_ip",        default="restfs.beolink.org", help="ip address of resource locator")
            define("service_port",      default="443", help="port of resource locator")
            define("preferred_server_mode",      default="ping", help="Preferred server algorithm")
            define("preferred_server",      default="443", help="port of resource locator")
       
        tornado.options.parse_config_file(options.conf)
        # cache .. 
        # More parameter Conf File ?
        
        self._META_SERVER_LIST = {}

        if options.ssl == 'True':
            self._WBS_PROTOCOL = 'wss'
        else:
            self._WBS_PROTOCOL = 'ws'
            
        self._CONNECTION_MANAGER_TYPE = type
        
        
    def send(self, bucket_name, packet, async=None, retry=0):
        # Lock method 
        
        data = None
        try :
            #FIXME Logger
            print "Sent DATA"
            print 'pack',packet
        
            #modificare send, fare un reconnect oppure cercare un nuovo server
            if not self._META_SERVER_LIST.has_key(bucket_name):
                self._connect(bucket_name)
            
            self._META_SERVER_LIST[bucket_name].send(packet, WBS.ABNF.OPCODE_BINARY)
            if not async :
                #FIXME Caricare Time out
                #modificare receive, se cade il server devo sollevare una eccezione
                data = self._META_SERVER_LIST[bucket_name].recv()
                print "Received DATA"
                print data
                
        except Exception, e :
            exc_type, exc_value, exc_tb = sys.exc_info()
            print exc_type
            print exc_value
            print exc_tb
            raise e
            self._reconnect()
            # Check New  Server for connection
            
            
        return data
        
            
    def recv(self):
        #Lock method
        pass
    
    def _findCluster(self,bucket_name):
        ping_list = []
        if options.service_mode == 'ip':
            ping_list.append(options.service_ip+':'+options.service_port)
            ip_list = []
            for i in ping_list:
                ip = i.split(':')
                ip_list.append(ip[0])
            for ip_address in ip_list:
                RTT = Ping.doOne(ip_address)
                ping_list.append((ip_address+':'+options.service_port,RTT))
            ping_list = sorted(ping_list, key=lambda i:i[-1])
        else:
            msg = JsonRPCPayload('findCluster',[bucket_name])
            data = self.conn_service.send(msg.getPacket())
            msg.load(data)
            ping_list = msg.get('result')
            
        return ping_list 
    
    def _connect(self, bucket_name):
        
        server_list = self._findCluster(bucket_name)
        ws = WebSocket()
        while len(server_list) > 0:
            server = self._WBS_PROTOCOL+'://'+server_list[0][0]+'/'+self._METADATA_ENDPOINT
            try:
                ws.connect(server)
                self._META_SERVER_LIST[bucket_name] = ws
                return
                #exit 
            except:
                #connection error
                server_list.pop(0)
        
        
        #no connection, let's try with another server
        
    
    
    
    
    # For The Feture !
    def _determine_threads(self,options):
        '''Return optimum number of upload threads'''
    
        cores = os.sysconf('SC_NPROCESSORS_ONLN')
        memory = os.sysconf('SC_PHYS_PAGES') * os.sysconf('SC_PAGESIZE')
    
        if options.compress == 'lzma':
            # Keep this in sync with compression level in backends/common.py
            # Memory usage according to man xz(1)
            mem_per_thread = 186 * 1024 ** 2
        else:
            # Only check LZMA memory usage
            mem_per_thread = 0
    
        if cores == -1 or memory == -1:
            #log.warn("Can't determine number of cores, using 2 upload threads.")
            return 1
        elif 2 * cores * mem_per_thread > (memory / 2):
            threads = min(int((memory / 2) // mem_per_thread), 10)
            if threads > 0:
                #log.info('Using %d upload threads (memory limited).', threads)
                pass
            else:
                #log.warn('Warning: compression will require %d MB memory '
                #         '(%d%% of total system memory', mem_per_thread / 1024 ** 2,
                #         mem_per_thread * 100 / memory)
                threads = 1
            return threads

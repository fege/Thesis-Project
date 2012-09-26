import types

import string
import restfs.utils.JsonRPCUtils as jsonrpc
from restfs.utils.bson import  Binary
from restfs.objects.JsonRPCError import JsonRPCError

class JsonRPCPayload(dict):
    _version     = 2.0
    _id          = None
    _PARAM_TYPES = (types.ListType, types.DictType, types.TupleType)
    _payload     = dict()
    __idCHARS = string.ascii_lowercase+string.digits
    
    def __init__(self, method=None, params=[], version=None, rpcid=None):
        if not version:
            version = self._version
        self._id = rpcid
        self._version = float(version)
        if method:
            self.setRequest(method, params)
        else:
            self._payload = dict()
        
    def load(self,data):
        print 'PAYLOAD', data
        self._payload = jsonrpc.loads(data)

        # Check response
        if not self._payload:
            err = JsonRPCError(-32600, 'Request inval_id -- no request data.')
            raise err       
        
        if type(self._payload) is types.ListType:
            # FIXME ADD BATCH MODE
            return None
        
        if type(self._payload) is not types.DictType:
            err = JsonRPCError(
                -32600, 'Request must be {}, not %s.' % type(self._payload)
            )
            raise err
        
        self._id = self._payload.get('id', None)
        version = self._get_version()
        print 'VERSIONE', self._get_version()
        if not version:
            err = JsonRPCError(-32600, 'Request %s inval_id.' % self._payload, rpcid=self._id)
            raise err
                
        #self._payload.setdefault('params', [])

        #method = self._payload.get('method', None)
        #params = self._payload.get('params')

        
        #if not method or type(method) not in types.StringTypes or \
        #    type(params) not in self._PARAM_TYPES:
        #    err = JsonRPCError(-32600, 'Inval_id request parameters or method.', rpc_id=rpc_id)
        #    raise err
        
        
    
    def setRequest(self, method, params=[]):
        self._payload = dict()
        if type(method) not in types.StringTypes:
            raise ValueError('Method name must be a string.')
        if not self._id:
            self._id = jsonrpc.random_id()
        self._payload = { 'id':self._id, 'method':method }
        if params:
            self._payload['params'] = params
        if self._version >= 2:
            self._payload['jsonrpc'] = str(self._version)
        


    def setNotify(self, method, params=[]):
        self.request(method, params)
        if self._version >= 2:
            del self._payload['id']
        else:
            self._payload['id'] = None
        

    def setResponse(self, result=None, rpcid=None):
        self._payload = {'result':result, 'id':rpcid}
        if self._version >= 2:
            self._payload['jsonrpc'] = str(self._version)
        else:
            self._payload['error'] = None


    def setError(self, code=-32000, message='Server error.', rpcid=None):
        if rpcid:
            self._id=rpcid
        self._payload = dict()
        if self._version < 2:
            self._payload['result'] = None
        self._payload['id'] = self._id
        self._payload['error'] = {'code':code, 'message':message}
    
    
    def get(self,name):
        return self._payload[name]
    
    def getPacket(self,encode="json"):
        return jsonrpc.dumps(self._payload)

    def getPayload(self):
        return self._payload
    
    # Internal
    ############################
    def _get_version(self):
        # must be a dict
        if 'jsonrpc' in self._payload.keys():
            return 2.0
        if 'id' in self._payload.keys():
            return 1.0
        return None
    

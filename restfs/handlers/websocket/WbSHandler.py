import types
import traceback
import sys

from restfs.handlers.websocket.WbSBaseHandler import WbSBaseHandler
import restfs.utils.JsonRPCUtils as jsonrpc
from restfs.objects.JsonRPCError import JsonRPCError
from restfs.objects.JsonRPCPayload import JsonRPCPayload


class WbSHandler(WbSBaseHandler):
    wbsRequest = None
    
    
    def _parseWbSMessage(self,data):
        self.wbsRequest  = JsonRPCPayload()
        try:
            self.wbsRequest.load(data)
        except JsonRPCError as e:
            response = JsonRPCPayload(rpcid=None)
            response.setError(e.code, e.errorString)
            self.write_message(response.getPacket(self.application.rpc_encode_type))
            
        
      
    def _findServiceEndpoint(self, name):
        meth = None
        
        try:
            meth = getattr(self.service, name)
            if hasattr(meth, "rpcmethod"):
                return meth
            else:
                return None
             
        except AttributeError:
            return None


    def _dispatch(self, method, params):
      
        func = self._findServiceEndpoint(method)
       
        response = ''
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
            except TypeError:
                err_lines = traceback.format_exc().splitlines()
                trace_string = '%s | %s' % (err_lines[-3], err_lines[-1])
                exc_type, exc_value, exc_tb = sys.exc_info()
                
                raise JsonRPCError(-32602, 'Invalid parameters: %s' %  trace_string)
            except:
                err_lines = traceback.format_exc().splitlines()
                trace_string = '%s | %s' % (err_lines[-3], err_lines[-1])
                raise JsonRPCError(-32603, 'Server error: %s' % 
                                         trace_string)

        else:
            raise JsonRPCError(-32601, 'Method %s not supported.' % method)
        
        

    
    
 

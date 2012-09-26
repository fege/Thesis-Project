import sys
import traceback
import tornado.websocket
import restfs.utils.JsonRPCUtils as jsonrpc
from restfs.objects.JsonRPCError import JsonRPCError
from restfs.manager.MetadataManager import MetadataManager
from restfs.handlers.websocket.WbSHandler import WbSHandler
from restfs.objects.JsonRPCPayload import JsonRPCPayload

class WbServiceHandler(WbSHandler):
    
    def open(self):
        self.service = self.application.resourceMng

        

    def on_message(self, message):
        # TODO - Use the multiprocessing and skip the response if
        # it is a notification
        # Put in support for custom dispatcher here
        # (See SimpleXMLRPCServer._marshaled_dispatch)
        self._parseWbSMessage(message)
        method = self.wbsRequest.get('method')
        params = self.wbsRequest.get('params')
        response = JsonRPCPayload()
         
        result = ''
        try:
            result = self._dispatch(method, params)
         
        except Exception, e:

            response.setError(e.errorCode, e.errorString, self.wbsRequest.get('id'))
            self.write_message(response.getPacket(self.application.rpc_encode_type))
            return
        
        if  self.wbsRequest.get('id') == None:
            # It's a notification
            self.write_message(None)
        print self.wbsRequest.get('id')
        try:
            response.setResponse(result,self.wbsRequest.get('id'))
            self.write_message(response.getPacket(self.application.rpc_encode_type))
        except:
            exc_type, exc_value, exc_tb = sys.exc_info()
            traceback.print_exc
            response.setError(-32603, '%s:%s' % (exc_type, exc_value),self.wbsRequest.get('id'))
            self.write_message(response.getPacket(self.application.rpc_encode_type))
            #FIXME Close connection
                

    def on_close(self):
        pass
    
    

         

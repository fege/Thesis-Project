import sys
import tornado.websocket
import restfs.utils.JsonRPCUtils as jsonrpc

from restfs.objects.JsonRPCError import JsonRPCError
from restfs.handlers.websocket.WbSHandler import WbSHandler



from restfs.objects.JsonRPCPayload import JsonRPCPayload

class WbSDataHandler(WbSHandler):
    
    def open(self):
        self.service = self.application.storageMng
        

    def on_message(self, message):
        print "on_message"
        # TODO - Use the multiprocessing and skip the response if
        # it is a notification
        # Put in support for custom dispatcher here
        # (See SimpleXMLRPCServer._marshaled_dispatch)
        self._parseWbSMessage(message)
        method = self.wbsRequest.get('method')
        params = self.wbsRequest.get('params')
        response = JsonRPCPayload()
        print method 
        print params
        
        result = ''
        try:
            print "invoco DATA"
            result = self._dispatch(method, params)
            print "eseguito DATA"
            print result
            
        except Exception, e:

            response.setError(e.errorCode, e.errorString, self.wbsRequest.get('id'))
            self.write_message(response.getPacket(self.application.rpc_encode_type))
            return
        
        if  self.wbsRequest.get('id') == None:
            # It's a notification
            print "Notification DATA"
            self.write_message(None)
        
        try:
            print "sono entrato DATA"
            #CHANGED FOR BINARY FILES
            # FIXME
            if method == 'readBlock':
                self.write_message(result)
            else:
                response.setResponse(result,self.wbsRequest.get('id'))
                self.write_message(response.getPacket(self.application.rpc_encode_type))
            print 'ho scritto DATA'
        except:
            print "excptions DATA"
            exc_type, exc_value, exc_tb = sys.exc_info()
            response.setError(-32603, '%s:%s' % (exc_type, exc_value),self.wbsRequest.get('id'))
            self.write_message(response.getPacket(self.application.rpc_encode_type))
            #FIXME Close connection
                

    def on_close(self):
        pass
    
    

         

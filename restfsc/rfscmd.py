from restfs.objects.JsonRPCPayload import JsonRPCPayload
import restfs.utils.JsonRPCUtils as JsonRPCUtils
from restfsc.net.WebSocket import WebSocket
import restfs.factories.ObjectFactory as OF

if __name__ == '__main__':
    try:
        websock = WebSocket()
        #websock.settimeout(timeout != None and timeout or default_timeout)
        websock.connect('wss://restfs.beolink.org:443/metadata')

        #LS

        print 'client connected'
        print '######LS######'
        
        '''        payload = JsonRPCPayload()
        payload.setRequest('access', ['momo3','/',''])
        print type(payload)
        websock.send(payload.getPacket())
        result =  websock.recv()
        print result
        payr = JsonRPCPayload()
        payr.load(result)
        print payr.getPayload()

        payload.setRequest('getObjectList',[payr.get('result') , ''])
        websock.send(payload.getPacket())
        result =  websock.recv()
        print result
        payr = JsonRPCPayload()
        payr.load(result)
        print payr.getPayload()'''
        
        
        print '################'
        print 'CREATE BUCKET'
        
        payload = JsonRPCPayload()
        payload.setRequest('createBucket',['ziocane', '', '', ''])
        print 'invio', payload.getPacket()
        websock.send(payload.getPacket())
        result =  websock.recv()
        print 'result',result
        payr = JsonRPCPayload()
        payr.load(result)
        print 'payr', payr.getPayload()
        
        #CREATE BUCKET
        print '######CREATE BUCKET######'
       
        payload = JsonRPCPayload()
        payload.setRequest('createBucket',['nuovoBucket', '', '', 'manfred'])
        print 'invio', payload.getPacket()
        websock.send(payload.getPacket())
        result =  websock.recv()
        print 'result',result
        payr = JsonRPCPayload()
        payr.load(result)
        print 'payr', payr.getPayload()
        
         
        #CREATE OBJECT
        print '######CREATE OBJECT######'

        
        payload = JsonRPCPayload()
        payload.setRequest('access', ['momo3','/','manfred'])
        print type(payload)
        websock.send(payload.getPacket())
        result =  websock.recv()
        print result
        payr = JsonRPCPayload()
        payr.load(result)
        print payr.getPayload()

        payload.setRequest('createObject',['momo3','/ciao2.txt', OF.TYPE_FILE, 'manfred'])
        websock.send(payload.getPacket())
        result =  websock.recv()
        print result
        payr = JsonRPCPayload()
        payr.load(result)
        print payr.getPayload()
        
        #DELETE OBJECT
        print '######DELETE OBJECT######'
        
        payload = JsonRPCPayload()
        payload.setRequest('access', ['momo3','/ciao2.txt','manfred'])
        print type(payload)
        websock.send(payload.getPacket())
        result =  websock.recv()
        print result
        payr = JsonRPCPayload()
        payr.load(result)
        print payr.getPayload()
        
        payload.setRequest('removeObject',[payr.get('result'), 'manfred'])
        websock.send(payload.getPacket())
        result =  websock.recv()
        print result
        payr = JsonRPCPayload()
        payr.load(result)
        print payr.getPayload()
        
        #CREATE OBJECT DIR
        print '######CREATE OBJECT DIR######'

        
        payload = JsonRPCPayload()
        payload.setRequest('access', ['momo3','/','manfred'])
        print type(payload)
        websock.send(payload.getPacket())
        result =  websock.recv()
        print result
        payr = JsonRPCPayload()
        payr.load(result)
        print payr.getPayload()

        payload.setRequest('createObject',['momo3','/dir/', OF.TYPE_DIR, 'manfred'])
        websock.send(payload.getPacket())
        result =  websock.recv()
        print result
        payr = JsonRPCPayload()
        payr.load(result)
        print payr.getPayload()
        
        #DELETE OBJECT DIR
        print '######DELETE OBJECT DIR ######'
        
        payload = JsonRPCPayload()
        payload.setRequest('access', ['momo3','/dir/','manfred'])
        print type(payload)
        websock.send(payload.getPacket())
        result =  websock.recv()
        print result
        payr = JsonRPCPayload()
        payr.load(result)
        print payr.getPayload()
        
        payload.setRequest('removeObject',[payr.get('result'), 'manfred'])
        websock.send(payload.getPacket())
        result =  websock.recv()
        print result
        payr = JsonRPCPayload()
        payr.load(result)
        print payr.getPayload()
        
        #DELETE BUCKET
        print '######DELETE BUCKET######'

              
        payload = JsonRPCPayload()
        payload.setRequest('access', ['nuovoBucket','/',''])
        print type(payload)
        websock.send(payload.getPacket())
        result =  websock.recv()
        print result
        payr = JsonRPCPayload()
        payr.load(result)
        print payr.getPayload()
        
        payload.setRequest('removeBucket',['nuovoBucket',  'manfred'])
        websock.send(payload.getPacket())
        result =  websock.recv()
        print result
        payr = JsonRPCPayload()
        payr.load(result)
        print payr.getPayload()

        
        
    except KeyboardInterrupt:
        print 'client disconnected'
        ws.close()

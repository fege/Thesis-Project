import string
import random
from restfs.utils.bson import BSON
# JSON library importing
cjson = None
json = None
try:
    import cjson
except ImportError:
    try:
        import json
    except ImportError:
        try:
            import simplejson as json
        except ImportError:
            raise ImportError(
                'You must have the cjson, json, or simplejson ' +
                'module(s) available.'
            )





_IDCHARS = string.ascii_lowercase+string.digits

def loads(data):
        """
        This differs from the Python implementation, in that it returns
        the request structure in Dict format instead of the method, params.
        It will return a list in the case of a batch request / response.
        """
        if data == '':
            # notification
            return None
        #result = jloads(data)
        result = BSON(data).decode()
        print 'RES',result
        # if the above raises an error, the implementing server code 
        # should return something like the following:
        # { 'jsonrpc':'2.0', 'error': fault.error(), id: None }
        #if config.use_jsonclass == True:
        #    from jsonrpclib import jsonclass
        #    result = jsonclass.load(result)
        return result
    
def dumps(obj):
      
        #FIX ADD BSON
        print 'ENCODE',obj
        bobj = BSON.encode(obj)
        return bobj
    
    

def jdumps(obj, encoding='utf-8'):
    # Do 'serialize' test at some point for other classes
    global cjson
    if cjson: 
        return cjson.encode(obj)
    else:
        return json.dumps(obj, encoding=encoding)


def jloads(json_string):
    global cjson
    if cjson:
        return cjson.decode(json_string)
    else:
        return json.loads(json_string)

def random_id(length=8):
        return_id = ''
        for i in range(length):
            return_id += random.choice(_IDCHARS)
        return return_id

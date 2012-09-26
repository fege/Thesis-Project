from hashlib import sha1
import hmac
import base64

def signString(password,string_to_sign):
        sign = hmac.new(password, string_to_sign, sha1).digest()
        signature = base64.encodestring(sign).strip()
       
        return signature

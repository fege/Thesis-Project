# -*- coding: latin-1 -*-
import logging
import exceptions
import datetime
import urllib
import random
import  restfs.factories.ErrorCodeFacotry  as ErrCode

from tornado import escape
from restfs.handlers.s3.BaseHandler import BaseHandler
from restfs.objects.S3Request import S3Request
from restfs.factories.HeaderFactory   import AWS
from restfs.objects.RestFSError import RestFSError
from restfs.objects.User import User



class S3Handler(BaseHandler):
    _LOGGER = logging.getLogger('S3_HANDLER')
    
    
    def prepare(self):
        
        self._LOGGER.info('preparing S3Handler')
        self.s3Request = self._parseS3Header()
        self._s3Auth()
        
        
    
    
    def s3Render(self, response, id_request=0, headers=None, code=None):
        
        
        self._LOGGER.info('s3Render')
        # Setting HTTP Code 
        self._LOGGER.info('Setting HTTP Code')
        if code:
            self.set_status(code)
            if code == 100:
                self.set_status(200) 
        self._LOGGER.debug(code)   
        # Setting HEADER
        self._LOGGER.info('Setting Header')
        self.set_header('x-amz-id-2', self.application.id_system)
        self.set_header('x-amz-request-id', id_request)
        content = False

        if headers:
            for (key,value) in headers.iteritems():
                        if key == "Content-Type":
                            content = True
                        self.set_header(key, value)
        
        if not content:
                self.set_header("Content-Type", "application/xml; charset=UTF-8")
                
        self._LOGGER.debug(headers)
        # Setting Content
        if response :

            self._LOGGER.info('Setting Content')            
            assert isinstance(response, dict) and len(response) == 1
            name = response.keys()[0]
            parts = []
            parts.append('<' + escape.utf8(name) +
                         ' xmlns="http://doc.s3.amazonaws.com/2006-03-01">')
            self._render_parts(response.values()[0], parts)
            parts.append('</' + escape.utf8(name) + '>')
            parts = '<?xml version="1.0" encoding="UTF-8"?>\n' + ''.join(parts)
            self._LOGGER.debug("XML Response: %s" % parts)
            self._LOGGER.debug("Resposnse ---------")
            self._LOGGER.debug(parts)
            self.finish(parts)
        else :
            self.finish()


    def write_error(self, status_code, **kwargs):

        if isinstance(kwargs['exc_info'][1],exceptions.__class__) :
            self._LOGGER.error(ErrCode.err['GenericError']['message'])
            raise Exception, ErrCode.err['GenericError']['message']
        if not kwargs['exc_info'][1].log_message:
            self._LOGGER.error(ErrCode.err['InternalServerError']['message'])
            raise ErrCode.err['InternalServerError']['message']


        msg = kwargs['exc_info'][1].log_message
        code = str(status_code)
        res = self.s3Request.RESOURCE
        self._LOGGER.debug(code)
        self._LOGGER.debug(msg)
        self._LOGGER.debug(res)
        self.finish('<Error>\
        <Code>'+code+'</Code>\
        <Message>'+msg+'</Message>\
        <Resource>'+res+'</Resource>\
        <RequestId></RequestId></Error>')
        
        
        
        
# --------------------------------------------------------------
#  INTERNALS
# --------------------------------------------------------------

    # Search sub resource command 
    def _subResource(self,url):
        subResource = dict()    
        for lk in AWS.SUB_RESOURCE:
            value = self.get_parameter(lk,None)
            if value:
                    subResource[lk] = ""
         
        return subResource
   
    #Render 
    # FIX ME ERROR HANDLER
    def _render_parts(self, value, parts=[]):
        self._LOGGER.info('rendering parts')
        self._LOGGER.debug(value)
        if isinstance(value, str) or isinstance(value, unicode):
            #parts.append(escape.xhtml_escape(value))
            parts.append(value)
        elif isinstance(value, int):
            parts.append(str(value))
        elif isinstance(value, datetime.datetime):
            parts.append(value.strftime("%Y-%m-%dT%H:%M:%S.000Z"))
        elif isinstance(value, dict):
            for name, subvalue in value.iteritems():
                if not isinstance(subvalue, list):
                    subvalue = [subvalue]
                for subsubvalue in subvalue:
                    
                    parts.append('<' + escape.utf8(name) + '>')
                    self._render_parts(subsubvalue, parts)
                    parts.append('</' + escape.utf8(name) + '>')
        elif value == None:
            parts.append("")
        else:
            self._LOGGER.debug(parts)
            self._LOGGER.error("Unknown S3 value type %r", value)
            raise Exception("Unknown S3 value type %r", value)
    
    
   
    def _s3Auth(self):
        
        
        self._LOGGER.info('Authentication')        
        if self.s3Request == None:
            #fatto errore nella ErrorCodeFactory
            self._LOGGER.error(ErrCode.err['InternalServerError']['message'])
            raise RestFSError(ErrCode.err['InternalServerError']['code'],\
                              ErrCode.err['InternalServerError']['message'],\
                              ErrCode.err['InternalServerError']['http'])   
        # if self.fsRequest():            
        if self.s3Request.id == None  :
                # Anonymous
            self._LOGGER.info('User Anonymous')        
            self.user = User(0,"anonymous","Anonymous","","",0,0)
        else:
            self._LOGGER.info('Check identity user')        
            auth = self.getAuthManager()
            self.user = auth.checkS3Auth(self.s3Request.id,self.s3Request.signature,self.s3Request.getStringToSign())
            self._LOGGER.debug("Fs Request id %s " % self.s3Request.id)   
            self._LOGGER.debug("Fs Request signature %s " % self.s3Request.signature)
            self._LOGGER.debug("Fs Request getStringToSign %s " % self.s3Request.getStringToSign)            
            if self.user == None:
                #fatto errore nella ErrorCodeFactory
                self._LOGGER.error(ErrCode.err['LoginFailed']['message'])
                raise RestFSError(ErrCode.err['LoginFailed']['code'],\
                          ErrCode.err['LoginFailed']['message'],\
                          ErrCode.err['LoginFailed']['http'])            
       
            
              
    
    def _parseS3Header(self):
        
        self._LOGGER.info('parsing Header')
        headers         = self.request.headers
        authS3Headers   = dict()
        subResource     = dict()
        self._LOGGER.debug("HEADER %s " % headers)
        self._LOGGER.debug("URI %s " % self.request.uri)
        self._LOGGER.debug("PATH %s " % self.request.path)

                
        for key in headers:
            lkey = key.lower()
            if lkey in AWS.AUTH_S3_HEADER or lkey.startswith(AWS.HEADER_PREFIX):
                authS3Headers[lkey] = headers[key].strip()
    
        # these keys get empty strings if they don't exist, for the signing key part
        if not authS3Headers.has_key('content-type'):
            authS3Headers['content-type'] = ''
        if not authS3Headers.has_key('content-md5'):
            authS3Headers['content-md5'] = ''
    
        # just in case 
        if authS3Headers.has_key('x-amz-date'):
            authS3Headers['date'] = ''
    
        #Host vs bucket (without port)
        host = headers['Host'].split(":")[0]
        
        #Parametrs
        url  = self.request.uri.split("?")    
        path = self.request.path
 
        #BUCKET dns based 
       
        if host != self.getDefaultHost(): 
            path   = "/"+host[:host.find(".")]+url[0]
            url[0]    = path

        #FIXME..some better idea ..SUB RESOURCE 
        if len(url) > 1:           
            sub  = url[1].split("&")
            for el in sub:
                key = el.split('=')
                if key[0] in AWS.SUB_RESOURCE:
                    subResource[key[0]] = el
        
        urlEl = url[0].split('/')
       
        bucket = urlEl[1]
        object = ""
        lenB = len(bucket)+1
        if len(urlEl) > 2 :
            object = urllib.unquote(url[0][lenB:])
        
        id  = None
        key = None
        if headers.has_key('Authorization'):
            credential = headers['Authorization'].strip().split(":")
            key = credential[1]
            if credential[0].startswith("AWS"):
                id = credential[0][4:]
              
        else:
            #FIXME Search also credential in url
            pass
        
        id_request = random.getrandbits(128)
        #Save sub resources in request
        #qua creo id della request
        s3Req = S3Request(id,  key, self.request.method, authS3Headers, path, bucket, object, subResource, id_request)
        return s3Req
       
   
        

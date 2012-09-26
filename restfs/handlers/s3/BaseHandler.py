
import tornado.web

class BaseHandler(tornado.web.RequestHandler):
	#SUPPORTED_METHODS 		= ("PUT", "GET", "DELETE")
	
	#def __init__(self):
	#	pass
	#
	s3request = None
	
	def getMethod(self):
		return self.request.method
		
	def getUri(self):
		return self.request.uri
	
	def getBody(self):
		return self.request.body
		
	def getHeaders(self):
		self.request.headers
				
	def getDefaultHost(self):
		return self.application.default_host
	
	def getBucketService(self):
		return self.application.bucketSrv
	
	def getObjectService(self):
		return self.application.objectSrv
		
	def getAuthManager(self):
		return self.application.authMng
	
	def getUser(self):
		return self.user
	
	def getUsername(self):
		return self.user._USER_NAME

	def getResourceManager(self):
		return self.application.resourceMng
		
		
	

	

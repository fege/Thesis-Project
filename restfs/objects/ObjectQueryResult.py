class ObjectQueryResult(object):
	objectList = []
	def __init__(self, bucket_name="", prefix="",marker="",max_keys=-1,isTruncated=False):
		self.bucket_name = bucket_name
		self.prefix    = prefix
		self.marker    = marker
		self.max_keys  = max_keys
		self.isTruncated = isTruncated
		
		
	def getObjectList(self):
		return self.objectList
	
	def setObjectList(self,objectList):
		if objectList:
			self.objectList = objectList
	

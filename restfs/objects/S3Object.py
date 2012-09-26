""" 
    SIMPLE INTERFACE !!!!!!!!!!!!
	Must be implemented by Storage layer

"""
class S3Object(object):
	
	_READ  = "READ"
	_WRITE = "WRITE"

	"""
	 Overwrite by backend implementation
	"""
	def __init__(self):
		pass
		
	def open(self):
		pass
		
	def read(self):
		pass
		
	def write(self):
		pass
	
	def close(self):
		pass
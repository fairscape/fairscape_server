
class ROCrateException(Exception):
	""" Exception class for all ROCrate Exceptions
	"""
	def __init__(self, message, errors):
		self.errors = errors
		self.message = message
		super().__init__(message) 

	def __str__(self):
		return self.message

	def __repr__(self):
		return self.message


class ROCrateFilterException(ROCrateException):	
	""" Exception when filtering an ROCrates Metadata
	"""
	def __init__(self, message, results):
		self.results = results
		self.message = message
		super().__init__(self.message)

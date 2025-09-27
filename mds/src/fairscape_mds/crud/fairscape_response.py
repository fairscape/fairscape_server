
class FairscapeResponse():
	def __init__(
		self, 
		success: bool, 
		statusCode: int, 
		model=None, 
		fileResponse=None, 
		error: dict= {},
		jsonResponse: dict={}
	):
		self.model = model
		self.success = success
		self.statusCode = statusCode
		self.error = error
		self.fileResponse = fileResponse
		self.jsonResponse = jsonResponse

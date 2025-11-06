class IdentifierNotFound(Exception):
	def __init__(self, message: str, guid: str):
		self.guid = guid
		self.message = message

		super().__init__(self.message)


class FileNotFound(Exception):
	def __init__(self, message: str, guid: str):
		self.guid = guid
		self.message = message

		super().__init__(self.message)

class UserNotAuthorized(Exception):
	def __init__(
			self, 
			message: str, 
			guid: str, 
			userEmail: str,
			action: str
		):
		self.message = message
		self.guid = guid
		self.userEmail = userEmail
		self.action = action
		super().__init__(self.message)


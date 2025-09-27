from fairscape_mds.core.config import FairscapeConfig


class FairscapeRequest():
	def __init__(
			self, 
			backendConfig: FairscapeConfig
	):
		self.config = backendConfig
  
	def getMetadata(self, guid: str):
		return self.config.identifierCollection.find_one({"@id": guid}, {"_id": 0})


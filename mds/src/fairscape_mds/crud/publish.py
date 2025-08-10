from fairscape_mds.crud.fairscape_request import FairscapeRequest
from fairscape_mds.crud.fairscape_response import FairscapeResponse

class FairscapePublishRequest(FairscapeRequest):

	def updatePublicationStatus(self)->FairscapeResponse:

		# TODO write publication status changing code
		return FairscapeResponse(success=True, statusCode=200, jsonResponse={"hello": "world"})

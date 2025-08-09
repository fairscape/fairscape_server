from fairscape_mds.crud.fairscape_request import FairscapeRequest
from fairscape_mds.crud.fairscape_response import FairscapeResponse

from fairscape_mds.crud.identifier import getMetadata, deleteIdentifier

from fairscape_mds.models.user import UserWriteModel, Permissions, checkPermissions
from fairscape_mds.models.software import SoftwareWriteModel
from fairscape_models.software import Software

class FairscapeSoftwareRequest(FairscapeRequest):

	def createSoftware(
		self, 
		requestingUser: UserWriteModel,		
		softwareInstance: Software
	):

		writeModel = SoftwareWriteModel.model_validate({
			**softwareInstance.model_dump(by_alias=True, mode='json'),
			"permissions": requestingUser.getPermissions()
		})

		insertResult = self.identifierCollection.insert_one(
			writeModel.model_dump(by_alias=True, mode='json')
		)

		return FairscapeResponse(
			success=True,
			statusCode=201,
			model=writeModel
		)

	def getSoftware(self, guid: str):
		return getMetadata(self.identifierCollection, Software, guid)

	def deleteSoftware(
		self,		
		requestingUser: UserWriteModel, 
		guid: str
	):

		return deleteIdentifier(
			self.identifierCollection,
			requestingUser,
			SoftwareWriteModel,
			guid
		)

	def updateSoftware(self):
		pass
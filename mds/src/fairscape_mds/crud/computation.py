from fairscape_mds.crud.fairscape_request import FairscapeRequest
from fairscape_mds.crud.fairscape_response import FairscapeResponse
from fairscape_mds.crud.identifier import getMetadata, deleteIdentifier

from fairscape_mds.models.computation import ComputationWriteModel
from fairscape_mds.models.user import UserWriteModel

from fairscape_models.computation import Computation

class FairscapeComputationRequest(FairscapeRequest):

	def createComputation(
		self, 
		requestingUser: UserWriteModel,		
		computationInstance: Computation
	):

		writeModel = ComputationWriteModel.model_validate({
			**computationInstance.model_dump(by_alias=True, mode='json'),
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


	def getComputation(self, guid: str):
		return getMetadata(self.identifierCollection, Computation, guid)


	def deleteComputation(
		self,		
		requestingUser: UserWriteModel, 
		guid: str
	):
		return deleteIdentifier(
			self.identifierCollection,
			requestingUser,
			Computation,
			guid
		)


	def updateComputation(self):
		pass
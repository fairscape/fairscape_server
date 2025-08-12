from fairscape_mds.crud.fairscape_request import FairscapeRequest
from fairscape_mds.crud.fairscape_response import FairscapeResponse
from fairscape_mds.crud.identifier import getMetadata, deleteIdentifier, getStoredIdentifier

from fairscape_mds.models.computation import ComputationWriteModel
from fairscape_mds.models.user import UserWriteModel
from fairscape_mds.models.identifier import (
	StoredIdentifier,
	PublicationStatusEnum,
	MetadataTypeEnum
)
from fairscape_models.computation import Computation
import datetime

class FairscapeComputationRequest(FairscapeRequest):

	def createComputation(
		self, 
		requestingUser: UserWriteModel,		
		computationInstance: Computation
	):
		createdDatetime = datetime.datetime.now(tz=datetime.timezone.utc)

		writeModel = StoredIdentifier.model_validate({
			"@id": computationInstance.guid,
			"@type": MetadataTypeEnum.COMPUTATION,
			"metadata": computationInstance.model_dump(by_alias=True, mode='json'),
			"permissions": requestingUser.getPermissions(),
			"distribution": None,
			"dateCreated": createdDatetime,
			"dateModified": createdDatetime
		})

		insertResult = self.identifierCollection.insert_one(
			writeModel.model_dump(by_alias=True, mode='json')
		)

		return FairscapeResponse(
			success=True,
			statusCode=201,
			model=writeModel
		)


	def getComputation(self, guid: str)->ComputationWriteModel:
		foundMetadata = self.getMetadata(guid)['metadata']
		if foundMetadata is None:
			raise Exception
		else:
			return ComputationWriteModel.model_validate({**foundMetadata})


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
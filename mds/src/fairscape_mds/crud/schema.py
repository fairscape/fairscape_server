from fairscape_mds.crud.fairscape_request import FairscapeRequest
from fairscape_mds.crud.fairscape_response import FairscapeResponse
from fairscape_mds.crud.identifier import getMetadata, deleteIdentifier

from fairscape_mds.models.schema import SchemaWriteModel
from fairscape_mds.models.user import UserWriteModel, checkPermissions
from fairscape_models.schema import Schema

class FairscapeSchemaRequest(FairscapeRequest):

	def createSchema(
		self, 
		requestingUser: UserWriteModel,
		schemaInstance: Schema
	):
		writeModel = SchemaWriteModel.model_validate({
			**schemaInstance.model_dump(by_alias=True, mode='json'),
			"permissions": requestingUser.getPermissions(),
			"published": True
		})

		insertResult = self.identifierCollection.insert_one({
			writeModel.model_dump(by_alias=True, mode='json')
		})

		return FairscapeResponse(
			success=True,
			statusCode=201,
			model=writeModel
		)


	def getSchema(self, guid: str):
		return getMetadata(self.identifierCollection, Schema, guid)


	def updateSchema(self):
		pass

	def deleteSchema(
		self,
		requestingUser: UserWriteModel,
		guid: str	
		):
		return deleteIdentifier(
			self.identifierCollection,
			requestingUser,
			SchemaWriteModel,
			guid
		)
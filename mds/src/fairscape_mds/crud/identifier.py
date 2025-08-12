from fairscape_mds.crud.fairscape_request import FairscapeRequest
from fairscape_mds.crud.fairscape_response import FairscapeResponse
from fairscape_mds.models.user import UserWriteModel, checkPermissions
from fairscape_mds.models.identifier import StoredIdentifier, MetadataTypeEnum, PublicationStatusEnum, UpdatePublishRequest
from pydantic import ValidationError

class IdentifierRequest(FairscapeRequest):

	def updatePublicationStatus(
		self, 
		publicationChange: UpdatePublishRequest,
		requestingUser: UserWriteModel 
	)->FairscapeResponse:

		guid = publicationChange.guid
		newStatus = publicationChange.publicationStatus

		# get the identifier metadata
		metadata = self.identifierCollection.find_one(
			{"@id": guid}, 
			projection={"_id": False}
		)

		if not metadata:
			return FairscapeResponse(
				success=False,
				statusCode=404,
				error={"error": "identifier not found"}
			)

		# serialize metadata into model 
		try:
			foundIdentifier = StoredIdentifier.model_validate(metadata)
		except ValidationError as e:
			return FairscapeResponse(
				success=False,
				statusCode=500,
				error={"message": "validation error", "error": e}
			)

		if checkPermissions(foundIdentifier.permissions, requestingUser):

			# update the permissions on
			updateResult = self.identifierCollection.update_one(
				{"@id": guid},
				{"$set": {"publicationStatus": newStatus}}
				)

			# TODO check the update result

			return FairscapeResponse(
				success=True,
				statusCode=200,
				jsonResponse={
					"@id": guid,
					"publicationStatus": newStatus
					}
			)

		else:
			return FairscapeResponse(
				success=False,
				statusCode=401,
				error={"error": "user unauthorized to modify identifier"}
			)


	def listType(self, requestType: MetadataTypeEnum)->FairscapeResponse:

		cursor = self.identifierCollection.find({"@type": requestType})

		pass


	def listPublished(self)->FairscapeResponse:
		pass


	def updateMetadata(self):
		pass

	def deleteIdentifier(self):
		pass


def getStoredIdentifier(identifierCollection, guid: str)->FairscapeResponse:
	metadata =identifierCollection.find_one(
		{"@id": guid}, 
		projection={"_id": False}
		)
	
	if metadata:
		try:
			foundIdentifier = StoredIdentifier.model_validate(metadata)
			return FairscapeResponse(
				success=True,
				statusCode=200,
				model= foundIdentifier
			)
		except ValidationError as e:
			return FairscapeResponse(
				success=False,
				statusCode=500,
				jsonResponse={"message": "validation error", "error": e}
			)

	else:
		return FairscapeResponse(
			success=False, 
			statusCode=404, 
			error={
				"error": "identifier not found"
			}
		)



def getMetadata(
	mongoCollection, 
	passedModel, 
	guid: str
	)-> FairscapeResponse:

	foundMetadata = mongoCollection.find_one(
		{"@id": guid}
	)

	if not foundMetadata:
		return FairscapeResponse(
			success=False,
			statusCode=404,
			error= {"message": "identifier not found"}
		)

	modelInstance = passedModel.model_validate(foundMetadata)

	return FairscapeResponse(
		success=True,
		statusCode=200,
		model=modelInstance
	)



def deleteIdentifier(
	idCollection, 
	requestingUser: UserWriteModel, 
	modelClass, 
	guid: str
	)-> FairscapeResponse:
	# find the dataset
	foundMetadata = idCollection.find_one({
		"@id": guid,
	})

	if not foundMetadata:
		return FairscapeResponse(
			success=False,
			statusCode=404,
			error= {"message": "dataset not found"}
		)

	modelInstance = modelClass.model_validate(foundMetadata)

	# check permissions
	if checkPermissions(modelInstance.permissions, requestingUser):

		# update the 
		idCollection.update_one(
			{"@id": guid},
			{"$set": {
				"publicationStatus": PublicationStatusEnum.ARCHIVED
			}}
		)

		modelInstance.published = False

		return FairscapeResponse(
			success=True,
			statusCode=200,
			model=modelInstance
		)

	else:
		return FairscapeResponse(
			success=False,
			statusCode=401,
			error={"message": "user unauthorized"}
		)

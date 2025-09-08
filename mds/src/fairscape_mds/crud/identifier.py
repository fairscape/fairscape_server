from fairscape_mds.crud.fairscape_request import FairscapeRequest
from fairscape_mds.crud.fairscape_response import FairscapeResponse
from fairscape_mds.models.user import UserWriteModel, checkPermissions
from fairscape_mds.models.identifier import StoredIdentifier, MetadataTypeEnum, PublicationStatusEnum, UpdatePublishRequest
from fairscape_mds.models.dataset import DistributionTypeEnum
from pydantic import ValidationError
from typing import Optional


class IdentifierRequest(FairscapeRequest):

	def getContent(self, guid: str)->FairscapeResponse:
		""" Get Operation for Published Only Content, returns a FairscapeResponse with the Content from minio
		"""

		# get the metadata
		metadata = self.config.identifierCollection.find_one(
			{"@id": guid},
			projection={"_id": False}
		)

		if not metadata:
			return FairscapeResponse(
				success=False,
				statusCode=404,
				error={"error": "identifier not found"}
			)
		
		identifier = StoredIdentifier.model_validate(metadata)

		if identifier.publicationStatus != PublicationStatusEnum.PUBLISHED:
			return FairscapeResponse(
				success=False,
				statusCode=401,
				error={"error": "identifier not published"}
			)

		if identifier.metadataType != MetadataTypeEnum.DATASET and identifier.metadataType != MetadataTypeEnum.ROCRATE:
			return FairscapeResponse(
				success=False,
				statusCode=400,
				error={"error": "identified must be dataset or rocrate"}
			)

		# is distribution stored locally
		if identifier.distribution.distributionType != DistributionTypeEnum.MINIO:
			return FairscapeResponse(
				success=False,
				statusCode=404,
				error={"error": "identifier content not stored locally"}
			)
		
		# get distribution content
		objectKey = identifier.distribution.location.path

		response = self.config.minioClient.get_object(
			Bucket=self.config.minioBucket,
			Key=objectKey
		)

		return FairscapeResponse(
			success=True,
			statusCode=200,
			fileResponse=response,
			model=identifier
		)

	def updatePublicationStatus(
		self, 
		publicationChange: UpdatePublishRequest,
		requestingUser: UserWriteModel 
	)->FairscapeResponse:

		guid = publicationChange.guid
		newStatus = publicationChange.publicationStatus

		# get the identifier metadata
		metadata = self.config.identifierCollection.find_one(
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
				error={"message": "validation error", "error": e.json()}
			)

		if not checkPermissions(foundIdentifier.permissions, requestingUser):
			return FairscapeResponse(
				success=False,
				statusCode=401,
				error={"error": "user unauthorized to modify identifier"}
			)


		# TODO if its an ROCrate change all contained items to the new status
		if foundIdentifier.metadataType == MetadataTypeEnum.ROCRATE:

			# update all members
			updateMembersResult = self.config.identifierCollection.update_many(
				{"metadata.isPartOf.@id": guid},
				{"$set": {"publicationStatus": repr(newStatus)}}
			)

			# TODO check the update result
			# updateMembersResult.modified_count == len

		# update the permissions on
		updateResult = self.config.identifierCollection.update_one(
			{"@id": guid},
			{"$set": {"publicationStatus": repr(newStatus)}}
			)

		# TODO check the update result


		return FairscapeResponse(
			success=True,
			statusCode=200,
			jsonResponse={
				"@id": guid,
				"publicationStatus": repr(newStatus)
				}
		)



	def listType(
		self, 
		requestType: MetadataTypeEnum, 
		user: Optional[UserWriteModel]
	)->FairscapeResponse:

		#cursor = self.identifierCollection.find({"@type": requestType})

		return FairscapeResponse(
			success=False,
			statusCode=400,
			error={"error": "not implemented"}
		)


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

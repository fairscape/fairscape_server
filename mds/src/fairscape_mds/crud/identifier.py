from fairscape_mds.crud.fairscape_request import FairscapeRequest
from fairscape_mds.crud.fairscape_response import FairscapeResponse
from fairscape_mds.models.user import UserWriteModel, checkPermissions
from fairscape_mds.models.identifier import (
	StoredIdentifier, 
	MetadataTypeEnum, 
	PublicationStatusEnum, 
	UpdatePublishRequest, 
	determineMetadataType
)
from fairscape_mds.models.dataset import DistributionTypeEnum
from pydantic import ValidationError
from typing import Optional
import datetime
from pymongo import ReturnDocument


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

		# public identifiers
		publishedIdentifiers = self.config.identifierCollection.find(
			{
				"@type": requestType, 
				"publicationStatus": PublicationStatusEnum.PUBLISHED 
			},
			projection={"_id": False}
		)

		if user:
			# users identifiers
			usersIdentifiers = self.config.identifierCollection.find(
				{
					"@type": requestType, 
					"permissions.owner": user.email
				},
				projection={"_id": False}
			)

			identifiers = list(usersIdentifiers) +  list(publishedIdentifiers)
		
		else:
			identifiers = list(publishedIdentifiers)

		return FairscapeResponse(
			success=True,
			statusCode=200,
			model=identifiers
		)


	def listPublished(self)->FairscapeResponse:
		identifiers = self.config.identifierCollection.find(
			{"publicationStatus": PublicationStatusEnum.PUBLISHED}, 
			projection={"_id": False}
		)

		return FairscapeResponse(success=True, statusCode=200, model=list(identifiers))


	def updateMetadata(
			self, 
			guid: str,
			user: UserWriteModel,
			newMetadata
		):

		# check if identifier exists
		foundMetadata = self.config.identifierCollection.find_one(
			{"@id": guid},
			projection={"_id": False}
		)

		if not foundMetadata:
			return FairscapeResponse(
				statusCode=404,
				success=False,
				error= {"error": "identifier not found"}
			)

		# check if user can update the identifier
		metadataOwner = foundMetadata.get("permissions", {}).get("owner")

		if user.email != metadataOwner:
			return FairscapeResponse(
				statusCode=401,
				success=False,
				error={"error": "unauthorized to update"}
			)

		# TODO set up validation for specific types
		newMetadataType = newMetadata.metadataType
		if not newMetadataType:
			return FairscapeResponse(
				statusCode=400,
				success=False,
				error={"error": "replacement metadata missing '@type' property"}
			)

		try:
			metadataType = determineMetadataType(newMetadataType)
		except:
			return FairscapeResponse(
				statusCode=400,
				success=False,
				error={"error": "replacement metadata missing '@type' property"}
			)

		# TODO validate metadata type of update

		updateResult = self.config.identifierCollection.find_one_and_update(
			{"@id": guid},
			{
				"$set": {
					"metadata": newMetadata.model_dump(by_alias=True, mode="json"),
					"dateModified": datetime.datetime.now()
				}
			},
			projection = {"_id": False},
			return_document=ReturnDocument.AFTER
		)

		updatedIdentifier = StoredIdentifier.model_validate(updateResult)

		return FairscapeResponse(
			statusCode=200,
			success=True,
			model=updatedIdentifier
		)



	def deleteIdentifier(
		self,
		guid: str,
		forceDelete: bool, 
		user: UserWriteModel
		)->FairscapeResponse:

		# check if identifier exists
		foundMetadata = self.config.identifierCollection.find_one(
			{"@id": guid},
			projection={"_id": False}
		)

		if not foundMetadata:
			return FairscapeResponse(
				statusCode=404,
				success=False,
				error= {"error": "identifier not found"}
			)

		foundIdentifier = StoredIdentifier.validate(foundMetadata)

		# check if user has permissions
		metadataOwner = foundIdentifier.permissions.owner

		if user.email != metadataOwner:
			return FairscapeResponse(
				statusCode=401,
				success=False,
				error={"error": "user not allowed to delete identifier"}
			)

		if forceDelete:

			# depending on metadata type 

			# if rocrate

			# if dataset

			# else


			return FairscapeResponse(
				success=True,
				statusCode=200,
				jsonResponse=foundMetadata
			)

		else:
			# set the metadata to publication status archive	

			return FairscapeResponse(
				success=True,
				statusCode=200,
				jsonResponse=foundMetadata
			)



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

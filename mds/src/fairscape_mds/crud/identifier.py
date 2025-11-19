from fairscape_mds.crud.fairscape_request import FairscapeRequest
from fairscape_mds.crud.fairscape_response import FairscapeResponse
from fairscape_mds.models.user import UserWriteModel, checkPermissions
from fairscape_mds.models.dataset import DatasetDistribution
from fairscape_mds.models.identifier import (
	StoredIdentifier, 
	MetadataTypeEnum, 
	PublicationStatusEnum, 
	UpdatePublishRequest, 
	determineMetadataType
)
from fairscape_mds.crud.rocrate import userPath, setDatasetObjectKey
from fairscape_mds.models.statistics import (
	DescriptiveStatistics,
	CategoricalStatistics
)
from fairscape_mds.crud.statistics import (
	generateSummaryStatistics
)
from fairscape_models import IdentifierValue
from fairscape_models.model_card import ModelCard
from fairscape_mds.models.dataset import DistributionTypeEnum
from fairscape_mds.models.errors import IdentifierNotFound, FileNotFound
from fastapi import UploadFile
from pydantic import ValidationError
from typing import Optional
import datetime
import pathlib
from pymongo import ReturnDocument
from io import BytesIO
import pandas
import mimetypes


class IdentifierRequest(FairscapeRequest):


	def getIdentifier(self, guid):
		""" Find Identifier metadata and marshal into a StoredIdentifier class
		"""
		
		# get the metadata for a stored identifier
		datasetMetadata = self.config.identifierCollection.find_one(
			{"@id": guid},
			projection={"_id": False}
		)

		if not datasetMetadata:
			raise IdentifierNotFound(
				guid=guid,
				message=f"{guid} does not exist"
			)

		identifier = StoredIdentifier.model_validate(datasetMetadata)
		return identifier


	def generatePresignedGetURL(self, guid: str):
		""" Given a GUID, determine if content is in minio. 
				If content exists return a presigned GET URL
		"""
		identifier = self.getIdentifier(guid)

		if identifier.distribution is None:
			raise FileNotFound(
				guid=guid,
				message=f"{guid} does not have a distribution in fairscape"
			)
		elif identifier.distribution.distributionType != DistributionTypeEnum.MINIO:
			raise FileNotFound(
				guid=guid,
				message=f"{guid} does not have a distribution in fairscape"
			)
		
		else:
			response = self.config.minioClient.generate_presigned_url(
				'get_object',
				Params={
					'Bucket': self.config.minioBucket,
					'Key': identifier.distribution.location.path
					}
			)
			return response


	def loadContent(
			self, 
			guid: str
		):
		""" Given a GUID, determine if content exists. If it does, load the content into memory.
		"""
		identifier = self.getIdentifier(guid)
		
		if identifier.distribution is None:
			raise FileNotFound(
				guid=guid,
				message=f"{guid} does not have a distribution in fairscape"
			)
		elif identifier.distribution.distributionType != DistributionTypeEnum.MINIO:
			raise FileNotFound(
				guid=guid,
				message=f"{guid} does not have a distribution in fairscape"
			)
		
		try:
			contentPath = identifier.distribution.location.path
			response = self.config.minioClient.get_object(
				Bucket = self.config.minioBucket,
				Key = contentPath
			)
			body = response['Body'].read()
			return body
		except self.config.minioClient.exceptions.NoSuchKey:
			raise FileNotFound(
				guid=guid,
				message=f"content not found at path {contentPath}"
			)
		except Exception as e:
			raise Exception(
				f"Error Reading Content: {contentPath}"
		)


	def generateStatistics(
		self, 
		guid: str,
		fileName: str
		):
		""" Given an Ark Generate Statistics and update the identifier.
		"""

		datasetContent = self.loadContent(guid)

		# TODO handle more mimetypes
		datasetMimetype, _ = mimetypes.guess_type(fileName)

		match datasetMimetype: 
				case "text/csv":
					dataframe = pandas.read_csv(BytesIO(datasetContent))
				case "text/tab-seperated-values":
					dataframe = pandas.read_csv(BytesIO(datasetContent), sep="\t")
				case "application/vnd.ms-excel":
					#TODO iterate for each excel sheet
					dataframe = pandas.read_excel(BytesIO(datasetContent), sheet_name=0)
				case "application/vnd.apache.parquet":
					#TODO iterate for each parquet table
					return None
				# hdf5
				case None:
					return None

				case _:
					return None

		summaryStatistics = generateSummaryStatistics(dataframe)

		# update identifier
		updateOperation = self.config.identifierCollection.update_one(
			{"@id": guid},
			{
				"$set" : {"descriptiveStatistics": summaryStatistics} 
			}
		)

		return summaryStatistics


	def getContent(self, guid: str)->FairscapeResponse:
		""" API Operation to Download Published Only Content, returns a FairscapeResponse with the Content from minio
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
		""" Request from user to change the publication status, modify the stored document in mongo.
		"""

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
		""" List all metadata instances of a specific type
		"""
	
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


	def listPublished(
		self
		)->FairscapeResponse:
		""" List all published content, return to user
		"""
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
		""" Replace metadata on an existing GUID
		"""

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
					"dateModified": datetime.datetime.now().isoformat()
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
		""" Delete API Operation, if not a force delete marks applicable identifiers as Archived. If force delete removes content from minio and mongo.
		"""
		deleteRequest = DeleteIdentifier(
			config = self.config,
			guid = guid,
			requestingUser = user,
			force = forceDelete
		)

		return deleteRequest.delete()

	def UploadMLModel(
		self,
		userInstance: UserWriteModel,
		mlModelMetadata: ModelCard,
		mlModelContent: Optional[UploadFile] = None
		)->FairscapeResponse:
		""" API Request to upload a ML Model
		"""

		permissionsSet = userInstance.getPermissions()
		now = datetime.datetime.now()

		foundMetadata = self.getMetadata(mlModelMetadata.guid)
		if foundMetadata:
			return FairscapeResponse(
				success=False,
				statusCode=400,
				error={"error": "identifier already exists"}
			)

		# if no content is passed
		contentUrl = mlModelMetadata.contentUrl
		if mlModelContent is None:
			if contentUrl is None:
				modelDistribution = None
			if 'http://' in contentUrl or 'https://' in contentUrl:
				modelDistribution = DatasetDistribution.model_validate({
					"distributionType": "url",
					"location": {"uri": contentUrl}
				})

			if 'ftp://' in contentUrl:
				modelDistribution = DatasetDistribution.model_validate({
					"distributionType": "ftp",
					"location": {"uri": contentUrl}
				})

		else:

			# set the upload path for ml models
			userFilepath = userPath(userInstance.email)	
			basePath = self.config.minioDefaultPath
			contentName = pathlib.Path(contentUrl).name
			if basePath is None:
				objectKey = f"{userFilepath}/datasets/{contentName}"
			else:
				objectKey = f"{basePath}/{userFilepath}/datasets/{contentName}"

			uploadResult = self.config.minioClient.upload_fileobj(
				Bucket=self.config.minioBucket,
				Key= objectKey,
				Fileobj=mlModelContent
			)

			# create distribution for metadata
			modelDistribution = DatasetDistribution.model_validate({
					"distributionType": DistributionTypeEnum.MINIO.value,
							"location": {"path": objectKey}
					})


		mlIdentifier = StoredIdentifier.model_validate({
			"@id": mlModelMetadata.guid,
			"@type": MetadataTypeEnum.ML_MODEL.value,
			"metadata": mlModelMetadata,
			"permissions": permissionsSet,
			"distribution": modelDistribution,
			"publicationStatus": PublicationStatusEnum.DRAFT.value,
			"descriptiveStatistics": None,
			"dateCreated": now,
			"dateModified": now
		})

		insertOneResult = self.config.identifierCollection.insert_one(
			mlIdentifier.model_dump(by_alias=True, mode='json')
		)

		uploadedModelIdentifierValue = IdentifierValue.model_validate({
			"@id": mlIdentifier.guid,
			"@type": MetadataTypeEnum.ML_MODEL.value,
			"name": mlIdentifier.metadata.name
		})

		return FairscapeResponse(
			success=True,
			statusCode=200,
			model=uploadedModelIdentifierValue
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


class DeleteIdentifier():
	def __init__(
			self, 
			config, 
			guid: str, 
			requestingUser: UserWriteModel,
			force: bool = False
		):
		self.config = config
		self.guid = guid
		self.user = requestingUser
		self.force = force


	def getIdentifier(self):
		metadata = self.config.identifierCollection.find_one(
			{"@id": self.guid},
			projection={"_id": False}
		)

		return metadata


	def deleteROCrate(
		self, 
		identifier: StoredIdentifier
	) -> FairscapeResponse:

			# delete minio content
		
		if identifier.distribution:
			distributionType = identifier.distribution.distributionType
			objectKey = identifier.distribution.location.path
		else:
			distributionType = None
			objectKey = None
			

		if self.force:	
			if distributionType  == DistributionTypeEnum.MINIO:
				# remove the archive 
				self.config.minioClient.delete_object(
					Bucket = self.config.minioBucket,
					Key = objectKey
				)

			# delete the identifier
			self.config.identifierCollection.delete_one(
				{"@id": identifier.guid}
			)

			# delete all hasPart identifiers
			self.config.identifierCollection.delete_many({
				"metadata.isPartOf.@id": identifier.guid
			})


			return FairscapeResponse(
				success=True,
				statusCode = 200,
				model = identifier
			)

		else:
			self.config.identifier.update_one(
				{"@id": identifier.guid},
				{"$set": {"publicationStatus": PublicationStatusEnum.ARCHIVED}}
			)

			self.config.identifierCollection.update_many(
				{"metadata.isPartOf.@id": identifier.guid},
				{"$set": {"publicationStatus": PublicationStatusEnum.ARCHIVED}}
			)

			return FairscapeResponse(
				success=True,
				statusCode = 200,
				model = identifier
			)


	def deleteDataset(
		self, 
		identifier: StoredIdentifier
		) -> FairscapeResponse:

		# if dataset content is included in an rocrate
		try:
			isPartOf = identifier.metadata.isPartOf
		except AttributeError:
			isPartOf = None
		
		if identifier.distribution:
			distributionType = identifier.distribution.distributionType
			objectKey = identifier.distribution.location.path
		else:
			distributionType = None
			objectKey = None
		
		if isPartOf and distributionType  == DistributionTypeEnum.MINIO and self.force:
			return FairscapeResponse(
				success=False,
				statusCode = 400,
				error = {"error": "identifier is a dataset with a file included in an rocrate, delete the rocrate to remove this record"}
			)
		
		elif distributionType == DistributionTypeEnum.MINIO and self.force:
			# delete object from minio
			self.config.minioClient.delete_object(
				Bucket = self.config.minioBucket,
				Key = objectKey
			)

		#elif isPartOf:
			# remove the metadata record from the ROCrate
			#rocrateGUID = isPartOf.get("@id")

		if self.force:
			# remove metadata record
			self.config.identifierCollection.delete_one({"@id": self.guid})
		else:
			self.config.identifierCollection.update_one(
				{"@id": self.guid},
				{"$set": {"publicationStatus": PublicationStatusEnum.ARCHIVED}}
			)

		return FairscapeResponse(
			success = True,
			statusCode = 200,
			model = identifier
		)


	def deleteMetadataElem(
		self, 
		identifier: StoredIdentifier
		):
		
		# is elem included in an rocrate
		try:
			isPartOf = identifier.metadata.isPartOf
		except AttributeError:
			isPartOf = None

		if isPartOf:
			return FairscapeResponse(
				success=False,
				statusCode = 400,
				error = {"error": "identifier is included in an rocrate, delete the rocrate to remove this record"}
			)

		if self.force:
			self.config.identifierCollection.delete_one({
				"@id": identifier.guid
			})

		else:
			# set publication status to archived
			self.config.identifierCollection.update_one(
				{ "@id": identifier.guid},
				{ "publicationStatus": PublicationStatusEnum.ARCHIVED }
			)

		return FairscapeResponse(
			success=True,
			statusCode = 200,
			model = identifier
		)


	def delete(self)->FairscapeResponse:

		metadata = self.getIdentifier()
		if not metadata:
			return FairscapeResponse(
				statusCode = 404,
				success = False,
				error = {"error": "identifier not found"}
			)

		try:
			identifierInstance = StoredIdentifier.model_validate(metadata)
			self.identifier = identifierInstance
		except:
			return FairscapeResponse(
				statusCode = 500,
				success = False,
				error = {"error": "error validating identifier"}
			)

		# check permissions
		if not checkPermissions(identifierInstance.permissions, self.user):
			return FairscapeResponse(
				statusCode=401,
				success=False,
				error = {"error": "unauthorized to delete this identifier"}
			)

		# delete based on type 
		match StoredIdentifier.metadataType:
			case MetadataTypeEnum.DATASET:
				return self.deleteDataset(identifierInstance)

			case MetadataTypeEnum.ML_MODEL:
				return self.deleteDataset(identifierInstance)

			case MetadataTypeEnum.ROCRATE:
				return self.deleteROCrate(identifierInstance)

			case _:
				return self.deleteMetadataElem(identifierInstance)

		pass

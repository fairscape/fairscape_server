from fairscape_mds.crud.fairscape_request import FairscapeRequest
from fairscape_mds.crud.fairscape_response import FairscapeResponse
from fairscape_mds.models.user import UserWriteModel, Permissions, checkPermissions
from fairscape_mds.models.dataset import (
	DatasetWriteModel, 
	DatasetDistribution, 
	DistributionTypeEnum,
	DatasetUpdateModel,
)
from fairscape_mds.models.errors import (
	IdentifierNotFound,
	FileNotFound
)
from fairscape_mds.models.identifier import (
	PublicationStatusEnum, 
	MetadataTypeEnum, 
	StoredIdentifier
)
from fairscape_models.dataset import Dataset
from typing import Optional
from fastapi import UploadFile
import datetime
import pathlib
from pymongo import ReturnDocument


def setDatasetObjectKey(
	datasetFilename: str, 
	userInstance: UserWriteModel, 
	basePath: str = ""
	):
	contentName = pathlib.Path(datasetFilename).name
	if basePath is None:
		return f"{userInstance.email}/datasets/{contentName}"
	else:
		return f"{basePath}/{userInstance.email}/datasets/{contentName}"


def uploadObjectMinio(
    minioClient,
    minioBucket: str,
    minioKey: str,
    datasetFile: UploadFile,
)-> DatasetDistribution:
	""" Upload a object 
	"""
	uploadResult = minioClient.upload_fileobj(
			Bucket=minioBucket,
			Key=minioKey,
			Fileobj=datasetFile
	)

	# create distribution for metadata
	distribution = DatasetDistribution.model_validate({
			"distributionType": DistributionTypeEnum.MINIO,
					"location": {"path": minioKey}
			})

	return distribution


class FairscapeDatasetRequest(FairscapeRequest): 
	def getDatasetMetadata(
			self,
			datasetGUID: str
	):
		foundMetadata = self.getMetadata(datasetGUID)['metadata']
		if foundMetadata is None:
			raise Exception
		else:
			return DatasetWriteModel.model_validate({**foundMetadata})



	def getDatasetContent(
		self, 
		userInstance: UserWriteModel, 
		datasetGUID: str,
	)->FairscapeResponse:
		datasetMetadata = self.config.identifierCollection.find_one(
			{"@id": datasetGUID},
			projection={"_id": False}
		)

		if not datasetMetadata:
				return FairscapeResponse(
					success=False,
					statusCode=404,
					jsonResponse={"error": "Dataset not found"}
				)

		storedDataset = StoredIdentifier.model_validate(
			datasetMetadata
		)

		if not checkPermissions(storedDataset.permissions, userInstance):
			return FairscapeResponse(
				success=False,
				statusCode=401,
				jsonResponse={"error": "user unauthorized"}
			)

		if storedDataset.distribution:
			if storedDataset.distribution.distributionType == DistributionTypeEnum.MINIO:
				# get the distribution location from metadata
				objectKey = storedDataset.distribution.location.path

				response = self.config.minioClient.get_object(
					Bucket=self.config.minioBucket,
					Key=objectKey
				)

				return FairscapeResponse(
					success=True,
					statusCode=200,
					fileResponse=response,
					model=storedDataset
				)
			else:
				return FairscapeResponse(
					success=False,
					statusCode=400,
					jsonResponse={"error": "Dataset Not Stored Locally"}
				)

		else:
			return FairscapeResponse(
				success=False,
				statusCode=400,
				jsonResponse={"error": "Dataset Not Stored Locally"}
			)

 
	def createDataset(
		self, 
		userInstance: UserWriteModel,
		inputDataset: Dataset,
		datasetContent: Optional[UploadFile]=None
	)->FairscapeResponse:
		# check if guid already exists
		foundMetadata = self.getMetadata(inputDataset.guid)

		if foundMetadata:
			return FairscapeResponse(
				success=False,
				statusCode=400,
				error={"error": "identifier already exists"}
			)
		
		# if no content is passed
		if datasetContent is None:
			
			if inputDataset.contentUrl is None:
				distribution = None 
			
			# if http URI add url distribution
			elif 'http' in inputDataset.contentUrl:
				distribution = DatasetDistribution.model_validate({
						"distributionType": "url",
						"location": {"uri": inputDataset.contentUrl}
						})

			# if ftp URI add url distribution
			elif 'ftp' in inputDataset.contentUrl:
				distribution = DatasetDistribution.model_validate({
					"distributionType": "ftp",
					"location": {"uri": inputDataset.contentUrl}
				})

		# upload dataset content to minio
		else:
				
			# determine object key
			uploadKey = setDatasetObjectKey(
				datasetContent.filename, 
				userInstance, 
				basePath= self.config.minioDefaultPath
			)
			
			# upload content and return a dataset distribution
			distribution = uploadObjectMinio(
				self.config.minioClient, 
				self.config.minioBucket, 
				uploadKey, 
				datasetContent.file
				)

		# set remainder of metadata for storage
		permissionsSet = userInstance.getPermissions()

		now = datetime.datetime.now()

		# convert to stored identifier
		outputDataset = StoredIdentifier.model_validate({
			"@id": inputDataset.guid,
			"@type": MetadataTypeEnum.DATASET,
			"metadata": inputDataset,
			"permissions": permissionsSet, 
			"distribution": distribution,
			"publicationStatus": PublicationStatusEnum.DRAFT,
			"dateCreated": now,
			"dateModified": now
			})

		# insert identifier metadata into mongo
		insertResult = self.config.identifierCollection.insert_one(
			outputDataset.model_dump(by_alias=True, mode="json")
		)

		# TODO handle insert errors

		
		# add identifier to users'dataset
		updateResult = self.config.userCollection.update_one(
				{"email": userInstance.email}, 
				{"$push": {"identifiers": inputDataset.guid}}
		)

		# TODO handle update errors 

		# return fairscape response
		return FairscapeResponse(
			success=True,
			statusCode=201,
			model=outputDataset
		)


	def updateDataset(
			self,
			requestingUser: UserWriteModel,
			updateInstance: DatasetUpdateModel
	):
		""" Update Dataset Metadata
		"""

		identifierMetadata = self.config.identifierCollection.find_one(
			{"@id": updateInstance.guid},
			projection={"_id": False}
		)

		if not identifierMetadata:
			# TODO raise fastapi httpexception 
			raise Exception

		# validate into storedidentifier
		storedIdentifier = StoredIdentifier.model_validate(identifierMetadata)

		# if an identifier is published changes can be made to identifiers 
		if storedIdentifier.publicationStatus != PublicationStatusEnum.PUBLISHED:
			# if not published than 
			if not checkPermissions(storedIdentifier.permissions, requestingUser):
				return FairscapeResponse(
					success=False,
					statusCode=401,
					jsonResponse={"error": "user unauthorized to edit dataset metadata"}
				)
		
		setUpdateValues = updateInsance.set.model_dump(mode="json", exclude_unset=True)
		pushUpdateValues = updateInsance.push.model_dump(mode="json", exclude_unset=True)

		# push cannot overwrite null fields
		# check that no push updates are on fields set to none
		identifierMetadata = storedIdentifier.metadata.model_dump(by_alias=True, mode='json')


		keysToTransfer = []
		for key in pushUpdateValues.keys():
			if not identifierMetadata.get("key"):
				keysToTransfer.append(key)

		for key in keysToTransfer:
				setUpdateValues[key] = pushUpdateValues[key]
				del pushUpdateValues[key]

		setUpdatePrepped = { f"metadata.{key}": value for key, value in setUpdateValues.items() }
		pushUpdatePrepped = { f"metadata.{key}": value for key, value in pushUpdateValues.items() }

		# set update
		setUpdatePrepped['dateModified'] = datetime.datetime.now()

		updateResponse = self.config.identifierCollection.find_one_and_update(
			{"@id": updateInstance.guid},
			{
				"$set": setUpdatePrepped,
				"$push": pushUpdatePrepped
			},
			projection={"_id": False},
			return_document=ReturnDocument.AFTER
		)

		newModel = StoredIdentifier.model_validate(updateResponse)

		return FairscapeResponse(
			success=True,
			statusCode=201,
			model=newModel	
		)

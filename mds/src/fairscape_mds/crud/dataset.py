from fairscape_mds.crud.fairscape_request import FairscapeRequest
from fairscape_mds.crud.fairscape_response import FairscapeResponse
from fairscape_mds.models.user import UserWriteModel, Permissions, checkPermissions
from fairscape_mds.crud.entity_creation_utils import validateROCrateParents
from fairscape_mds.crud.entity_creation_utils import addEntityToROCrate
from fairscape_mds.crud.identifier import IdentifierRequest
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

		if inputDataset.isPartOf and len(inputDataset.isPartOf) > 0:

			validation = validateROCrateParents(
				self.config.identifierCollection,
				inputDataset.isPartOf,
				userInstance
			)

			if not validation['valid']:
				return FairscapeResponse(
					success=False,
					statusCode=400,
					error={
						"error": "Invalid parent RO-Crate(s)",
						"details": validation['errors']
					}
				)

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
			# Check if dataset belongs to a RO-Crate
			rocrate_guid = None
			if inputDataset.isPartOf and len(inputDataset.isPartOf) > 0:
				from fairscape_mds.crud.entity_creation_utils import findFirstROCrateInIsPartOf
				rocrate_guid = findFirstROCrateInIsPartOf(
					self.config.identifierCollection,
					inputDataset.isPartOf
				)

			if rocrate_guid:
				sanitized_guid = rocrate_guid.replace("ark:", "").replace("/", "-")

				contentName = pathlib.Path(datasetContent.filename).name
				uploadKey = f"{self.config.minioDefaultPath}/{userInstance.email}/rocrates/{sanitized_guid}/datasets/{contentName}"
			else:
				uploadKey = setDatasetObjectKey(
					datasetContent.filename,
					userInstance,
					basePath=self.config.minioDefaultPath
				)

			# upload content and return a dataset distribution
			distribution = uploadObjectMinio(
				self.config.minioClient,
				self.config.minioBucket,
				uploadKey,
				datasetContent.file
			)

			# Update contentUrl to point to download endpoint
			inputDataset.contentUrl = f"{self.config.baseUrl}/dataset/download/{inputDataset.guid}"

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
			"descriptiveStatistics": {}, 
			"isPartOf": inputDataset.isPartOf if inputDataset.isPartOf else None,
			"dateCreated": now,
			"dateModified": now
			})

		# insert identifier metadata into mongo
		insertResult = self.config.identifierCollection.insert_one(
			outputDataset.model_dump(by_alias=True, mode="json")
		)

		# TODO handle insert errors

		# Update parent RO-Crates' hasPart if isPartOf is set
		if inputDataset.isPartOf and len(inputDataset.isPartOf) > 0:

			failed_updates = []
			for parent in inputDataset.isPartOf:
				# Only update if parent is a RO-Crate
				parent_doc = self.config.identifierCollection.find_one(
					{"@id": parent.guid},
					projection={"@type": 1, "_id": 0}
				)

				if parent_doc and parent_doc.get('@type') == MetadataTypeEnum.ROCRATE.value:
					success = addEntityToROCrate(
						self.config.identifierCollection,
						parent.guid,
						inputDataset.guid,
						MetadataTypeEnum.DATASET.value,
						inputDataset.name
					)

					if not success:
						failed_updates.append(parent.guid)

			# Rollback if any RO-Crate update failed
			if failed_updates:
				#TO-DO handle failed update
				pass
		
  		# Logic
		if distribution and distribution.distributionType == DistributionTypeEnum.MINIO:
			
			file_path = distribution.location.path
			filename = file_path.split('/')[-1]
			_, file_ext = pathlib.Path(filename).stem, pathlib.Path(filename).suffix
			supported_extensions = ['.csv', '.tsv', '.xlsx', '.xls', '.parquet', '.hdf5']

			if file_ext.lower() in supported_extensions:
				try:
					identifier_request = IdentifierRequest(self.config)
					stats = identifier_request.generateStatistics(
						guid=inputDataset.guid,
						fileName=filename
					)
					print(f"Generated statistics for dataset {inputDataset.guid}")
				except Exception as e:
					print(f"Warning: Failed to generate statistics for {inputDataset.guid}: {e}")

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
		
		setUpdateValues = updateInstance.set.model_dump(mode="json", exclude_unset=True)
		pushUpdateValues = updateInstance.push.model_dump(mode="json", exclude_unset=True)

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

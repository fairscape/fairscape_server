
from fairscape_mds.models.user import Permissions, UserWriteModel, checkPermissions
from fairscape_mds.models.dataset import DatasetWriteModel, DatasetDistribution, DistributionTypeEnum
from fairscape_mds.crud.fairscape_request import FairscapeRequest
from fairscape_mds.crud.fairscape_response import FairscapeResponse
from fairscape_mds.crud.identifier import deleteIdentifier
from fairscape_mds.models.rocrate import (
	ROCrateUploadRequest, 
	ROCrateMetadataElemWrite
)
from fairscape_mds.models.identifier import (
	StoredIdentifier,
	MetadataTypeEnum,
	PublicationStatusEnum,
	determineMetadataType
)

from typing import Optional, Dict, Any
from fairscape_models import ROCrateV1_2, ROCrateMetadataElem, Dataset, GenericMetadataElem
import traceback

import pydantic
import pymongo
import fastapi
import json
import uuid
import pathlib
import datetime
import re
import botocore

# ROCrate Helper Functions

def userPath(inputEmail):
	searchResults = re.search("(^[a-zA-Z-1-9_.+-]+)@", inputEmail) 

	if searchResults is None:
			raise Exception
	else:
			return searchResults.group(1)


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


class FairscapeROCrateRequest(FairscapeRequest):

	def __init__(self, config):
		super().__init__(config)
		self.config = config

	def uploadROCrate(
		self, 
		userInstance: UserWriteModel, 
		rocrate: fastapi.UploadFile
	):
		
		# set upload path
		rocrateFilepath = pathlib.Path(rocrate.filename)
		rocrateFilename = rocrateFilepath.name

		# get email path
		userEmailPath = userPath(userInstance.email)
		
		uploadPath = f"{self.config.minioDefaultPath}/{userEmailPath}/rocrates/{rocrateFilename}"

		# check that object doesn't already exist
		try:
			self.config.minioClient.head_object(
				Bucket=self.config.minioBucket,
				Key=uploadPath
			)

		# minio should not have an existing rocrate on this path
		except botocore.exceptions.ClientError:	

			# create a metadata record
			transactionGUID = uuid.uuid4()
			
			uploadRequestInstance = ROCrateUploadRequest.model_validate({
				"guid": str(transactionGUID),
				"transactionFolder": str(transactionGUID),
				"permissions": userInstance.getPermissions(),
				"uploadPath": str(uploadPath)
			})

			# TODO check insert result
			# create record in the async collection
			insertResult = self.config.asyncCollection.insert_one(
				uploadRequestInstance.model_dump(mode='json')
			)
			
			rocrate.file.seek(0)
			
			uploadOperationResult = self.config.minioClient.upload_fileobj(
					Bucket = self.config.minioBucket,
					Key = str(uploadPath),
					Fileobj = rocrate.file,
					ExtraArgs = {'ContentType': 'application/zip'}
			)


			# return a response
			response = FairscapeResponse(
				success=True, 
				statusCode=200, 
				model=uploadRequestInstance
				)

			return response

		else:
			return FairscapeResponse(
				success=False, 
				statusCode=400, 
				error={
					"message": "rocrate already exists, rename rocrate",
					"path": uploadPath
				})



	def processTaskWriteDatasets(
		self,
		userInstance: UserWriteModel, 
		rocrateInstance: ROCrateV1_2, 
		uploadPath: str,
		includeStem: bool,
		stem: str = None
	):
		""" Write ROCrate metadata to identifier collection for all dataset elements.

		Args:
				identifierCollection (pymongo.synchronous.collection.Collection): Collection to insert Identifier metadata
				userInstance (UserWriteModel): User Record for the user inserting the metadata
				rocrateInstance (fairscape_models.rocrate.ROCratev1_2): ROCrate Metadata as a pydantic model
				includeStem (bool):  look for object keys at the stemmed path
				stem (str): the stem of the folder path, the name of the top level folder

		Returns:
				List[str]: List of All Dataset Identifiers Minted
		"""

		baseUrl = self.config.baseUrl	
		datasetWriteList = []

		permissionsSet = userInstance.getPermissions()

		now = datetime.datetime.now()

		rocrateElem = rocrateInstance.getCrateMetadata()
		rocrateGUID = rocrateElem.guid

		datasetList = []

		for elem in rocrateInstance.metadataGraph:
			if isinstance(elem, Dataset):
				datasetList.append(elem)
			elif isinstance(elem, GenericMetadataElem) and 'Dataset' in elem.metadataType:
				datasetList.append(elem)

		for datasetElem in datasetList:

			if not datasetElem.contentUrl or datasetElem.contentUrl == 'Embargoed':

				outputDataset = StoredIdentifier.model_validate({
					"@id": datasetElem.guid,
					"@type": MetadataTypeEnum.DATASET,
					"metadata": datasetElem,
					"permissions": permissionsSet, 
					"distribution": None,	
					"publicationStatus": PublicationStatusEnum.EMBARGOED,
					"dateCreated": now,
					"dateModified": now,
				})

				output_json = outputDataset.model_dump(
					by_alias=True,
					mode='json'
				)

			else:

				if 'ftp://' in datasetElem.contentUrl:
					distribution = DatasetDistribution.model_validate({
							"distributionType": 'ftp',
							"location": {"uri": datasetElem.contentUrl}
							})

					outputDataset = StoredIdentifier.model_validate({
						"@id": datasetElem.guid,
						"@type": MetadataTypeEnum.DATASET,
						"metadata": datasetElem,
						"permissions": permissionsSet, 
						"distribution": distribution,	
						"publicationStatus": PublicationStatusEnum.DRAFT,
						"dateCreated": now,
						"dateModified": now,
					})
					
					output_json = outputDataset.model_dump(
						by_alias=True,
						mode='json'
					)

				if 'http://' in datasetElem.contentUrl or 'https://' in datasetElem.contentUrl:
					# create distribution for metadata
					distribution = DatasetDistribution.model_validate({
							"distributionType": 'url',
							"location": {"uri": datasetElem.contentUrl}
							})

					outputDataset = StoredIdentifier.model_validate({
						"@id": datasetElem.guid,
						"@type": MetadataTypeEnum.DATASET,
						"metadata": datasetElem,
						"permissions": permissionsSet, 
						"distribution": distribution,	
						"publicationStatus": PublicationStatusEnum.DRAFT,
						"dateCreated": now,
						"dateModified": now,
					})

					output_json = outputDataset.model_dump(
						by_alias=True,
						mode='json'
					)

				if 'file:///' in datasetElem.contentUrl:	
					# match the metadata path to content
					contentUrlKey = datasetElem.contentUrl.lstrip("file:///")

					# if file in datasetInstance
					if includeStem:
						objectKey = f"{uploadPath}/{stem}/{contentUrlKey}"
					else:
						objectKey = f"{uploadPath}/{contentUrlKey}"

					try:
						response = self.config.minioClient.head_object(
							Bucket= self.config.minioBucket,
							Key=objectKey
						)
						
					except botocore.exceptions.ClientError as e:	
						raise Exception(f"message: Object Key Not Found\tkey: {objectKey}\tbucket: {self.config.minioBucket}")

					objectSize = response.get("ContentLength")
						
					# Update contentUrl for created dataset
					datasetElem.contentUrl = f"{baseUrl}/dataset/download/{datasetElem.guid}"
					
					# create distribution for metadata
					distribution = DatasetDistribution.model_validate({
							"distributionType": 'minio',
							"location": {"path": objectKey}
							})
						
					outputDataset = StoredIdentifier.model_validate({
						"@id": datasetElem.guid,
						"@type": MetadataTypeEnum.DATASET,
						"metadata": {
							**datasetElem.model_dump(by_alias=True, mode='json'),
							"size": objectSize,
							"isPartOf": {
							"@id": rocrateGUID,
							"@type": MetadataTypeEnum.ROCRATE,
							"name": rocrateElem.name
							}
						},
						"permissions": permissionsSet, 
						"distribution": distribution,	
						"publicationStatus": PublicationStatusEnum.DRAFT,
						"dateCreated": now,
						"dateModified": now,
					})

					output_json = outputDataset.model_dump(
						by_alias=True,
						mode='json'
					)
	
			# insert all identifiers for datasets
			insertResult = self.config.identifierCollection.insert_one(
				output_json
			)

			# TODO: check insertResult for success
			if not insertResult.inserted_id:
				print(f"error writing {output_json.get('@id')}")

			# append guid to dataset list
			datasetWriteList.append(outputDataset.guid)

		return datasetWriteList


	def processTaskWriteMetadataElements(
		self,
		userInstance,
		rocrateInstance
	):
		""" Write ROCrate metadata for all elements excluding datasets

		Args:
				identifierCollection (pymongo.synchronous.collection.Collection): Collection to insert Identifier metadata
				userInstance (UserWriteModel): User Record for the user inserting the metadata
				rocrateInstance (fairscape_models.rocrate.ROCratev1_2): ROCrate Metadata as a pydantic model

		Returns:
				List[str]: List of all ARKs minted
		"""
		metadataCollection = self.config.identifierCollection
		
		userPermissions = userInstance.getPermissions()
		crateGUID = rocrateInstance.getCrateMetadata().guid
		
		# written identifiers
		guidList = []

		now = datetime.datetime.now()

		# mint all metadata elements
		for metadataModel in rocrateInstance.metadataGraph:
			if isinstance(metadataModel.metadataType,list):
				#if 'https://w3id.org/EVI#ROCrate' in metadataModel.metadataType:
				continue

			if 'Dataset' in metadataModel.metadataType or metadataModel.guid == 'ro-crate-metadata.json':
				continue
			else:

				processedMetadataType = determineMetadataType(metadataModel.metadataType)

				if processedMetadataType == MetadataTypeEnum.CREATIVE_WORK:
					continue
				
				metadataDict = metadataModel.model_dump(by_alias=True, mode="json")
				insertIdentifier = StoredIdentifier.model_validate({
					"@id": metadataModel.guid,
					"@type": processedMetadataType,
					"metadata": metadataDict,
					"permissions": userPermissions, 
					"distribution": None,	
					"publicationStatus": PublicationStatusEnum.DRAFT,
					"dateCreated": now,
					"dateModified": now,
				})
				
				insertResult = metadataCollection.insert_one(
					insertIdentifier.model_dump(
						by_alias=True,
						mode='json'
					)
				)

				if not insertResult.inserted_id:
					raise Exception(
						f"Writing Identifier To Mongo Failed id: {insertIdentifier.guid}"
						)
				
				guidList.append(metadataModel.guid)	

		return guidList


	def getROCrateContentsMinio(self, zipCratePath: str):
		# helper function to get all contents inside a zipped crate when there are more than 1k objects
		# list entire subdirectory for rocrate upload
		listObjects = self.config.minioClient.list_objects_v2(
				Bucket= self.config.minioBucket,
				Prefix= str(zipCratePath) 	
				)

		objectList = listObjects['Contents']

		isTruncated = listObjects.get('IsTruncated')
		nextContinueToken = listObjects.get('NextContinuationToken')

		while isTruncated:

			listObjects = self.config.minioClient.list_objects_v2(
				Bucket = self.config.minioBucket,
				Prefix = zipCratePath,
				ContinuationToken = nextContinueToken
			)
			nextContinueToken = listObjects.get('NextContinuationToken')
			isTruncated = listObjects.get('IsTruncated')
			objectList= objectList + listObjects['Contents']

		return objectList


	def processTaskGetInitialJobMetadata(self, transactionGUID: str):
		uploadMetadata = self.config.asyncCollection.find_one(
			{"guid": transactionGUID}, 
			{"_id": 0}
			)
		
		if uploadMetadata is None:
			raise Exception('Upload Metadata not found')
		
		# TODO try and except for ROCrate Validation
		uploadInstance = ROCrateUploadRequest.model_validate(uploadMetadata)
		
		# find the user uploading the ROCrate to set permissions
		userMetadata = self.config.userCollection.find_one(
				{"email": uploadInstance.permissions.owner }
		)
		
		if userMetadata is None:
			raise Exception('User Metadata not found')

		# TODO try and except for user validation Validation
		foundUser = UserWriteModel.model_validate(userMetadata)
		
		return foundUser, uploadInstance


	def processTaskGetMetadata(self, rocrateContents):

		storedROCrateMetadataFiles = list(
			filter(
				lambda elem: 'ro-crate-metadata.json' in elem.get("Key"), 
				rocrateContents
			)
		)

		# TODO handle multiple ROCrate metadata files 
		if len(storedROCrateMetadataFiles)>1:
			raise Exception('Multiple ROCrate Metadata Files not Supported')
		elif len(storedROCrateMetadataFiles) == 0 :
			raise Exception('ROCrate Metadata File is Not Included')	
		else:
			# get the metadata file 
			metadataFile = storedROCrateMetadataFiles[0]
			metadataFileKey = metadataFile.get("Key")

			# get the object
			s3Response = self.config.minioClient.get_object(
				Bucket = self.config.minioBucket,
				Key= metadataFileKey
			)	

			try:
				content = s3Response['Body'].read()
				roCrateJSON = json.loads(content)

			except json.JSONDecodeError:
				raise Exception("Error Reading ro-crate-metadata.json")

			rocrateInstance = ROCrateV1_2.model_validate(roCrateJSON)
			return rocrateInstance


	def processROCrate(self, transactionGUID: str):
		# get the current rocrate upload job

		now = datetime.datetime.now()

		self.config.asyncCollection.update_one(
        	{"guid": transactionGUID},
        	{"$set": 
            	{
                	"stage": "starting job"
            	}
        	}
    	)

		foundUser, uploadInstance = self.processTaskGetInitialJobMetadata(transactionGUID)
		zippedCratePath = uploadInstance.uploadPath

		# get ROCrateContents needs the path to terminate with / to get subcontents of Zip
		if zippedCratePath.endswith("/"):
			pass
		else:
			zippedCratePath = zippedCratePath + "/"

		# get rocrate metadata
		uploadPathString = uploadInstance.uploadPath

		if not uploadPathString:
			raise Exception(
				"ROCrate Upload Job Missing Upload Path Property"
				)


		jobUploadPath = pathlib.PurePosixPath(uploadInstance.uploadPath)
		baseDirectory = uploadInstance.uploadPath 
		metadataKey = baseDirectory + "/ro-crate-metadata.json"
		metadataFound = False
		stem = jobUploadPath.stem
		includeStem = False


		try: 
			s3Response = self.config.minioClient.get_object(
				Bucket = self.config.minioBucket,
				Key = metadataKey
			)
			metadataFound = True
		except self.config.minioClient.exceptions.NoSuchKey:
			metadataFound = False

		if not metadataFound:

			metadataKey = f"{baseDirectory}/{jobUploadPath.stem}/ro-crate-metadata.json"

			try: 
				s3Response = self.config.minioClient.get_object(
					Bucket = self.config.minioBucket,
					Key = metadataKey
				)
				metadataFound = True
			except self.config.minioClient.exceptions.NoSuchKey:
				metadataFound = False

		if metadataFound:
			includeStem = True
			try:
				content = s3Response['Body']
				roCrateJSON = json.loads(content.read())

			except json.JSONDecodeError as e:

				self.config.asyncCollection.update_one(
					{"guid": transactionGUID},
					{"$set": 
						{
							"stage": "reading metadata",
							"error": str(e),
							"timeFinished": datetime.datetime.now(),
							"success": False,
							"completed": True
						}
					}
				)	
				raise Exception("Failed to Decode Metadata JSON")

		else:
			raise Exception("Metadata Not Found in RO-Crate")

		# validate
		try:
			roCrateModel = ROCrateV1_2.model_validate(roCrateJSON)
		except pydantic.ValidationError as e:
			print(f"ValidationError: {str(e)}")
			traceback.print_exc()

			# update job as failure
			self.config.asyncCollection.update_one(
				{"guid": transactionGUID},
				{"$set": 
					{
						"stage": "reading metadata",
						"error": json.loads(e.json()),
						"timeFinished": datetime.datetime.now(),
						"success": False,
						"completed": True
					}
				}
			)	

			return None

		crateMetadataElem = roCrateModel.getCrateMetadata()


		# if a terminating backslash is present on the identifier trim
		# TODO apply to all identifiers
		if crateMetadataElem.guid.endswith("/"):
			crateMetadataElem.guid = crateMetadataElem.guid.rstrip("/")

		roCrateGUID = crateMetadataElem.guid

		self.config.asyncCollection.update_one(
			{"guid": transactionGUID},
			{"$set": 
				{
					"stage": "found metadata",
					"rocrateGUID": roCrateGUID
				}
			}
		)	

		# check identifier conflicts	
		self.config.asyncCollection.update_one(
			{"guid": transactionGUID},
			{"$set": 
				{
					"stage": "checking for identifier conflicts",
				}
			}
		)	

		# check for ROCrate identifier conflict
		foundROCrateMetadata = self.config.identifierCollection.find_one({
			"@id": roCrateGUID
		})

		if foundROCrateMetadata:
			# check identifier conflicts	
			self.config.asyncCollection.update_one(
				{"guid": transactionGUID},
				{"$set": 
					{
						"timeFinished": datetime.datetime.now(),
						"complete": "True",
						"success": "False",
						"error": "Found Identifier Conflict for ROCrate"
					}
				}
			)	

			# clean up job
			self.config.minioClient.delete_object(
				Bucket=self.config.minioBucket,
				Key=uploadInstance.uploadPath
			)

			raise Exception(f"ROCrate Identifier Conflict: {roCrateGUID}")


		self.config.asyncCollection.update_one(
			{"guid": transactionGUID},
			{"$set": 
				{
					"stage": "minting datasets",
				}
			}
		)	

		# write dataset records
		datasetGUIDS = self.processTaskWriteDatasets(
			foundUser, 
			roCrateModel, 
			uploadInstance.uploadPath,
			includeStem,
			stem
		)
		
		# write metadata elements
		nonDatasetGUIDS = self.processTaskWriteMetadataElements(
				foundUser,
				roCrateModel
		)


		# write the metadata elem as an identifier to identifierCollection and ROCrate
		metadataElem = roCrateModel.getCrateMetadata()
		
		roCrateDistribution = DatasetDistribution.model_validate({
			"distributionType": 'minio',
			"location": {"path": uploadInstance.uploadPath}
			})

		# set hasPart for metadata element
		metadataElem.hasPart = [
			{
				"@id": elem.guid,
				"@type": elem.metadataType,
				"name": elem.name
			} for elem in roCrateModel.metadataGraph if elem.guid != "ro-crate-metadata.json"] 

		# TODO needs to be stored identifier		
		storedMetadataElem = StoredIdentifier.model_validate({
			"@id": metadataElem.guid,
			"@type": MetadataTypeEnum.ROCRATE,
			"permissions": foundUser.getPermissions().model_dump(mode='json', by_alias=True) ,              
			"metadata": metadataElem,
			"distribution": roCrateDistribution.model_dump(
				mode='json', 
				by_alias=True
			),                
			"publicationStatus": PublicationStatusEnum.DRAFT,
			"dateCreated": now,
			"dateModified": now
		})

		# suppresses warnings from serializer handling nested models
		storedMetadataElem.model_rebuild()
	
		# dump into identifier collection and rocrate collection
		insertResult = self.config.identifierCollection.insert_one(
			storedMetadataElem.model_dump(
				by_alias=True,
				mode='json',
				warnings=False
				) 
			)

		# TODO check insert result is correct

		# TODO documents too large causes errors 
		# write the whole ROCrateV1_2 model into the rocrate collection
		#rocrate_doc_for_collection = {
		#	"@id": metadataElem.guid,
		#	"@type": ['Dataset', "https://w3id.org/EVI#ROCrate"], 
		#	"owner": foundUser.email,
		#	"permissions": foundUser.getPermissions().model_dump(mode='json', by_alias=True),
		#	"metadata": roCrateModel.model_dump(mode='json', by_alias=True) 
		#}
		#self.config.rocrateCollection.insert_one(rocrate_doc_for_collection)
  
		# update process as success
		updateResult = self.config.asyncCollection.update_one(
				{"guid": uploadInstance.guid},
				{"$set": {
						"completed": True,
						"identifiersMinted": len(datasetGUIDS + nonDatasetGUIDS)+1,
						"rocrateIdentifier": metadataElem.guid,
						"timeFinished": datetime.datetime.now(),
						"success": True,
						"status": "finished",
						"stage": "completed all tasks successfully"
				}}
		)

		# check update result
		if updateResult.modified_count != 1:
			raise Exception(f"Failed to Update Job Metadata: {uploadInstance.guid}")

		return metadataElem.guid


	def getUploadMetadata(self, requestingUser: UserWriteModel, transactionGUID: str):
		# get upload metadata
		uploadMetadata = self.config.asyncCollection.find_one({
				"guid": transactionGUID
		})

		if uploadMetadata is None:
			return FairscapeResponse(
					success=False,
					statusCode=404,
					error={"message": "upload request not found"}
					)
		
		uploadInstance = ROCrateUploadRequest.model_validate(uploadMetadata)

		# check that user has permission to view upload request
		if checkPermissions(uploadInstance.permissions, requestingUser):
			return FairscapeResponse(
					model=uploadInstance,
					success=True,
					statusCode=200
			)
		else:
			return FairscapeResponse(
					success=False,
					statusCode=401,
					error={"message": "user unauthorized to view upload status"}
			)

	def _build_rocrate_structure(self, root_guid: str, root_metadata: dict, parts: list) -> dict:
		context = {
			"@vocab": "https://schema.org/",
			"EVI": "https://w3id.org/EVI#",
			"rai":"http://mlcommons.org/croissant/RAI/"
		}
		
		graph = [
			{
				"@id": "ro-crate-metadata.json",
				"@type": "CreativeWork",
				"conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"},
				"about": {"@id": root_guid}
			},
			root_metadata
		]
		
		graph.extend(parts)
		
		return {
			"@context": context,
			"@graph": graph
		}

	def getROCrateMetadata(self, rocrateGUID: str):
		root_doc = self.config.identifierCollection.find_one(
			{"@id": rocrateGUID},
			projection={"_id": False}
		)
		
		if not root_doc:
			return FairscapeResponse(
				success=False,
				statusCode=404,
				error={"message": "rocrate not found"}
			)
		
		root_metadata = root_doc.get("metadata", {})
		
		has_part = root_metadata.get("hasPart", [])
		
		if not has_part:
			rocrate_doc = self._build_rocrate_structure(rocrateGUID, root_metadata, [])
			return FairscapeResponse(
				success=True,
				statusCode=200,
				model=rocrate_doc
			)
		
		part_guids = [part.get("@id") for part in has_part if part.get("@id")]
		
		parts_cursor = self.config.identifierCollection.find(
			{"@id": {"$in": part_guids}},
			projection={"_id": False}
		)
		
		parts_metadata = []
		for part_doc in parts_cursor:
			part_metadata = part_doc.get("metadata", {})
			if part_metadata:
				parts_metadata.append(part_metadata)
		
		rocrate_doc = self._build_rocrate_structure(rocrateGUID, root_metadata, parts_metadata)
		
		return FairscapeResponse(
			success=True,
			statusCode=200,
			model=rocrate_doc
		)


	def getROCrateMetadataElem(self, rocrateGUID: str):
		rocrateMetadata = self.config.identifierCollection.find_one(
			{
				"@id": rocrateGUID
			},
			projection={"_id": False}
		)
		
		# if no metadata is found return 404
		if not rocrateMetadata:
			return FairscapeResponse(
					success=False,
					statusCode=404,
					error={"message": "rocrate not found"}
			)
		else:
			rocrateModel = ROCrateMetadataElemWrite.model_validate(rocrateMetadata)
			return FairscapeResponse(
					success=True,
					model=rocrateModel,
					statusCode=200
			)
		
	
	def downloadROCrateArchive(
		self, 
		requestingUser: UserWriteModel, 
		rocrateGUID: str
	):

		rocrateIdentifier = self.config.identifierCollection.find_one(
			{"@id": rocrateGUID},
			projection={"_id": False}
		)	

		# if no metadata is found return 404
		if not rocrateIdentifier:
				return FairscapeResponse(
						success=False,
						statusCode=404,
						error={"message": "rocrate not found"}
				)

		# TODO handle validation errors
		storedROCrate = StoredIdentifier.model_validate(rocrateIdentifier)



		if not checkPermissions(storedROCrate.permissions, requestingUser):
			return FairscapeResponse(
				success=False,
				statusCode=401,
				error={"message": "user unauthorized to download rocrate archive"}
			)

		if storedROCrate.distribution:
			if storedROCrate.distribution.location:
				if storedROCrate.distribution.distributionType==DistributionTypeEnum.MINIO:
					# get the object from s3
					# TODO handle key missing error
					objectResponse = self.config.minioClient.get_object(
							Bucket=self.config.minioBucket,
							Key=storedROCrate.distribution.location.path
					)

					# create a FairscapeResponse with a fileResponse item
					return FairscapeResponse(
							success=True,
							statusCode=200,
							fileResponse=objectResponse.get('Body'),
							model=storedROCrate
					)

		return FairscapeResponse(
			success=False,
			statusCode=400,
			jsonResponse={"error": "Dataset Not Stored Locally"}
		)


	def _validateMetadataOnlyCrate(self, crateModel: ROCrateV1_2) -> Optional[dict]:
		"""Checks for file:// contentUrls in Datasets."""
		errors = {}
		datasets = crateModel.getDatasets()

		file_content_urls = {}

		for crateDataset in datasets:
			if crateDataset.contentUrl:
				if isinstance(crateDataset.contentUrl, list):
					for url in crateDataset.contentUrl:
						if url and isinstance(url, str) and url.lower().startswith("file://"):
							file_content_urls[crateDataset.guid] = url
				elif isinstance(crateDataset.contentUrl, str) and crateDataset.contentUrl.lower().startswith("file://"):
					file_content_urls[crateDataset.guid] = crateDataset.contentUrl	

		if file_content_urls:
			errors["message"] = "Metadata-only ROCrates cannot contain local file references (file://)"
			errors["details"] = file_content_urls
			return errors
		return None

	def mintMetadataOnlyROCrate(
			self,
			requestingUser: UserWriteModel,
			crateModel: ROCrateV1_2
		) -> FairscapeResponse:
			"""
			Mints metadata records for an ROCrate and its elements without file content upload.
			- Stores the full ROCrate model dump in rocrateCollection (keyed by root GUID).
			- Stores individual contextual element dumps in identifierCollection (keyed by element GUIDs),
			wrapped in MongoDocument.
			- Does NOT update the user model's lists of associated identifiers.
			"""
			try:
				crateModel.cleanIdentifiers()
			except Exception as e:
				return FairscapeResponse(
					success=False, statusCode=500,
					error={"message": "Failed to clean ROCrate identifiers", "details": str(e)}
				)
		
			validation_errors = self._validateMetadataOnlyCrate(crateModel)
			if validation_errors:
				return FairscapeResponse(
					success=False, statusCode=400, error=validation_errors
				)
		
			root_metadata_entity = crateModel.getCrateMetadata()
			if not root_metadata_entity or not root_metadata_entity.guid:
				return FairscapeResponse(
					success=False, statusCode=400,
					error={"message": "ROCrate root descriptor or its @id is missing."}
				)
			root_guid = root_metadata_entity.guid
			user_permissions = requestingUser.getPermissions()

			now = datetime.datetime.now()
		
			# 1. Store the full RO-Crate dump in rocrateCollection
			try:

				rocrate_doc_for_rocrate_collection = StoredIdentifier.model_validate({
					"@id": root_guid,
					"@type": MetadataTypeEnum.ROCRATE,
					"metadata": crateModel,
					"permissions": user_permissions,
					"distribution": None,
					"publicationStatus": PublicationStatusEnum.DRAFT,
					"dateCreated": now,
					"dateModified": now
				})
				insertResult = self.config.rocrateCollection.insert_one(
					rocrate_doc_for_rocrate_collection.model_dump(by_alias=True, mode="json")
				)
			except pymongo.errors.DuplicateKeyError:
				return FairscapeResponse(
					success=False, statusCode=409,
					error={"message": f"ROCrate with @id '{root_guid}' already exists in rocrateCollection."}
				)
			except Exception as e:
				return FairscapeResponse(
					success=False, statusCode=500,
					error={"message": "Database error storing full ROCrate model.", "details": str(e)}
				)
		
			user_permissions = requestingUser.getPermissions().model_dump(mode='json')
			
			# Initialize collections to store results
			documents_for_identifier_collection = []
			minted_element_guids = []
			
			# Process each element in the ROCrate metadata graph
			for elem in crateModel.metadataGraph:
				if elem.metadataType in ["Project", "Organization"] or elem.guid == "ro-crate-metadata.json":
					continue

				try:

					# TODO future proof for more list types
					elemMetadataType = determineMetadataType(elem.metadataType)
					
					metadataDict = elem.model_dump(by_alias=True, mode="json")
					element_document_data = StoredIdentifier.model_validate({
						"@id": elem.guid,
						"@type": elemMetadataType, 
						"owner": requestingUser.email, 
						"permissions": user_permissions,
						"metadata": metadataDict,
						"distribution": None,
      					"publicationStatus": PublicationStatusEnum.DRAFT,	
						"dateCreated": now,
						"dateModified": now
					})

					documents_for_identifier_collection.append(element_document_data.model_dump(by_alias=True, mode="json"))
					minted_element_guids.append(elem.guid)
		
				except Exception as e:
					print(f"Unexpected error processing element {elem.guid}: {e}")
					return FairscapeResponse(
						success=False, statusCode=500,
						error={"message": f"Unexpected error processing element {elem.guid}", "details": str(e)}
					)

			if not documents_for_identifier_collection:
				print("No individual elements to insert into identifierCollection after filtering.")
				return FairscapeResponse(
					success=True, statusCode=201,
					model={"rocrate_guid": root_guid, "minted_element_identifiers": []}
				)

			try:
				insert_identifier_result = self.config.identifierCollection.insert_many(documents_for_identifier_collection)

				if len(insert_identifier_result.inserted_ids) != len(documents_for_identifier_collection):
					print(f"Warning: Inserted fewer documents ({len(insert_identifier_result.inserted_ids)}) into identifierCollection than expected ({len(documents_for_identifier_collection)})")
					return FairscapeResponse(
						success=False, statusCode=500,
						error={"message": "Database error: Partial insert into identifierCollection occurred"}
					)

			except pymongo.errors.BulkWriteError as bwe:
				print(f"Bulk write error during insert into identifierCollection: {bwe.details}")
				return FairscapeResponse(
					success=False, statusCode=500,
					error={"message": "Database error during bulk insert into identifierCollection", "details": bwe.details}
				)
			except Exception as e:
				print(f"Unexpected database error during insert_many into identifierCollection: {e}")
				return FairscapeResponse(
					success=False, statusCode=500,
					error={"message": "Unexpected database error storing elements.", "details": str(e)}
				)

			return FairscapeResponse(
				success=True, statusCode=201,
				model={"rocrate_guid": root_guid, "minted_element_identifiers": minted_element_guids}
			)

	def deleteROCrate(
		self,				 
		requestingUser: UserWriteModel, 
		guid: str
	):
		""" Mark ROCrate as status ARCHIVED
		"""

		return deleteIdentifier(
			self.config.identifierCollection,
			requestingUser,
			ROCrateMetadataElemWrite,
			guid
		)


	def list_crates(
			self,
			requestingUser: UserWriteModel
		) -> FairscapeResponse:
			"""
			Lists RO-Crates based on user permissions.
			Admins see all crates. Regular users see crates they own or that belong to their primary group.
			"""
			query: Dict[str, Any] = {}
			is_admin = self.config.adminGroup in requestingUser.groups

			if not is_admin:
				user_primary_group = requestingUser.groups[0] if requestingUser.groups else None
				
				or_conditions = [{"owner": requestingUser.email}]
				if user_primary_group:
					or_conditions.append({"permissions.group": user_primary_group})
				
				query["$or"] = or_conditions
			
			try:
				cursor = self.config.rocrateCollection.find(
					query,
					projection={
						"_id": 0,
						"@id": 1,
						"metadata": 1,
					}
				)

				crates_list = []
				for crate_doc in cursor:
					crate_id = crate_doc.get("@id")
					crates_list.append({
						"@id": crate_id,
						"name": crate_doc.get('metadata',{}).get("@graph",[{},{}])[1].get("name"),
						"description": crate_doc.get('metadata',{}).get("@graph",[{},{}])[1].get("description"),
						"@graph": []
					})

				return FairscapeResponse(
					success=True,
					statusCode=200,
					model={"rocrates": crates_list}
				)

			except Exception as e:
				return FairscapeResponse(
					success=False,
					statusCode=500,
					error={"message": f"Error listing RO-Crates: {str(e)}"})

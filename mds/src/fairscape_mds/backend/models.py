import fastapi
from pydantic import (
    BaseModel,
    Field,
		ValidationError
)
from typing import (
    Optional,
    List,
    Literal,
    Dict,
    Any
)
import datetime
import jwt
import pymongo
from pymongo.collection import Collection
import struct
import copy

from fairscape_mds.core.config import FairscapeConfig

from fairscape_models.computation import Computation
from fairscape_models.software import Software
from fairscape_models.schema import Schema
from fairscape_models.dataset import Dataset
from enum import Enum

ADMIN_GROUP_IDENTIFIER = "admin"

class StoredIdentifer(BaseModel):
	guid: str

class PublicationStatusEnum(Enum):
	DRAFT = 0
	PUBLISHED = 1
	EMBARGOED = 2
	ARCHIVED = 3


	

class FairscapeResponse():
	def __init__(
		self, 
		success: bool, 
		statusCode: int, 
		model=None, 
		fileResponse=None, 
		error: dict= {},
		jsonResponse: dict={}
	):
		self.model = model
		self.success = success
		self.statusCode = statusCode
		self.error = error
		self.fileResponse = fileResponse
		self.jsonResponse = jsonResponse


class FairscapeRequest():
	def __init__(
			self, 
			backendConfig: FairscapeConfig
	):
		self.config = backendConfig
  
	def getMetadata(self, guid: str):
		return self.config.identifierCollection.find_one({"@id": guid}, {"_id": 0})




#######################
#      User Models    #
#######################

class Permissions(BaseModel):
	owner: str
	group: Optional[str] = Field(default=None)


class UserCreateModel(BaseModel):
	email: str
	firstName: str
	lastName: str
	password: str


class UserWriteModel(UserCreateModel):
	metadataType: Literal['Person'] = Field(alias="@type", default="Person")
	session: Optional[str] = Field(default=None)
	groups: Optional[List[str]] = Field(default=[])
	datasets: Optional[List[str]] = Field(default=[])
	software: Optional[List[str]] = Field(default=[])
	computations: Optional[List[str]] = Field(default=[])
	rocrates: Optional[List[str]] = Field(default=[])

	def getPermissions(self)->Permissions:
		permissionsDict = {
				"owner": self.email,
		}
		
		if len(self.groups)>0:
			permissionsDict['group'] = self.groups[0]
		else:
			permissionsDict['group'] = None

		return Permissions.model_validate(permissionsDict)


def checkPermissions(
	permissionsInstance: Permissions, 
	requestingUser: UserWriteModel
	):

	if permissionsInstance.owner == requestingUser.email:
		return True
	elif permissionsInstance.group:
		if permissionsInstance.group in requestingUser.groups:
			return True
	else:
		return False


class FairscapeUserRequest(FairscapeRequest):
	
	def createUser(self, userInstance):
		userWriteInstance = UserWriteModel.model_validate({**userInstance.model_dump()})
		
		insertResult = self.config.userCollection.insert_one(
				userWriteInstance.model_dump(by_alias=True)
		)

		# check that insertResult was successfull

		return insertResult

	def loginUser(self, userEmail: str, userPassword: str):
		""" Get a user record, create a session for the 
		"""
	
		foundUser = self.config.userCollection.find_one({
				"email": userEmail,
				"password": userPassword
		})

		if foundUser is None:
				return FairscapeResponse(
					success=False,
					statusCode=401,
					jsonResponse={"error": "credentials not found"}
				)

		# create a token for the user
		userEmail = foundUser['email']
		fullname = ' '.join([foundUser['firstName'], foundUser['lastName']])
		now = datetime.datetime.now(datetime.timezone.utc)
		exp = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=24)

		nowTimestamp = datetime.datetime.timestamp(now)
		expTimestamp = datetime.datetime.timestamp(exp)

		tokenMessage = {
			'iss': 'https://fairscape.net/',
			'sub':  userEmail,
			'name': fullname,
			#'email': userEmail,
			'iat': int(nowTimestamp),
			'exp': int(expTimestamp)
		}

		compactJWS = jwt.encode(
				tokenMessage, 
				self.config.jwtSecret, 
				algorithm="HS256"
		)
		
		# set session in userCollection
		updateTokenResult = self.config.userCollection.update_one({
				"email": userEmail,
				"password": userPassword
				},
				{
				"$set": {"session": compactJWS}
				}
		)

		# check that update is correct
		if updateTokenResult.matched_count == 1 and updateTokenResult.modified_count == 1:

			return FairscapeResponse(
				success = True,
				jsonResponse = {"access_token": compactJWS},
				statusCode = 200
			)

		else:
			
			return FairscapeResponse(
				success = False,
				jsonResponse = {
					"error": "failed to set token"
					},
				statusCode = 500
			)


	def getUserBySession(self, session: str):

		tokenMetadata = jwt.decode(
			jwt=session,
			key=self.config.jwtSecret,
			algorithms=["HS256"]
		)

		userEmail = tokenMetadata.get('sub')

		foundUser = self.config.userCollection.find_one({
			"email": userEmail
		})

		if foundUser:
				return UserWriteModel.model_validate(foundUser)
		else:
				return None

####################################
# Helper Functions for Identifiers #
####################################

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
				"published": False
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

#####################
#   Dataset Models  #
#####################
from fairscape_models.dataset import Dataset
import pathlib
from enum import Enum
from typing import Union, Optional

class DatasetCreateModel(Dataset):
	guid: Optional[str] = Field(
		title="guid",
		alias="@id",
		default=None
	)
	metadataType: Optional[str] = Field(alias="@type")
	dateRegistered: Optional[datetime.datetime] = Field(default_factory=datetime.datetime.now)

class DistributionTypeEnum(str, Enum):
	MINIO = 'minio'
	URL = 'url'
	GLOBUS = 'globus'
	FTP = 'ftp'

class MinioDistribution(BaseModel):
	path: str

class URLDistribution(BaseModel):
	uri: str

class DatasetDistribution(BaseModel):
	distributionType: DistributionTypeEnum
	location: Union[MinioDistribution, URLDistribution]


class DatasetWriteModel(DatasetCreateModel):
	published: Optional[bool] = Field(default=True)
	distribution: Optional[DatasetDistribution] = Field(default=None)
	permissions: Permissions


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
    datasetFile: fastapi.UploadFile,
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
	):
		datasetMetadata = self.getMetadata(datasetGUID)
		distribution = datasetMetadata.get('distribution')
		datasetPermissions = datasetMetadata.get('permissions')

		if distribution:
			distributionInstance = DatasetDistribution.model_validate(distribution)
			permissionsInstance = Permissions.model_validate(datasetPermissions)

			# check that datasetInstance has minio distribtuion
			if distributionInstance.distributionType != DistributionTypeEnum.MINIO:
				return FairscapeResponse(
					success=False,
					statusCode=400,
					jsonResponse={"error": "Dataset Not Stored Locally"}
				)

			else:
				# check permissions
				if checkPermissions(permissionsInstance, userInstance):

					# get the distribution location from metadata
					objectKey = distributionInstance.location.path

					response = self.config.minioClient.get_object(
						Bucket=self.config.minioBucket,
						Key=objectKey
					)

					return FairscapeResponse(
						success=True,
						statusCode=200,
						fileResponse=response,
					)
			
				else:
					return FairscapeResponse(
						success=False,
						statusCode=401,
						jsonResponse={"error": "user unauthorized"}
					)

 
	def createDataset(
		self, 
		userInstance: UserWriteModel,
		inputDataset: Dataset,
		datasetContent: Optional[fastapi.UploadFile]=None
	):
		# check if guid already exists
		foundMetadata = self.getMetadata(inputDataset.guid)

		if foundMetadata is not None:
			raise Exception('GUID Already assigned')
		
		# if no content is passed
		if datasetContent is None:
			
			# if http URI add url distribution
			if 'http' in inputDataset.contentUrl:
				distribution = DatasetDistribution.model_validate({
						"distributionType": "url",
						"location": {"uri": inputDataset.contentUrl}
						})


			# if ftp URI add url distribution
			if 'ftp' in inputDataset.contentUrl:
				distribution = DatasetDistribution.model_validate({
					"distributionType": "ftp",
					"location": {"uri": inputDataset.contentUrl}
				})

			if inputDataset.contentUrl is None:
				distribution = None 

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

		outputDataset = DatasetWriteModel.model_validate({
			**inputDataset.model_dump(by_alias=True),
			"permissions": permissionsSet, 
			"distribution": distribution,
			"published": True
			})

		# insert identifier metadata into mongo
		insertResult = self.config.identifierCollection.insert_one(
			outputDataset.model_dump(by_alias=True)
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


	def deleteDataset(
		self,		
		requestingUser: UserWriteModel, 
		guid: str
	):
		return deleteIdentifier(
			self.config.identifierCollection,
			requestingUser,
			DatasetWriteModel,
			guid
		)


#######################
#   ROCrate Backend   #
#######################

import re
import pathlib
import uuid
import json

from fairscape_models.rocrate import ROCrateV1_2, ROCrateMetadataElem
from fairscape_models.dataset import Dataset


class ROCrateUploadRequest(BaseModel):
	""" Pydantic Model for ROCrate Upload Request

	Created when an ROCrate Zip is uploaded and processed
	"""
	guid: str
	permissions: Permissions
	uploadPath: str
	rocrateGUID: Optional[str] = Field(default=None)
	timeStarted: Optional[datetime.datetime] = Field(default_factory=datetime.datetime.now)
	timeFinished: Optional[datetime.datetime] = Field(default=None)
	completed: Optional[bool] = Field(default=False)
	error: Optional[str] = Field(default=None)
	identifiersMinted: Optional[Union[int, List[str]]] = Field(default=None)
	rocrateIdentifier: Optional[str] = Field(default=None)
	transactionFolder: Optional[str] = Field(default=None)
	status: Optional[str] = Field(default=None)
	stage: Optional[str] = Field(default=None)
	success: Optional[bool] = Field(default=False)


# ROCrate Helper Functions

def GetROCrateMetadata(s3Client, bucketName, uploadInstance):

	# TODO try both with stem and not stem
	# TODO improve with searching for all ro-crate-metadata.json
	zippedCratePath = pathlib.Path(uploadInstance.uploadPath)

	# files can be nested inside a folder within the zip
	# i.e. if test.zip the contents can be test.zip/test/ro-crate-metadata.json
	stemPath = zippedCratePath / zippedCratePath.stem / 'ro-crate-metadata.json'
	directPath = zippedCratePath / 'ro-crate-metadata.json'

	def DownloadROCrateMetadata(zippedMetadataPath):	
		# get the values for the zipped metadata
		try:
			s3Response = s3Client.get_object(
					Bucket=bucketName,
					Key=str(zippedMetadataPath)
			)

			# get the metadata out of the s3 response
			content= s3Response['Body'].read()
			roCrateJSON = json.loads(content)
			return roCrateJSON
		
		except:
			return None
	
	metadata = DownloadROCrateMetadata(stemPath)

	if not metadata:
		metadata = DownloadROCrateMetadata(directPath)

	return metadata

def userPath(inputEmail):
	searchResults = re.search("(^[a-zA-Z0-9_.+-]+)@", inputEmail) 

	if searchResults is None:
			raise Exception
	else:
			return searchResults.group(1)


# set remainder of metadata for storage
def writeROCrateDataset(
	identifierCollection,
	userInstance: UserWriteModel,
	objectList,
	rocrateInstance,
	datasetElem: Dataset
):

	permissionsSet = userInstance.getPermissions()

	if not datasetElem.contentUrl:
		outputDataset = DatasetWriteModel.model_validate({
			**datasetElem.model_dump(by_alias=True),
			"permissions": permissionsSet, 
			"distribution": None,
			"isPartOf": {
					"@id": rocrateInstance.metadataGraph[1].guid,
			},		
		})

		output_json = datasetElem.model_dump(by_alias=True, mode='json')
	
	else:
		if 'http' in datasetElem.contentUrl:
			# create distribution for metadata
			distribution = DatasetDistribution.model_validate({
				"distributionType": 'url',
				"location": {"uri": datasetElem.contentUrl}
			})
			outputDataset = DatasetWriteModel.model_validate({
					**datasetElem.model_dump(by_alias=True),
					"permissions": permissionsSet, 
					"distribution": None,
					"isPartOf": {
							"@id": rocrateInstance.metadataGraph[1].guid,
					},		
			})
			output_json = datasetElem.model_dump(by_alias=True, mode='json')

		else:	
			# match the metadata path to content
			datasetCratePath = datasetElem.contentUrl.lstrip('file:///')
			
			# filter function to match content url to key
			matchedElementList = list(
					filter(
					lambda x: datasetCratePath in x.get('Key'),
					objectList
					)
			)

			if len(matchedElementList)>0:
				matchedElement = matchedElementList[0]
			else:
				print(f"ContentNotFound: {datasetElem.guid}\tPath: {datasetElem.contentUrl}")
				raise Exception
			
			# create metadata record to insert 
			objectSize = matchedElement.get('Size')
			objectPath = matchedElement.get('Key')
				
			# create distribution for metadata
			distribution = DatasetDistribution.model_validate({
					"distributionType": 'minio',
					"location": {"path": objectPath}
					})
					
			outputDataset = DatasetWriteModel.model_validate({
					**datasetElem.model_dump(by_alias=True),
					"permissions": permissionsSet, 
					"distribution": distribution,
					"size": objectSize,
					"isPartOf": {
							"@id": rocrateInstance.metadataGraph[1].guid,
					},		
			})

			output_json = {
				"@id": outputDataset.guid,
				"@type": outputDataset.metadataType,
						"metadata":outputDataset.model_dump(by_alias=True, mode='json'),
							"permissions": permissionsSet.model_dump(mode='json', by_alias=True), 
				"distribution": distribution.model_dump(by_alias=True, mode='json'),
				}
  
		# insert all identifiers for datasets
		insertResult = identifierCollection.insert_one(
			output_json
		)
	pass







class ROCrateMetadataElemWrite(ROCrateMetadataElem):
	permissions: Permissions
	published: Optional[bool] = Field(default=True)
	hasPart: Optional[List[dict]]
	distribution: Optional[DatasetDistribution]


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

		# upload zip to minio
		# Reset file position to beginning before upload
		rocrate.file.seek(0)
		
		uploadOperationResult = self.config.minioClient.upload_fileobj(
				Bucket = self.config.minioBucket,
				Key = str(uploadPath),
				Fileobj = rocrate.file,
				ExtraArgs = {'ContentType': 'application/zip'}
		)

		transactionGUID = uuid.uuid4()
		
		uploadRequestInstance = ROCrateUploadRequest.model_validate({
			"guid": str(transactionGUID),
   		"transactionFolder": str(transactionGUID),
			"permissions": userInstance.getPermissions(),
			"uploadPath": str(uploadPath)
		})
		# create record in the async collection
		insertResult = self.config.asyncCollection.insert_one(
			uploadRequestInstance.model_dump(mode='json')
		)

		# TODO check insert result

		# return a response
		response = FairscapeResponse(
			success=True, 
			statusCode=200, 
			model=uploadRequestInstance
			)

		return response


	def processTaskWriteDatasets(
		self,
		userInstance: UserWriteModel, 
		rocrateInstance: ROCrateV1_2, 
		objectList
	):
		""" Write ROCrate metadata to identifier collection for all dataset elements.

		Args:
				identifierCollection (pymongo.synchronous.collection.Collection): Collection to insert Identifier metadata
				userInstance (UserWriteModel): User Record for the user inserting the metadata
				rocrateInstance (fairscape_models.rocrate.ROCratev1_2): ROCrate Metadata as a pydantic model
				objectList (List[dict]): Content for the Zipped ROCrate from the s3 object_list_v1 call 

		Returns:
				List[str]: List of All Dataset Identifiers Minted
		"""

		baseUrl = self.config.baseUrl	
		datasetWriteList = []

		# TODO: set to userInstance for method
		permissionsSet = userInstance.getPermissions()

		for datasetElem in rocrateInstance.getDatasets():
			if not datasetElem.contentUrl or datasetElem.contentUrl == 'Embargoed':
				outputDataset = DatasetWriteModel.model_validate({
						**datasetElem.model_dump(by_alias=True),
						"permissions": permissionsSet, 
						"distribution": None,
						"isPartOf": {
								"@id": rocrateInstance.metadataGraph[1].guid,
						},		
				})

				output_json = {                   
						"@id": outputDataset.guid,
						"@type": outputDataset.metadataType,
						"metadata":outputDataset.model_dump(by_alias=True, mode='json'),
						"permissions": permissionsSet.model_dump(mode='json', by_alias=True),
						"distribution": None
						}
			else:

				if 'ftp' in datasetElem.contentUrl:
					distribution = DatasetDistribution.model_validate({
							"distributionType": 'ftp',
							"location": {"uri": datasetElem.contentUrl}
							})
					outputDataset = DatasetWriteModel.model_validate({
							**datasetElem.model_dump(by_alias=True),
							"permissions": permissionsSet, 
							"distribution": None,
							"isPartOf": {
									"@id": rocrateInstance.metadataGraph[1].guid,
							},		
					})

					# format as a write document
					output_json = {					
						"@id": outputDataset.guid,
						"@type": outputDataset.metadataType,
						"metadata":outputDataset.model_dump(by_alias=True, mode='json'),
						"permissions": permissionsSet.model_dump(mode='json', by_alias=True), 
						"distribution": distribution.model_dump(by_alias=True, mode='json'),
					}

				if 'http' in datasetElem.contentUrl:
					# create distribution for metadata
					distribution = DatasetDistribution.model_validate({
							"distributionType": 'url',
							"location": {"uri": datasetElem.contentUrl}
							})
					outputDataset = DatasetWriteModel.model_validate({
							**datasetElem.model_dump(by_alias=True),
							"permissions": permissionsSet, 
							"distribution": None,
							"isPartOf": {
									"@id": rocrateInstance.metadataGraph[1].guid,
							},		
					})

					# format as a write document
					output_json = {					
						"@id": outputDataset.guid,
						"@type": outputDataset.metadataType,
						"metadata":outputDataset.model_dump(by_alias=True, mode='json'),
						"permissions": permissionsSet.model_dump(mode='json', by_alias=True), 
						"distribution": distribution.model_dump(by_alias=True, mode='json'),
					}

				else:	
					# match the metadata path to content
					datasetCratePath = datasetElem.contentUrl.lstrip('file:///')
				
					# filter function to match content url to key
					matchedElementList = list(
							filter(
							lambda x: datasetCratePath in x.get('Key'),
							objectList
							)
					)

					if len(matchedElementList)>0:
						matchedElement = matchedElementList[0]
					else:

						# TODO handle error for content not found in the rocrate
						print(f"ContentNotFound: {datasetElem.guid}\tPath: {datasetElem.contentUrl}")
						continue
				
					# create metadata record to insert 
					objectSize = matchedElement.get('Size')
					objectPath = matchedElement.get('Key')
			
					# Update contentUrl for created dataset
					datasetElem.contentUrl = f"{baseUrl}/dataset/download/{datasetElem.guid}"
					
					# create distribution for metadata
					distribution = DatasetDistribution.model_validate({
							"distributionType": 'minio',
							"location": {"path": objectPath}
							})
						
					outputDataset = DatasetWriteModel.model_validate({
							**datasetElem.model_dump(by_alias=True),
							"permissions": permissionsSet, 
							"distribution": distribution,
							"size": objectSize,
							"isPartOf": {
									"@id": rocrateInstance.metadataGraph[1].guid,
							},		
					})

					# format as identifier write document
					output_json = {
						"@id": outputDataset.guid,
						"@type": outputDataset.metadataType,
						"metadata":outputDataset.model_dump(by_alias=True, mode='json'),
						"permissions": permissionsSet.model_dump(mode='json', by_alias=True), 
						"distribution": distribution.model_dump(by_alias=True, mode='json'),
						}
		
			# insert all identifiers for datasets
			insertResult = self.config.identifierCollection.insert_one(
				output_json
			)

			# TODO: check insertResult for success
			if insertResult.inserted_id:
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

		# mint all metadata elements
		for metadataModel in rocrateInstance.metadataGraph:

			if metadataModel.metadataType == "https://w3id.org/EVI#Dataset" or metadataModel.guid == 'ro-crate-metadata.json':
				pass
			if isinstance(metadataModel.metadataType,list):
				if 'https://w3id.org/EVI#ROCrate' in metadataModel.metadataType:
					continue
			else:
				insertDocument = {
					"@id": metadataModel.guid,
					"@type": metadataModel.metadataType,
					"metadata": { 
						**metadataModel.model_dump(by_alias=True, mode='json'),
						"isPartOf": crateGUID
					},
					'permissions': userPermissions.model_dump(mode='json'),
				}
				
				insertResult = metadataCollection.insert_one(
					insertDocument
				)

				if not insertResult.inserted_id:
					raise Exception(f"Writing Identifier To Mongo Failed id: {insertDocument.get('@id')}")
				
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

			content = s3Response['Body'].read()
			roCrateJSON = json.loads(content)

			rocrateInstance = ROCrateV1_2.model_validate(roCrateJSON)
			return rocrateInstance


	def processROCrate(self, transactionGUID: str):
		# get the current rocrate upload job

		self.config.asyncCollection.update_one(
        	{"guid": transactionGUID},
        	{"$set": 
            	{
                	"stage": "processing metadata"
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

		rocrateContents = self.getROCrateContentsMinio(
			zippedCratePath
		)

		try:
			roCrateModel = self.processTaskGetMetadata(rocrateContents)
		except Exception as e:
			print(f"ValidationError: {str(e)}")
			import traceback
			traceback.print_exc()
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

		# write dataset records
		datasetGUIDS = self.processTaskWriteDatasets(
			foundUser, 
			roCrateModel, 
			rocrateContents
		)
		
		self.config.asyncCollection.update_one(
			{"guid": transactionGUID},
			{"$set": 
				{
					"stage": "publishing metadata",
					"identifiersMinted": datasetGUIDS
				}
			}
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
				
		rocrate_metadata_elem_data = {
			"@id": metadataElem.guid,
			"@type": metadataElem.metadataType,
			"owner": foundUser.email,                               
			"permissions": foundUser.getPermissions().model_dump(mode='json', by_alias=True) ,              
			"published": True,                                      
			"metadata": {                                          
				**metadataElem.model_dump(by_alias=True, exclude={'@id', '@type'}), 
				"distribution": roCrateDistribution.model_dump(mode='json', by_alias=True),                
				"hasPart": [{"@id": elem} for elem in nonDatasetGUIDS + datasetGUIDS] ,
    			"permissions": foundUser.getPermissions().model_dump(mode='json', by_alias=True) ,              
				"published": True,   
			}
		}
	
		# dump into identifier collection and rocrate collection
		self.config.identifierCollection.insert_one(rocrate_metadata_elem_data  )

		# write the whole ROCrateV1_2 model into the rocrate collection
		rocrate_doc_for_collection = {
			"@id": metadataElem.guid,
			"@type": ['Dataset', "https://w3id.org/EVI#ROCrate"], 
			"owner": foundUser.email,
   			"permissions": foundUser.getPermissions().model_dump(mode='json', by_alias=True) ,                              
			"metadata": roCrateModel.model_dump(mode='json', by_alias=True) 
		}
		self.config.rocrateCollection.insert_one(rocrate_doc_for_collection)
  
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


	def getROCrateMetadata(self, rocrateGUID: str):
		rocrateMetadata = self.config.rocrateCollection.find_one({
				"$or":[
        			{"@id": rocrateGUID},
         			{"@id":f"{rocrateGUID}/"}]
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
			return FairscapeResponse(
					success=True,
					model=rocrateMetadata,
					statusCode=200
			)


	def getROCrateMetadataElem(self, rocrateGUID: str):
		rocrateMetadata = self.config.identifierCollection.find_one({
				"@id": rocrateGUID
		})
		
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

		rocrateMetadata = self.config.identifierCollection.find_one(			{"$or": [
				{"@id": rocrateGUID},
				{"@id": f"{rocrateGUID}/"}
			]},)['metadata']

		# if no metadata is found return 404
		if not rocrateMetadata:
				return FairscapeResponse(
						success=False,
						statusCode=404,
						error={"message": "rocrate not found"}
				)

		# TODO handle metadata failures
		rocrateInstance = ROCrateMetadataElemWrite.model_validate(rocrateMetadata)


		if not checkPermissions(rocrateInstance.permissions, requestingUser):
			return FairscapeResponse(
				success=False,
				statusCode=401,
				error={"message": "user unauthorized to download rocrate archive"}
			)

		# get the object from s3
		# TODO handle key missing error
		objectResponse = self.config.minioClient.get_object(
				Bucket=self.config.minioBucket,
				Key=rocrateInstance.distribution.location.path
		)

		# create a FairscapeResponse with a fileResponse item
		return FairscapeResponse(
				success=True,
				statusCode=200,
				fileResponse=objectResponse.get('Body'),
				model=rocrateInstance
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
		
			# 1. Store the full RO-Crate dump in rocrateCollection
			try:
				full_crate_dump = crateModel.model_dump(by_alias=True, mode='json')
				rocrate_doc_for_rocrate_collection = {
					"@id": root_guid,
					"@type": ['Dataset', "https://w3id.org/EVI#ROCrate"],
					"owner": requestingUser.email,
					"metadata": full_crate_dump,
				}
				self.config.rocrateCollection.insert_one(rocrate_doc_for_rocrate_collection)
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
					element_document_data = {
						"@id": elem.guid,
						"@type": elem.metadataType, 
						"owner": requestingUser.email, 
						"permissions": user_permissions,
						"metadata": elem.model_dump(by_alias=True, mode='json'),
						"distribution": None, 
					}

					documents_for_identifier_collection.append(element_document_data)
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
		""" Mark ROCrate as 
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

############
# Resolver #
############   


	

class FairscapeResolverRequest(FairscapeRequest):

	def resolveIdentifier(self, guid: str):	
		foundMetadata = self.config.identifierCollection.find_one(
			{"$or": [
				{"@id": guid},
				{"@id": f"{guid}/"}
			]},
			projection={"_id": False}
		)	

		if not foundMetadata:
			return FairscapeResponse(
				success=False,
				statusCode=404,
				error= {"message": "identifier not found"}
			)

		# identifierCases = {
		# 	"https://w3id.org/EVI#Dataset": Dataset,
		# 	"https://w3id.org/EVI#Computation": Computation,
		# 	"https://w3id.org/EVI#Software": Software,
		# 	"https://w3id.org/EVI#Schema": Schema,
		# }		

		# # TODO handle ROCrate for 
		# if isinstance(foundMetadata.get("@type"), str):
		# 	foundModel = identifierCases[foundMetadata.get("@type")].model_validate(foundMetadata)
		
		# else:
		# 	foundModel = ROCrateMetadataElemWrite.model_validate(foundMetadata)

		return FairscapeResponse(
			success=True,
			statusCode=200,
			model=foundMetadata
		)

#############
# Software  #
#############

class SoftwareWriteModel(Software):
	permissions: Permissions
	published: Optional[bool] = Field(default=True)


class SoftwareUpdateModel(BaseModel):
	name: Optional[str]
	description: Optional[str]


class FairscapeSoftwareRequest(FairscapeRequest):

	def createSoftware(
		self, 
		requestingUser: UserWriteModel,		
		softwareInstance: Software
	):

		writeModel = SoftwareWriteModel.model_validate({
			**softwareInstance.model_dump(by_alias=True, mode='json'),
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

	def getSoftware(self, guid: str):
		return getMetadata(self.identifierCollection, Software, guid)

	def deleteSoftware(
		self,		
		requestingUser: UserWriteModel, 
		guid: str
	):

		return deleteIdentifier(
			self.identifierCollection,
			requestingUser,
			SoftwareWriteModel,
			guid
		)

	def updateSoftware(self):
		pass

###############
# Computation #
###############

class ComputationWriteModel(Computation):
	permissions: Permissions
	published: Optional[bool] = Field(default=True)


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

###########
# Schema  #
###########

class SchemaWriteModel(Schema):
	permissions: Permissions
	published: bool = Field(default=True)


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


#########################
# Transfer to repository#
#########################

def extractFileFromZip(s3_client, bucket_name, file_path):
    """
    Extracts file from zip using partial reads to minimize data transfer
    """
    zip_path = str(file_path).split('.zip/')[0] + '.zip'
    internal_path = str(file_path).split('.zip/')[1]
    
    zip_size = s3_client.head_object(Bucket=bucket_name, Key=zip_path)['ContentLength']
    
    end_of_central_dir = getRangeFromS3(s3_client, bucket_name, zip_path, zip_size - 22, 22)
    
    if end_of_central_dir[:4] != b'PK\x05\x06':
        end_of_central_dir = findEndofCentralDir(s3_client, bucket_name, zip_path, zip_size)
    
    central_dir_size = struct.unpack('<L', end_of_central_dir[12:16])[0]
    central_dir_offset = struct.unpack('<L', end_of_central_dir[16:20])[0]
    
    central_directory = getRangeFromS3(s3_client, bucket_name, zip_path, central_dir_offset, central_dir_size)
    
    file_info = findFileInDir(central_directory, internal_path)
    if not file_info:
        raise FileNotFoundError(f"File {internal_path} not found in zip")
    
    local_header = getRangeFromS3(s3_client, bucket_name, zip_path, file_info['offset'], 30)
    
    filename_len = struct.unpack('<H', local_header[26:28])[0]
    extra_len = struct.unpack('<H', local_header[28:30])[0]
    
    data_offset = file_info['offset'] + 30 + filename_len + extra_len
    
    return getRangeFromS3(s3_client, bucket_name, zip_path, data_offset, file_info['compressed_size'])

def getRangeFromS3(s3_client, bucket_name, key, start, length):
    """
    Gets specific byte range from S3 object
    """
    response = s3_client.get_object(
        Bucket=bucket_name,
        Key=key,
        Range=f'bytes={start}-{start + length - 1}'
    )
    return response['Body'].read()

def findEndofCentralDir(s3_client, bucket_name, key, zip_size):
    """
    Searches for end of central directory record
    """
    search_size = min(65557, zip_size)
    data = getRangeFromS3(s3_client, bucket_name, key, zip_size - search_size, search_size)
    
    for i in range(len(data) - 4, -1, -1):
        if data[i:i+4] == b'PK\x05\x06':
            return data[i:i+22]
    
    raise ValueError("End of central directory not found")

def findFileInDir(central_dir, target_filename):
    """
    Finds file entry in central directory
    """
    offset = 0
    while offset < len(central_dir):
        if central_dir[offset:offset+4] != b'PK\x01\x02':
            break
        
        compressed_size = struct.unpack('<L', central_dir[offset+20:offset+24])[0]
        filename_len = struct.unpack('<H', central_dir[offset+28:offset+30])[0]
        extra_len = struct.unpack('<H', central_dir[offset+30:offset+32])[0]
        comment_len = struct.unpack('<H', central_dir[offset+32:offset+34])[0]
        local_header_offset = struct.unpack('<L', central_dir[offset+42:offset+46])[0]
        
        filename = central_dir[offset+46:offset+46+filename_len].decode('utf-8')
        
        if filename == target_filename:
            return {
                'compressed_size': compressed_size,
                'offset': local_header_offset
            }
        
        offset += 46 + filename_len + extra_len + comment_len
    
    return None

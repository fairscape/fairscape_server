import fastapi
from pydantic import (
    BaseModel,
    Field
)
from typing import (
    Optional,
    List,
    Literal
)
import datetime
import jwt
import pymongo
from pymongo.collection import Collection

from fairscape_models.computation import Computation
from fairscape_models.software import Software
from fairscape_models.schema import Schema
from fairscape_models.dataset import Dataset





class FairscapeResponse():
	def __init__(
		self, 
		success: bool, 
		statusCode: int, 
		model=None, 
		fileResponse=None, 
		error: dict=None
	):
		self.model = model
		self.success = success
		self.statusCode = statusCode
		self.error = error
		self.fileResponse = fileResponse


class FairscapeRequest():
	def __init__(
			self, 
			minioClient, 
			minioBucket, 
			identifierCollection, 
			userCollection, 
			asyncCollection,
			rocrateCollection=None,
			jwtSecret: str = None,
	):
		self.minioClient=minioClient
		self.minioBucket=minioBucket
		self.minioDefaultPath="fairscape"
		self.identifierCollection=identifierCollection
		self.userCollection=userCollection
		self.rocrateCollection=rocrateCollection
		self.asyncCollection=asyncCollection
		self.jwtSecret = jwtSecret
  
	def getMetadata(self, guid: str):
		return self.identifierCollection.find_one({"@id": guid})



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


def createUser(
	userCollection: Collection, 
	userInstance: UserCreateModel
	)->pymongo.results.InsertOneResult:
	userWriteInstance = UserWriteModel.model_validate({**userInstance.model_dump()})
	
	insertResult = userCollection.insert_one(
			userWriteInstance.model_dump(by_alias=True)
	)

	return insertResult


class FairscapeUserRequest(FairscapeRequest):

	def loginUser(self, userEmail: str, userPassword: str):
		""" Get a user record, create a session for the 
		"""
	
		foundUser = self.userCollection.find_one({
				"email": userEmail,
				"password": userPassword
		})

		if foundUser is None:
				return None

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
				self.jwtSecret, 
				algorithm="HS256"
		)
		
		# set session in userCollection
		updateTokenResult = self.userCollection.update_one({
				"email": userEmail,
				"password": userPassword
				},
				{
				"$set": {"session": compactJWS}
				}
		)

		print(updateTokenResult)

		#TODO check that update is correct

		return compactJWS


	def getUserBySession(self, session: str):

		tokenMetadata = jwt.decode(
			jwt=session,
			key=self.jwtSecret,
			algorithms=["HS256"]
		)

		userEmail = tokenMetadata.get('sub')

		foundUser = self.userCollection.find_one({
			"email": userEmail
		})

		if foundUser:
				return UserWriteModel.model_validate(foundUser)
		else:
				return None

####################################
# Helper Functions for Identifiers #
####################################

def deleteIdentifier(
	idCollection, 
	requestingUser: UserWriteModel, 
	modelClass, 
	guid: str
	):
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
	basePath: str = None
	):
	if basePath is None:
		contentName = pathlib.Path(datasetFilename).name
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
			"distributionType": DistributionTypeEnum.Minio,
					"location": {"path": minioKey}
			})

	return distribution


class FairscapeDatasetRequest(FairscapeRequest): 
	def getDatasetMetadata(
			self,
			datasetGUID: str
	):
		foundMetadata = self.getMetadata(datasetGUID)
		if foundMetadata is None:
			raise Exception
		else:
			return DatasetWriteModel.model_validate({**foundMetadata})

	def getDatasetContent(
		self, 
		userInstance: UserWriteModel, 
		datasetGUID: str,
	):
		datasetInstance = self.getDatasetMetadata(datasetGUID)

		# check that datasetInstance has minio distribtuion
		if datasetInstance.distribution.distributionType != DistributionTypeEnum.MINIO:
			raise Exception

		# get the distribution location from metadata
		objectKey = datasetInstance.distributionType.location.path

		response = self.minioClient.get_object(
			Bucket=self.minioBucket,
			Key=objectKey
		)

		return FairscapeResponse(
			success=True,
			statusCode=200,
			fileResponse=response,
			model=datasetInstance
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
			
			# process dataset
			if 'http' in inputDataset.contentUrl:
				distribution = DatasetDistribution.model_validate({
						"distributionType": "url",
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
				basePath= self.minioDefaultPath
			)
			
			# upload content and return a dataset distribution
			distribution = uploadObjectMinio(self.minioClient, self.minioBucket, uploadKey, datasetContent)

		# set remainder of metadata for storage
		permissionsSet = userInstance.getPermissions()

		outputDataset = DatasetWriteModel.model_validate({
			**inputDataset.model_dump(by_alias=True),
			"permissions": permissionsSet, 
			"distribution": distribution,
			"published": True
			})

		# insert identifier metadata into mongo
		insertResult = self.identifierCollection.insert_one(
			outputDataset.model_dump(by_alias=True)
		)

		
		# add identifier to users'dataset
		updateResult = self.userCollection.update_one(
				{"email": userInstance.email}, 
				{"$push": {"identifiers": inputDataset.guid}}
		)

		# TODO return fairscape response
		return outputDataset

	def deleteDataset(
		self,		
		requestingUser: UserWriteModel, 
		guid: str
	):
		return deleteIdentifier(
			self.identifierCollection,
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
	timeStarted: Optional[datetime.datetime] = Field(default_factory=datetime.datetime.now)
	timeFinished: Optional[datetime.datetime] = Field(default=None)
	completed: Optional[bool] = Field(default=False)
	error: Optional[str] = Field(default=None)
	identifiersMinted: Optional[int] = Field(default=None)
	rocrateIdentifier: Optional[str] = Field(default=None)


# ROCrate Helper Functions

def getROCrateMetadata(s3Client, bucketName, uploadInstance):
	zippedCratePath = pathlib.Path(uploadInstance.uploadPath)
	zippedCrateMetadataPath = zippedCratePath / zippedCratePath.stem / 'ro-crate-metadata.json'
	
	# get the values for the zipped metadata
	try:
		s3Response = s3Client.get_object(
				Bucket=bucketName,
				Key=str(zippedCrateMetadataPath)
		)

		# get the metadata out of the s3 response
		content= s3Response['Body'].read()
		roCrateJSON = json.loads(content)
		return roCrateJSON
	
	except s3Client.exceptions.NoSuchKey as err:
		# TODO handle error
		print("No Key Found")
		print(err)

def userPath(inputEmail):
	searchResults = re.search("(^[a-zA-Z0-9_.+-]+)@", inputEmail) 

	if searchResults is None:
			raise Exception
	else:
			return searchResults.group(1)


# set remainder of metadata for storage
def writeDatasets(
	identifierCollection, 
	userInstance: UserWriteModel, 
	rocrateInstance, 
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
	
	datasetWriteList = []

	# TODO: set to userInstance for method
	permissionsSet = userInstance.getPermissions()

	for datasetElem in rocrateInstance.getDatasets():
		# TODO: handle when remote content is included in ROCrate
		#if 'http' in datasetElem.contentUrl:
		#    continue
				
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
			continue
	
		# create metadata record to insert 
		objectSize = matchedElement.get('Size')
		objectPath = matchedElement.get('Key').lstrip(self.minioDefaultBucket + '/')
			
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
						"@id": crateGUID,
				},
				
		})

		# insert all identifiers for datasets
		insertResult = identifierCollection.insert_one(
				outputDataset.model_dump(by_alias=True, mode='json')
		)

		# TODO: check insertResult for success

		# append guid to dataset list
		datasetWriteList.append(outputDataset.guid)
	
	return datasetWriteList

def writeMetadataElements(
	identifierCollection,
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
	
	# mint software and computation and biochem entity
	rocrateMetadataElements = rocrateInstance.getSoftware() + rocrateInstance.getComputations() + rocrateInstance.getSchemas() + \
			rocrateInstance.getBioChemEntities() + rocrateInstance.getMedicalConditions()

	userPermissions = userInstance.getPermissions()
	crateGUID = rocrateInstance.getCrateMetadata().guid
	
	# written identifiers
	guidList = []

	# mint all metadata elements
	for metadataModel in rocrateMetadataElements:
		insertDocument = {
			**metadataModel.model_dump(by_alias=True, mode='json'),
			'permissions': userPermissions.model_dump(mode='json'),
			'isPartOf': {"@id": crateGUID}
		}
		
		insertResult = identifierCollection.insert_one(
			insertDocument
		)

		# TODO check insertResult
		
		guidList.append(metadataModel.guid)
			

	return guidList


# helper function to get all contents inside a zipped crate when there are more than 1k objects
def getROCrateContentsMinio(s3Client, bucketName, zipCratePath: str):
	# list entire subdirectory for rocrate upload
	listObjects = s3Client.list_objects_v2(
			Bucket= bucketName,
			Prefix= str(zipCratePath) + '/'
	)

	objectList = listObjects['Contents']

	isTruncated = listObjects.get('IsTruncated')
	nextContinueToken = listObjects.get('NextContinuationToken')

	while isTruncated:

		listObjects = s3Client.list_objects_v2(
			Bucket = bucketName,
			Prefix = str(zippedCratePath) + '/',
			ContinuationToken = nextContinueToken
		)
		nextContinueToken = listObjects.get('NextContinuationToken')
		isTruncated = listObjects.get('IsTruncated')
		objectList= objectList + listObjects['Contents']

	return objectList


class ROCrateMetadataElemWrite(ROCrateMetadataElem):
	permissions: Permissions
	published: Optional[bool] = Field(default=True)
	hasPart: Optional[List[dict]]
	distribution: Optional[DatasetDistribution]


class FairscapeROCrateRequest(FairscapeRequest):

	def __init__(
			self, 
			minioClient, 
			minioBucket, 
			identifierCollection, 
			userCollection, 
			asyncCollection,
			rocrateCollection=None,
	):
		self.minioClient=minioClient
		self.minioBucket=minioBucket
		self.minioDefaultPath="fairscape"
		self.identifierCollection=identifierCollection
		self.userCollection=userCollection
		self.rocrateCollection=rocrateCollection
		self.asyncCollection=asyncCollection

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
		
		uploadPath = f"{self.minioDefaultPath}/{userEmailPath}/rocrates/{rocrateFilename}"

		# upload zip to minio
		# TODO switch with fastapi.UploadFile
		with rocrate.file.open('rb') as zippedFileObj:
			uploadOperationResult = self.minioClient.upload_fileobj(
					Bucket = self.minioBucket,
					Key = str(uploadPath),
					Fileobj = zippedFileObj,
					ExtraArgs = {'ContentType': 'application/zip'}
			)

		transactionGUID = uuid.uuid4()
		
		uploadRequestInstance = ROCrateUploadRequest.model_validate({
			"guid": str(transactionGUID),
			"permissions": userInstance.getPermissions(),
			"uploadPath": str(uploadPath)
		})
		# create record in the async collection
		insertResult = self.asyncCollection.insert_one(
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


	def processROCrate(self, transactionGUID: str):
		# get the current rocrate upload job
		uploadMetadata = self.asyncCollection.find_one({"guid": transactionGUID})

		if uploadMetadata is None:
				raise Exception

		uploadInstance = ROCrateUploadRequest.model_validate(uploadMetadata)
		
		# find the user uploading the ROCrate to set permissions
		userMetadata = self.userCollection.find_one(
				{"email": uploadInstance.permissions.owner }
		)

		foundUser = UserWriteModel.model_validate(userMetadata)

		# TODO getROCrateMetadata
		roCrateMetadata = getROCrateMetadata(self.minioClient, self.minioBucket, uploadInstance)
		
		# parse the metadata into the rocrate
		try:
				roCrateModel = ROCrateV1_2.model_validate(roCrateJSON)
		
		except ValidationError as validationErr:
				# TODO return an error
				print("ValidationError")
				return None

		# get rocrate GUID
		crateMetadata = roCrateModel.getCrateMetadata()

		# get list of the objects from minio
		objectList = getROCrateContentsMinio(s3, minioDefaultBucket, zippedCratePath)

		# write dataset records
		datasetGUIDS = writeDatasets(
				self.identifierCollection, 
				foundUser, 
				roCrateModel, 
				objectList
		)

		# write metadata elements
		nonDatasetGUIDS = writeMetadataElements(
				self.identifierCollection,
				foundUser,
				roCrateModel
		)


		# write the metadata elem as an identifier to identifierCollection and ROCrate
		metadataElem = roCrateModel.getCrateMetadata()
		
		roCrateDistribution = DatasetDistribution.model_validate({
				"distributionType": 'minio',
						"location": {"path": uploadInstance.uploadPath}
						})
				
		rocrateMetadataElem = ROCrateMetadataElemWrite.model_validate({
				**metadataElem.model_dump(by_alias=True),
				"permissions": permissionsSet, 
				"distribution": roCrateDistribution,
				"hasPart": [{"@id": elem} for elem in nonDatasetGUIDS + datasetGUIDS],
				})

		rocrateMetadataWrite = rocrateMetadataElem.model_dump(by_alias=True, mode='json')
		
		# dump into identifier collection and rocrate collection
		self.identifierCollection.insert_one(rocrateMetadataWrite)
		self.rocrateCollection.insert_one(rocrateMetadataWrite)
		
		# update process as success
		updateResult = self.asyncCollection.update_one(
				{"guid": uploadInstance.guid},
				{"$set": {
						"completed": True,
						"identifiersMinted": len(datasetGUIDS + nonDatasetGUIDS)+1,
						"rocrateIdentifier": metadataElem.guid,
						"timeFinished": datetime.datetime.now()
				}}
		)

		#TODO: check update result
		return datasetGUIDS + nonDatasetGUIDS


	def getUploadMetadata(self, requestingUser: UserWriteModel, transactionGUID: str):
		# get upload metadata
		uploadMetadata = self.asyncCollection.find_one({
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
		rocrateMetadata = self.rocrateCollection.find_one({
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

		rocrateMetadata = self.identifierCollection.find_one({
				"@id": rocrateGUID
		})

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
		objectResponse = self.minioClient.get_object(
				Bucket=self.minioBucket,
				Key=rocrateInstance.distribution.location.path
		)

		# create a FairscapeResponse with a fileResponse item
		return FairscapeResponse(
				success=True,
				statusCode=200,
				fileResponse=objectResponse.get('Body')
		)

	def deleteROCrate(
		self,				 
		requestingUser: UserWriteModel, 
		guid: str
	):

		return deleteIdentifier(
			self.identifierCollection,
			requestingUser,
			ROCrateMetadataElemWrite,
			guid
		)

############
# Resolver #
############   

def getMetadata(mongoCollection, passedModel, guid: str):

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

	

class FairscapeResolverRequest(FairscapeRequest):

	def resolveIdentifier(self, guid: str):	

		foundMetadata = self.identifierCollection.find_one(
			{"@id": guid}
		)	

		if not foundMetadata:
			return FairscapeResponse(
				success=False,
				statusCode=404,
				error= {"message": "identifier not found"}
			)

		identifierCases = {
			"https://w3id.org/EVI#Dataset": Dataset,
			"https://w3id.org/EVI#Computation": Computation,
			"https://w3id.org/EVI#Software": Software,
			"https://w3id.org/EVI#Schema": Schema,
		}		

		# TODO handle ROCrate for 
		if isinstance(foundMetadata.get("@type"), str):
			foundModel = identifierCases[foundMetadata.get("@type")].model_validate(foundMetadata)
		
		else:
			foundModel = ROCrateMetadataElemWrite.model_validate(foundMetadata)

		return FairscapeResponse(
			success=True,
			statusCode=200,
			model=foundModel
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
		pass

	def getSchema(self):
		pass

	def updateSchema(self):
		pass

	def deleteSchema(self):
		pass


#########################
# Transfer to repository#
#########################
from fairscape_mds.crud.fairscape_request import FairscapeRequest
from fairscape_mds.crud.fairscape_response import FairscapeResponse
from fairscape_mds.crud.identifier import deleteIdentifier
from fairscape_mds.models.user import UserWriteModel, Permissions, checkPermissions
from fairscape_mds.models.dataset import (
	DatasetWriteModel, 
	DatasetDistribution, 
	DistributionTypeEnum
)
from fairscape_models.dataset import Dataset
from typing import Optional
from fastapi import UploadFile
import pathlib


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
		datasetContent: Optional[UploadFile]=None
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
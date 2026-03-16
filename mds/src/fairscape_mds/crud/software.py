from fairscape_mds.crud.fairscape_request import FairscapeRequest
from fairscape_mds.crud.fairscape_response import FairscapeResponse

from fairscape_mds.crud.identifier import getMetadata

from fairscape_mds.models.user import UserWriteModel, Permissions, checkPermissions
from fairscape_mds.models.software import SoftwareWriteModel
from fairscape_mds.models.identifier import StoredIdentifier
from fairscape_mds.models.dataset import DistributionTypeEnum
from fairscape_models.software import Software

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

		insertResult = self.config.identifierCollection.insert_one(
			writeModel.model_dump(by_alias=True, mode='json')
		)

		return FairscapeResponse(
			success=True,
			statusCode=201,
			model=writeModel
		)

	def getSoftware(self, guid: str):
		return getMetadata(self.config.identifierCollection, Software, guid)

	def getSoftwareContent(
		self,
		userInstance: UserWriteModel,
		softwareGUID: str,
	) -> FairscapeResponse:
		softwareMetadata = self.config.identifierCollection.find_one(
			{"@id": softwareGUID},
			projection={"_id": False}
		)

		if not softwareMetadata:
			return FairscapeResponse(
				success=False,
				statusCode=404,
				jsonResponse={"error": "Software not found"}
			)

		storedSoftware = StoredIdentifier.model_validate(softwareMetadata)

		if not checkPermissions(storedSoftware.permissions, userInstance):
			return FairscapeResponse(
				success=False,
				statusCode=401,
				jsonResponse={"error": "user unauthorized"}
			)

		if storedSoftware.distribution:
			if storedSoftware.distribution.distributionType == DistributionTypeEnum.MINIO:
				objectKey = storedSoftware.distribution.location.path

				response = self.config.minioClient.get_object(
					Bucket=self.config.minioBucket,
					Key=objectKey
				)

				return FairscapeResponse(
					success=True,
					statusCode=200,
					fileResponse=response,
					model=storedSoftware
				)
			else:
				return FairscapeResponse(
					success=False,
					statusCode=400,
					jsonResponse={"error": "Software Not Stored Locally"}
				)

		else:
			return FairscapeResponse(
				success=False,
				statusCode=400,
				jsonResponse={"error": "Software Not Stored Locally"}
			)

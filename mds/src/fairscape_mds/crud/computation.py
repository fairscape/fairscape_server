from fairscape_mds.crud.fairscape_request import FairscapeRequest
from fairscape_mds.crud.fairscape_response import FairscapeResponse
from fairscape_mds.crud.identifier import getMetadata, getStoredIdentifier

from fairscape_mds.models.computation import ComputationWriteModel
from fairscape_mds.models.user import UserWriteModel
from fairscape_mds.models.identifier import (
	StoredIdentifier,
	PublicationStatusEnum,
	MetadataTypeEnum
)
from fairscape_models.computation import Computation
import datetime
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError

class FairscapeComputationRequest(FairscapeRequest):

	def reasonEntailments(self, computationInstance: Computation):
		""" Function to look into mongo and reason EVI Properties
		"""
		if computationInstance.usedSoftware:
			# query for usedSoftware
			softwareUpdate = {
				"$push": {
					"usedByComputation": {
						"@id": computationInstance.guid
						}
					}
			}

			usedSoftwareUpdates = [ UpdateOne({"@id": sw.guid}, softwareUpdate) for sw in computationInstance.usedSoftware]
		else:
			usedSoftwareUpdates = []

		# query for usedDataset
		if computationInstance.usedDataset:
			updateUsedBy = {
				"$push": {
					"usedByComputation": {
						"@id": computationInstance.guid
						}
					}
				}
			
			usedDatasetUpdates = [ UpdateOne({"@id": ds.guid}, updateUsedBy) for ds in computationInstance.usedDataset]
		else:
			usedDatasetUpdates = []


		# query for generated elements to update
		if computationInstance.generated:
			generatedByUpdate = {
				"$push": {
					"generatedBy": {
						"@id": computationInstance.guid
						}
					}
				}

			generatedUpdates = [ UpdateOne({"@id": ds.guid}, generatedByUpdate) for ds in computationInstance.generated ]
		else:
			generatedUpdates = []


		reasoningUpdates = usedSoftwareUpdates + usedDatasetUpdates + generatedUpdates

		try:
			self.config.identifierCollection.bulk_write(
				reasoningUpdates
			)
		except BulkWriteError as bwe:
			pass

		return True


	def createComputation(
		self, 
		requestingUser: UserWriteModel,		
		computationInstance: Computation
	):

		# check if computation already exists
		identifierMetadata = self.config.identifierCollection.find_one(
			{"@id": computationInstance.guid},
			projection={"_id": False}
			)

		if identifierMetadata:
			return FairscapeResponse(
				success=False,
				statusCode=400,
				error={"error": "identifier already exists"}
			)

		createdDatetime = datetime.datetime.now(tz=datetime.timezone.utc)

		writeModel = StoredIdentifier.model_validate({
			"@id": computationInstance.guid,
			"@type": MetadataTypeEnum.COMPUTATION,
			"metadata": computationInstance.model_dump(by_alias=True, mode='json'),
			"permissions": requestingUser.getPermissions(),
			"publicationStatus": PublicationStatusEnum.DRAFT,
			"distribution": None,
			"dateCreated": createdDatetime,
			"dateModified": createdDatetime
		})

		insertResult = self.config.identifierCollection.insert_one(
			writeModel.model_dump(by_alias=True, mode='json')
		)

		if not insertResult.inserted_id:
			return FairscapeResponse(
				success=False,
				statusCode=500,
				error={"error": "error writing identifier"}
			)

		return FairscapeResponse(
			success=True,
			statusCode=201,
			model=writeModel
		)


	def getComputation(self, guid: str)->ComputationWriteModel:
		foundMetadata = self.getMetadata(guid)['metadata']
		if foundMetadata is None:
			raise Exception
		else:
			return ComputationWriteModel.model_validate({**foundMetadata})

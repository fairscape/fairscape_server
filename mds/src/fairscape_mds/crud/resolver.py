import logging
from fairscape_mds.crud.fairscape_request import FairscapeRequest
from fairscape_mds.crud.fairscape_response import FairscapeResponse
from fairscape_mds.models.identifier import StoredIdentifier
from fairscape_models.rocrate import GenericMetadataElem

logger = logging.getLogger(__name__)

class FairscapeResolverRequest(FairscapeRequest):

	def resolveIdentifier(self, guid: str):	
		foundMetadata = self.config.identifierCollection.find_one(
			{"@id": guid},
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
		try:
			foundMetadata = StoredIdentifier.model_validate(foundMetadata)
		except:
			foundMetadata = GenericMetadataElem.model_validate(foundMetadata)

		return FairscapeResponse(
			success=True,
			statusCode=200,
			model=foundMetadata
		)

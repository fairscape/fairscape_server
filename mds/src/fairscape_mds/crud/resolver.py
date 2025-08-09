from fairscape_mds.crud.fairscape_request import FairscapeRequest	
from fairscape_mds.crud.fairscape_response import FairscapeResponse

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

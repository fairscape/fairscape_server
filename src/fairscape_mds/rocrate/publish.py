import pymongo
from pydantic_core import ValidationError
from fairscape_mds.models.rocrate import (
	ROCrateV1_2,
	ROCrateMetadataElem,
)
from fairscape_mds.models.document import MongoDocument
from fairscape_mds.rocrate.errors import ROCrateException

class MintROCrateMetadataRequest():
	""" Class for Creating metadata only RO Crate records
	"""

	def __init__(self, 
		rocrateCollection: pymongo.collection.Collection, 
		identifierCollection: pymongo.collection.Collection, 
		crateModel: ROCrateV1_2,
		ownerCN: str,
		):

		self.rocrateCollection = rocrateCollection
		self.identifierCollection = identifierCollection
		self.crateModel = crateModel
		self.ownerCN = ownerCN


	def validateROCrateMetadata(self):
		datasets = self.crateModel.getDatasets()

		contentUrlDict = {
			crateDataset.guid: crateDataset.contentUrl for crateDataset in datasets
			if "file" in crateDataset.contentUrl
		}

		print(contentUrlDict)

		if len(contentUrlDict.items()) != 0:
			raise ROCrateException(
				message="All Identifiers must reference content by URI",
				errors = contentUrlDict
			)


	def writeIdentifiers(self):
		""" Write identifiers to mongo
		"""

		rocrateMetadataElem = self.crateModel.getCrateMetadata()

		# create a mongo document
		try:
			rocrateDocument = MongoDocument.model_validate({
				"@id": rocrateMetadataElem.guid,
				"@type": "https://w3id.org/EVI#ROCrate",
				"owner": self.ownerCN,
				"metadata": self.crateModel.model_dump(by_alias=True),
				"distribution": None
			})
		except ValidationError as e:
			raise ROCrateException("ROCrate Failed Validation", e)

		# publish rocrate 
		insertResult = self.rocrateCollection.insert_one(
			rocrateDocument.model_dump(by_alias=True)
			)

		# if rocrate metadata fails to write
		if insertResult.inserted_id is None:
			# TODO more detailed exception
			raise ROCrateException("Mongo Failed to Write")

		# insert identifier metadata for each of the elements
		#identifierList = [ rocrateMetadataElem ] + self.crateModel.getEVIElements()
		documentList = []
		for metadataElem in self.crateModel.metadataGraph:
			# TODO fix more elegantly
			isProject = metadataElem.metadataType == "Project"
			isOrg = metadataElem.metadataType == "Organization"
			isMetadataFile = metadataElem.guid == "ro-crate-metadata.json"

			if not isProject and not isOrg and not isMetadataFile:
				# skip
				#print(f"Proccessing {metadataElem.guid}\tType: {type(metadataElem)}")


				documentMetadata = {
					"@id": metadataElem.guid,
					"@type": metadataElem.metadataType,
					"owner": self.ownerCN,
					"metadata": metadataElem.model_dump(by_alias=True),
					"distribution": None
				}

				if isinstance(metadataElem, ROCrateMetadataElem):
					documentMetadata['@type'] = "https://w3id.org/EVI#ROCrate"
					
				try:
					metadataElemDocument = MongoDocument.model_validate(documentMetadata)
				except ValidationError as e:
					print(f"ERROR Validating: {metadataElem}")
					raise ROCrateException(f"ROCrate Element: {metadataElem.guid}", e)

				# add to list to insert into mongo
				documentList.append(
					metadataElemDocument.model_dump(by_alias=True)
				)

		# insert all documents into identifier collection
		insertResult = self.identifierCollection.insert_many(documents=documentList)

		if len(insertResult.inserted_ids) != len(documentList):
			raise ROCrateException("Error Minting ROCrates")

		# return identifier minted	
		return [ doc.get('@id') for doc in documentList]


	def publish(self):
		""" Preform all operations needed to write a metadata only ROCrate into Fairscape 
		"""

		self.crateModel.cleanIdentifiers()	
		self.validateROCrateMetadata()
		return self.writeIdentifiers()
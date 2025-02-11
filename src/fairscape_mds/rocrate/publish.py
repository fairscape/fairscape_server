import pymongo
from fairscape_mds.models.rocrate import (
	ROCrateV1_2,
	ROCrateMetadataElem,
)

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

		contentUrlDict = {
			crateDataset.guid: crateDataset.contentUrl for crateDataset in self.crateModel.getDatasets() 
			if "file" in crateDataset.contentUrl
		}

		if len(contentUrlDict.items()) != 0:
			raise ROCrateMetadataOnlyException(
				message="All Identifiers must reference content by URI",
				errors = contentUrlDict
			)


	def writeIdentifiers(self):
		""" Write identifiers to mongo
		"""

		rocrateMetadataElem = self.crateModel.getCrateMetadata()

		# create a mongo document
		rocrateDocument = MongoDocument.model_validate({
			"@id": rocrateMetadataElem.guid,
			"@type": "https://w3id.org/EVI#ROCrate",
			"owner": self.ownerCN,
			"metadata": self.crateModel,
			"distribution": None
		})

		# publish rocrate 
		insertResult = rocrateCollection.insert_one(
			rocrateDocument.model_dump(by_alias=True)
			)

		# if rocrate metadata fails to write
		if insertResult.inserted_id is None:
			# TODO more detailed exception
			raise Exception

		# insert identifier metadata for each of the elements
		identifierList = [ rocrateMetadataElem ] + crateModel.getEVIElements()
		documentList = []
		for metadataElem in identifierList:
			documentMetadata = {
				"@id": metadataElem.guid,
				"@type": metadataElem.metadataType,
				"owner": self.ownerCN,
				"metadata": metadataElem,
				"distribution": None
			}

			if isinstance(metadataElem, ROCrateMetadataElem):
				documentMetadata['@type'] = "https://w3id.org/EVI#ROCrate"
				

			metadataElemDocument = MongoDocument.model_validate(documentMetadata)

			# add to list to insert into mongo
			documentList.append(
				metadataElemDocument.model_dump(by_alias=True)
			)

		# insert all documents into identifier collection
		insertResult = identifierCollection.insert_many(documents=documentList)

		if len(insertResult.inserted_ids) != len(documentList):
			raise Exception

		# return identifier minted	
		return [ doc.guid for doc in documentList]


	def publish(self):
		""" Preform all operations needed to write a metadata only ROCrate into Fairscape 
		"""

		self.crateModel.cleanIdentifiers()	
		self.validateROCrateMetadata()
		return self.writeIdentifiers()
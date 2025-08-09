from fairscape_mds.crud.fairscape_request import FairscapeRequest
from fairscape_mds.crud.fairscape_response import FairscapeResponse
from fairscape_mds.models.user import UserWriteModel, checkPermissions


class IdentifierRequest(FairscapeRequest):
	def getMetadata(self):
		pass

	def updatePermissions(self):
		pass

	def updateMetadata(self):
		pass

	def deleteIdentifier(self):
		pass


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

from fairscape_mds.core.config import appConfig
from fairscape_mds.models.user import UserWriteModel
from fairscape_mds.models.identifier import StoredIdentifier
from fairscape_mds.crud.identifier import (
	IdentifierRequest, 
	MetadataTypeEnum,
	PublicationStatusEnum
)
from fairscape_mds.tests.crud.utils import load_test_data
from fairscape_models.dataset import Dataset
import pytest
import datetime


userInstance = UserWriteModel.model_validate({
			"email": "test@example.org",
			"firstName": "John",
			"lastName": "Doe",
			"password": "test"
			})


# create an identifier
datasetMetadata = load_test_data('dataset_content.json')
datasetInstance = Dataset.model_validate(datasetMetadata)

appConfig.identifierCollection.delete_many({		
	"@id": datasetInstance.guid,
})

identifier = StoredIdentifier.model_validate({
	"@id": datasetInstance.guid,
	"@type": MetadataTypeEnum.DATASET,
	"metadata": datasetInstance.model_dump(
		by_alias=True, 
		mode='json'
		),
	"permissions": userInstance.getPermissions(),
	"distribution": None,
	"publicationStatus": PublicationStatusEnum.DRAFT,
	"dateCreated": datetime.datetime.now(),
	"dateModified": datetime.datetime.now()
})

appConfig.identifierCollection.insert_one(
	identifier.model_dump(
		by_alias=True, 
		mode='json'
		)
)	

@pytest.fixture(scope="module")
def current_user():
	return UserWriteModel.model_validate({
			"email": "test@example.org",
			"firstName": "John",
			"lastName": "Doe",
			"password": "test"
			})

@pytest.fixture(scope="module")
def identifier_request():
	return IdentifierRequest(appConfig)

def test_update_identifier(
		identifier_request, 
		current_user
	):

	datasetInstance.name = "new name"

	response = identifier_request.updateMetadata(
		guid = datasetInstance.guid,
		user= current_user,
		newMetadata = datasetInstance
	)

	assert response.success
	assert response.statusCode == 200


#def test_delete_identifier():
#	pass


#def test_publication_status():
#	pass

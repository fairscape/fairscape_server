from fairscape_mds.crud.fairscape_request import FairscapeRequest
from fairscape_mds.crud.fairscape_response import FairscapeResponse
from fairscape_mds.core.config import appConfig
from fairscape_mds.tests.crud.utils import load_test_data
import pytest
from fairscape_mds.models.user import UserWriteModel
from fairscape_mds.crud.dataset import FairscapeDatasetRequest
from fairscape_models.dataset import Dataset
import fastapi


@pytest.fixture(scope="module")
def current_user():
	return UserWriteModel.model_validate({
			"email": "test@example.org",
			"firstName": "John",
			"lastName": "Doe",
			"password": "test"
			})

@pytest.fixture(scope="module")
def dataset_request():
	return FairscapeDatasetRequest(appConfig)


def test_0_create_dataset_metadata_only(dataset_request, current_user):

	singleDataset = load_test_data("single_dataset.json")
	assert singleDataset

	validatedDataset = Dataset.model_validate(singleDataset)

	datasetCreateResponse = dataset_request.createDataset(
		current_user,
		validatedDataset
	)

	assert datasetCreateResponse.success
	assert datasetCreateResponse.statusCode == 201
	assert datasetCreateResponse.model


def test_1_create_dataset_with_file(dataset_request, current_user):
	datasetMetadata = load_test_data("single_content.json")
	datasetFile = "data/example.csv"

	with open(datasetFile, "rb") as datafile:

		inputFile = fastapi.UploadFile(
			file=datafile
		)

		createResponse = dataset_request.createDataset(
			userInstance=current_user,
			inputDataset=datasetMetadata,
			datasetContent=inputFile
		)

	assert createResponse.success
	assert createResponse.statusCode == 201
	assert createResponse.model


def test_2_resolve_dataset(dataset_request, current_user):
	pass


def test_3_download_content(dataset_request, current_user):
	pass

def test_4_change_publication_status(dataset_request, current_user):
	pass


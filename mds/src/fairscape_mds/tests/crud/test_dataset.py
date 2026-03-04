import pytest
import pathlib
import fastapi
from fairscape_mds.crud.dataset import FairscapeDatasetRequest
from fairscape_mds.core.config import appConfig
from fairscape_models.dataset import Dataset
from fairscape_mds.tests.crud.utils import load_test_data

DATA_DIR = pathlib.Path(__file__).parent / "data"


@pytest.fixture(scope="module")
def dataset_request():
    return FairscapeDatasetRequest(appConfig)


def test_0_create_dataset_metadata_only(dataset_request, current_user):
    single_dataset = load_test_data("single_dataset.json")
    assert single_dataset

    validated = Dataset.model_validate(single_dataset)
    response = dataset_request.createDataset(current_user, validated)

    assert response.success
    assert response.statusCode == 201
    assert response.model


def test_1_create_dataset_with_file(dataset_request, current_user):
    dataset_metadata = load_test_data("single_content.json")

    with open(DATA_DIR / "example.csv", "rb") as datafile:
        input_file = fastapi.UploadFile(file=datafile)
        response = dataset_request.createDataset(
            userInstance=current_user,
            inputDataset=dataset_metadata,
            datasetContent=input_file,
        )

    assert response.success
    assert response.statusCode == 201
    assert response.model


def test_2_resolve_dataset(dataset_request, current_user):
    pass


def test_3_download_content(dataset_request, current_user):
    pass


def test_4_change_publication_status(dataset_request, current_user):
    pass

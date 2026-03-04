import pytest
from fairscape_mds.crud.computation import FairscapeComputationRequest
from fairscape_mds.core.config import appConfig
from fairscape_models.computation import Computation
from fairscape_mds.tests.crud.utils import load_test_data


@pytest.fixture(scope="module", autouse=True)
def clean_collection():
    appConfig.identifierCollection.delete_many({})


@pytest.fixture(scope="module")
def computation_request():
    return FairscapeComputationRequest(appConfig)


def test_computation_0_create(computation_request, current_user):
    single_computation = load_test_data("single_computation.json")
    assert single_computation

    validated = Computation.model_validate(single_computation)
    response = computation_request.createComputation(current_user, validated)

    assert response.success
    assert response.statusCode == 201
    assert response.model


def test_computation_1_create_duplicate(computation_request, current_user):
    single_computation = load_test_data("single_computation.json")
    assert single_computation

    validated = Computation.model_validate(single_computation)
    response = computation_request.createComputation(current_user, validated)

    assert not response.success
    assert response.statusCode == 400
    assert response.error


def test_computation_reasoning():
    pass

from fairscape_mds.crud.computation import FairscapeComputationRequest
from fairscape_mds.crud.fairscape_request import FairscapeRequest
from fairscape_mds.crud.fairscape_response import FairscapeResponse
from fairscape_mds.core.config import appConfig
from fairscape_mds.models.user import UserWriteModel
from fairscape_models.computation import Computation
import json
import pytest
import os

appConfig.identifierCollection.delete_many({})

def load_test_data(filename):
	filepath = os.path.join(os.path.dirname(__file__), "data", filename)
	with open(filepath, 'r') as f:
		return json.load(f)

@pytest.fixture(scope="module")
def computation_request():
	return FairscapeComputationRequest(appConfig)

@pytest.fixture(scope="module")
def current_user():
	return UserWriteModel.model_validate({
			"email": "test@example.org",
			"firstName": "John",
			"lastName": "Doe",
			"password": "test"
			})


def test_computation_0_create(computation_request, current_user):
	singleComputation = load_test_data("single_computation.json")
	assert singleComputation

	validatedComputation = Computation.model_validate(singleComputation)

	computation_response = computation_request.createComputation(
		current_user,
		validatedComputation
	)

	assert computation_response.success
	assert computation_response.statusCode == 201
	assert computation_response.model 


def test_computation_1_create_duplicate(computation_request, current_user):
	singleComputation = load_test_data("single_computation.json")
	assert singleComputation

	validatedComputation = Computation.model_validate(singleComputation)

	computation_response = computation_request.createComputation(
		current_user,
		validatedComputation
	)

	assert not computation_response.success
	assert computation_response.statusCode == 400
	assert computation_response.error 


def test_computation_reasoning():
	pass
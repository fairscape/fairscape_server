import pytest
from fairscape_mds.models.user import UserWriteModel


@pytest.fixture(scope="module")
def current_user():
    return UserWriteModel.model_validate({
        "email": "test@example.org",
        "firstName": "John",
        "lastName": "Doe",
        "password": "test"
    })

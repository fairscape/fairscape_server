from fastapi.testclient import TestClient
from fairscape_mds.main import app
from fairscape_mds.backend.backend import *

testApp = TestClient(app)


def test_live():
	response = testApp.get("/status")
	assert response.status_code == 200

def test_login_user():
    # create a test user for logging in
    testUser = UserWriteModel.model_validate({
        "email": "test@example.org",
        "firstName": "John",
        "lastName": "Doe",
        "password": "test"
        }) 
 
    # create a test user for the 
    insertUserResult = config.userCollection.insert_one(
        test_user.model_dump(by_alias=True, mode='json')
    )

    assert insertUserResult.inserted_id is not None

    # login the user
    loginData = {
        "username": testUser.email,
        "password": testUser.password
        }

    loginResponse = testApp.post("/login", data=loginData)
    loginJSON = loginResponse.json()

	assert loginResponse.status_code == 200
    assert loginJSON 
    assert loginJSON.get("access_token")

    # upload a test rocrate



def test_upload_rocrate():
    pass

def test_download_rocrate():
    pass

import os
import sys

import pytest
from fastapi.testclient import TestClient
from fairscape_mds.main import app
from fairscape_mds.backend.backend import *
from fairscape_mds.backend.models import UserWriteModel

testApp = TestClient(app)

testUser = UserWriteModel.model_validate({
    "email": "test@example.org",
    "firstName": "John",
    "lastName": "Doe",
    "password": "test"
    }) 
 
# create a test user for the 
insertUserResult = userCollection.insert_one(
    testUser.model_dump(by_alias=True, mode='json')
)

class UserFixture:
    def __init__(self, username, password):
        self.username = username
        self.password = password

class HeadersFixture:
    def __init__(self, headers):
        self.headers = headers


@pytest.fixture
def get_user():
    # create a test user for logging in
    return UserFixture(testUser.email, testUser.password)



def test_live():
    response = testApp.get("/status")
    assert response.status_code == 200

def test_login_user(get_user):

    # login the user
    loginData = {
        "username": get_user.username,
        "password": get_user.password
        }

    loginResponse = testApp.post("/login", data=loginData)
    loginJSON = loginResponse.json()

    assert loginResponse.status_code == 200
    assert loginJSON 
    assert loginJSON.get("access_token")

    authHeaders = {
        "Authorization": f"Bearer {loginJSON.get("access_token")}"
        }



@pytest.fixture
def get_headers():

    loginResponse = testApp.post("/login", data=loginData)
    loginJSON = loginResponse.json()
    authHeaders = {
        "Authorization": f"Bearer {loginJSON.get("access_token")}"
        }
    return HeadersFixture(authHeaders)


def test_upload_rocrate(get_headers):
    rocrateFiles = {
         "crate": ("Example.zip", open("test/data/Example.zip", "rb"), "application/zip")
    }

    # upload a test rocrate
    uploadResponse = testApp.post(
         "/rocrate/upload",
         files=rocrateFiles,
         headers=get_headers.headers
    )

    assert uploadResponse.status_code > 200

    uploadResponseJSON = uploadResponse.json()

    submissionUUID = uploadResponseJSON.get("guid")

    # check status
    checkStatus = testApp.get(
         f"/rocrate/upload/status/{submissionUUID}",
         headers=get_headers.headers
    )

    assert checkStatus.status_code == 200
    
    statusJSON = checkStatus.json()
    assert statusJSON

    # if upload is complete

    # wait until complete
    pass

def test_download_rocrate():
    pass

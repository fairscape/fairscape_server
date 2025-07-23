import pytest
import httpx
import time
from fairscape_mds.backend.backend import *
from fairscape_mds.backend.models import UserWriteModel

root_url = "http://localhost:8080"

testUser = UserWriteModel.model_validate({
    "email": "test@example.org",
    "firstName": "John",
    "lastName": "Doe",
    "password": "test"
    }) 

userCollection.delete_many({})
 
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
    testUser = UserWriteModel.model_validate({
        "email": "test@example.org",
        "firstName": "John",
        "lastName": "Doe",
        "password": "test"
        }) 
    # create a test user for logging in
    return UserFixture(testUser.email, testUser.password)


def test_live():
    response = httpx.get(root_url + "/status")
    assert response.status_code == 200



def test_upload_rocrate(get_user):
    # login the user
    loginData = {
        "username": get_user.username,
        "password": get_user.password
        }


    loginResponse = httpx.post(
        root_url + "/login", 
        data=loginData
        )
    loginJSON = loginResponse.json()

    assert loginResponse.status_code == 200
    assert loginJSON 
    assert loginJSON.get("access_token")

    authHeaders = {
        "Authorization": f"Bearer {loginJSON.get('access_token')}"
        }


    rocrateFiles = {
         "crate": ("Example.zip", open("tests/data/Example.zip", "rb"), "application/zip")
    }
    
    # upload a test rocrate
    uploadResponse = httpx.post(
         root_url + "/rocrate/upload-async",
         files=rocrateFiles,
         headers=authHeaders
    )

    # check that upload response is successfull
    assert uploadResponse.status_code == 200
    uploadResponseJSON = uploadResponse.json()
    assert uploadResponseJSON

    submissionUUID = uploadResponseJSON.get("guid")
    assert submissionUUID


    def loopStatus():
        jobFinished = False
        loop = 0
        while not jobFinished and loop > 5:
            loop+=1
            # check status
            checkStatus = httpx.get(
                 root_url + f"/rocrate/upload/status/{submissionUUID}",
                 headers=authHeaders
            )

            assert checkStatus.status_code == 200 
            statusJSON = checkStatus.json()
            assert statusJSON

            print(statusJSON)


            # if time started and time finished is none job is still running
            if not statusJSON.get("timeFinished"):
                time.sleep(5)
            else:
                return statusJSON

    statusJSON = loopStatus()
    # check that job is marked as success
    assert statusJSON.get("completed")
    assert not statusJSON.get("error")

    # get the crate guid
    assert statusJSON.get("rocrateGUID")

            





def test_download_rocrate():
    pass

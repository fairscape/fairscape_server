import pytest
import httpx
import time
import logging
import zipfile
import pathlib
import pymongo
from urllib.parse import quote_plus

testLogger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
httpxLogger = logging.getLogger('httpx')
httpxLogger.setLevel(logging.WARNING)

#from fairscape_mds.core.config import *
from fairscape_mds.models.user import UserWriteModel
root_url = "http://localhost:8080"



def _set_up_tests():
    connection_string = f"mongodb://{quote_plus('mongotestaccess')}:{quote_plus('mongotestsecret')}@localhost:27017/?authSource=admin&retryWrites=true"
    mongoClient = pymongo.MongoClient(connection_string)

    identifierCollection = mongoClient.fairscape.mds
    userCollection = mongoClient.fairscape.users
    rocrateCollection = mongoClient.fairscape.rocrate
    asyncCollection = mongoClient.fairsacpe['async']

    userCollection.delete_many({})
    identifierCollection.delete_many({})
    rocrateCollection.delete_many({})
    asyncCollection.delete_many({})
    
    testUser = UserWriteModel.model_validate({
        "email": "test@example.org",
        "firstName": "John",
        "lastName": "Doe",
        "password": "test"
        })

    # create a test user for the ROCrate Upload Operation
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



def _test_upload_rocrate(get_user, caplog):
    
    caplog.set_level(logging.INFO, logger=__name__)

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

    testLogger.info('Login Success')

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

    testLogger.info('Upload Success')

    submissionUUID = uploadResponseJSON.get("guid")
    assert submissionUUID

    checkStatus = httpx.get(
         root_url + f"/rocrate/upload/status/{submissionUUID}",
         headers=authHeaders
    )
    assert checkStatus.status_code == 200
    statusJSON = checkStatus.json()
    assert statusJSON

    # check if job is complete
    wait = 0
    completed = statusJSON.get("completed", False)
    while not completed or wait>10:
        # check upload status in 5 seconds
        testLogger.info('Awaiting Job Completion')
        time.sleep(5)
        wait += 5

        checkStatus = httpx.get(
             root_url + f"/rocrate/upload/status/{submissionUUID}",
             headers=authHeaders
        )
        assert checkStatus.status_code == 200
        statusJSON = checkStatus.json()
        completed = statusJSON.get("completed", False)


    testLogger.info('Check Upload Status Success')

    #statusJSON = loopStatus()

    # check that job is marked as success
    assert statusJSON.get("completed")
    assert not statusJSON.get("error")

    # get the crate guid
    assert statusJSON.get("rocrateGUID")

    rocrateGUID = statusJSON.get("rocrateGUID")

    testLogger.info(f'Found ROCrate GUID: {rocrateGUID}')

    # print(rocrateGUID)
    # resolve ROCrate identifier
    resolveIdentifier = httpx.get(
        root_url + f"/rocrate/{rocrateGUID}"
    )

    assert resolveIdentifier.status_code == 200
    roCrateIdentifier = resolveIdentifier.json() 
    assert roCrateIdentifier

    rocrateMetadataGraph = roCrateIdentifier.get('metadata', {}).get('@graph')
    assert rocrateMetadataGraph

    # get every @id in the graph
    guidList = [ elem.get("@id") for elem in rocrateMetadataGraph] 
 

    # resolve every identifier inside the rocrate
    for guid in guidList:
        if guid != "ro-crate-metadata.json":
            resolveGUID = httpx.get(
                root_url + f"/{guid}"
            )

            assert resolveGUID.status_code == 200

            identifierMetadata = resolveGUID.json()

            testLogger.info(f"Resolved GUID: {guid}")

            assert identifierMetadata

        else:
            pass
        # assert each identifier is minted with @id, metadata, permissions


    # download the rocrate
    downloadCrateResponse = httpx.get(
        root_url + f"/rocrate/download/{rocrateGUID}",
        headers=authHeaders
        )

    assert downloadCrateResponse.status_code == 200

    # download the rocrate to tmp
    with open("/tmp/Example.zip", "wb") as crateFile:
        crateFile.write(downloadCrateResponse.content)

    # check that the file is downloaded correctly
    with zipfile.ZipFile("/tmp/Example.zip", "r") as crateZip:
        crateZip.extractall("/tmp/ExampleCrate")


    # check that all files are available 
    exampleCrate = pathlib.Path("/tmp/ExampleCrate/Example")

    datafile = exampleCrate / "Demo PreMo_export_2025-05-06.csv"
    metadatafile = exampleCrate / "ro-crate-metadata.json"

    # check that Demo PreMo_export is there
    assert datafile.exists()
    assert metadatafile.exists()

    # download the premo data export
    
    datasetGUID = "ark:59852/dataset-example-data export-55c2016c"


    with httpx.stream("GET", root_url + f"/dataset/download/{datasetGUID}", headers=authHeaders) as response:
        response.raise_for_status()
        with open('/tmp/dataset.csv', 'wb') as downloadFile:
            for chunk in response.iter_bytes():
                downloadFile.write(chunk) 


def test_publication_status_dataset(get_user, caplog):
    
    caplog.set_level(logging.INFO, logger=__name__)

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
    authHeaders = {
        "Authorization": f"Bearer {loginJSON.get('access_token')}"
        }

    # change publication status
    datasetGUID = "ark:59852/dataset-example-data export-55c2016c"

    requestBody = {
        "@id": datasetGUID,
        "publicationStatus": "PUBLISHED"
    }

    updateResponse = httpx.put(
        root_url + "/publish",
        json=requestBody,
        headers=authHeaders
        )

    assert updateResponse.status_code==200
    updateResponseJSON = updateResponse.json()
    assert updateResponseJSON

    assert updateResponseJSON.get("@id")==datasetGUID
    assert updateResponseJSON.get("publicationStatus") == "PUBLISHED"


    with httpx.stream("GET", root_url + f"/download/{datasetGUID}") as response:
        response.raise_for_status()
        with open('/tmp/download.csv', 'wb') as downloadFile:
            for chunk in response.iter_bytes():
                downloadFile.write(chunk) 

    # check that 



def test_publication_status_rocrate():
    pass

def loopStatus(submissionUUID):
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




def test_download_rocrate():
    pass

import pytest
import httpx
import time
import logging
import zipfile
import pathlib
import pymongo
import hashlib
from urllib.parse import quote_plus
from fairscape_mds.models.user import UserWriteModel
import boto3 
from botocore.client import Config

testLogger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
httpxLogger = logging.getLogger('httpx')
httpxLogger.setLevel(logging.WARNING)

#from fairscape_mds.core.config import *
root_url = "http://localhost:8080/api"

# remove test 
object_key = "default/test/rocrates/Example.zip"

# create a boto s3 client
s3 = boto3.client('s3',
        endpoint_url="http://localhost:9000",
        aws_access_key_id= "miniotestadmin",
        aws_secret_access_key= "miniotestsecret",
        config=Config(signature_version='s3v4'),
        aws_session_token=None,
        region_name='us-east-1'
    )

try:
    s3.create_bucket(Bucket="default")
except:
    pass

try:
    s3.delete_object(
        Bucket="default",
        Key=object_key
    )
except:
    pass


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


def calculate_sha256_file_hash(filepath):
    """
    Calculates the SHA256 hash of a given file.

    Args:
        filepath (str): The path to the file.

    Returns:
        str: The hexadecimal SHA256 hash of the file, or None if an error occurs.
    """
    sha256_hash = hashlib.sha256()
    try:
        with open(filepath, "rb") as f:
            # Read the file in chunks to handle large files efficiently
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    except FileNotFoundError:
        print(f"Error: File not found at {filepath}")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None


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



def test_upload_rocrate(get_user, caplog):

    rocratePath = "tests/data/Example.zip"
    rocrateName = pathlib.Path(rocratePath).name
    rocrateSHA256= calculate_sha256_file_hash(rocratePath)

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
         "crate": (rocrateName, open(rocratePath, "rb"), "application/zip")
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
    resolveUrl = root_url + f"/{rocrateGUID}"
    resolveIdentifier = httpx.get(
        resolveUrl,
        headers=authHeaders
    )

    #assert resolveIdentifier.status_code == 200
    roCrateIdentifier = resolveIdentifier.json() 
    testLogger.info(f'Resolve URL: {resolveUrl}')
    testLogger.info(f'ResolveResponse: {roCrateIdentifier}')
    assert roCrateIdentifier

    testLogger.info(f'Resolved ROCrate GUID: {rocrateGUID}')

    rocrateMetadataGraph = roCrateIdentifier.get('metadata', {}).get('hasPart')
    assert rocrateMetadataGraph

    # get every @id in the graph
    guidList = [ elem.get("@id") for elem in rocrateMetadataGraph] 
 

    # resolve every identifier inside the rocrate
    for guid in guidList:
        if guid != "ro-crate-metadata.json":
            testLogger.info(f'Resolving ROCrate Member GUID: {guid}')

            resolveGUID = httpx.get(
                root_url + f"/{guid}",
                headers=authHeaders
            )

            assert resolveGUID.status_code == 200

            identifierMetadata = resolveGUID.json()

            testLogger.info(f"Resolved GUID: {guid}")

            assert identifierMetadata

        else:
            pass
        # assert each identifier is minted with @id, metadata, permissions

    testLogger.info(f'Downloading ROCrate File: {rocrateGUID}')

    # download the rocrate
    downloadCrateResponse = httpx.get(
        root_url + f"/rocrate/download/{rocrateGUID}",
        headers=authHeaders
        )

    assert downloadCrateResponse.status_code == 200

    #downloadedCrateSHA256 = calculate_sha256_file_hash("/tmp/Example.zip")
    #assert downloadedCrateSHA256 == rocrateSHA256

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

    testLogger.info(f'Downloading Dataset: {datasetGUID}')

    with httpx.stream("GET", root_url + f"/dataset/download/{datasetGUID}", headers=authHeaders) as response:
        response.raise_for_status()
        with open('/tmp/dataset.csv', 'wb') as downloadFile:
            for chunk in response.iter_bytes():
                downloadFile.write(chunk) 

 
    # login the user
    loginData = {
        "username": "test@example.org",
        "password": "test"
        }


    loginResponse = httpx.post(
        root_url + "/login", 
        data=loginData
        )

    assert loginResponse.status_code == 200
    loginJSON = loginResponse.json()

    assert loginJSON 
    assert loginJSON.get("access_token")

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

    # check download file exists 
    assert pathlib.Path('/tmp/download.csv').exists()

    # change publication status back to DRAFT
    requestBody = {
        "@id": datasetGUID,
        "publicationStatus": "DRAFT"
    }

    updateResponse = httpx.put(
        root_url + "/publish",
        json=requestBody,
        headers=authHeaders
        )

    rocrateGUID = "ark:59852/dataset-example-data export-55c2016c"
    requestBody = {
        "@id": rocrateGUID,
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

    assert updateResponseJSON.get("@id")==rocrateGUID
    assert updateResponseJSON.get("publicationStatus") == "PUBLISHED"

    rocrateDownloadPath = '/tmp/rocrate_download.zip'

    # download rocrate from download endpoint
    with httpx.stream("GET", root_url + f"/download/{rocrateGUID}") as response:
        response.raise_for_status()
        with open(rocrateDownloadPath, 'wb') as downloadFile:
            for chunk in response.iter_bytes():
                downloadFile.write(chunk) 

    datasetDownloadPath = '/tmp/dataset_download.csv'

    # check that dataset can be downloaded
    with httpx.stream("GET", root_url + f"/download/{datasetGUID}") as response:
        response.raise_for_status()
        with open(datasetDownloadPath, 'wb') as downloadFile:
            for chunk in response.iter_bytes():
                downloadFile.write(chunk) 

    assert pathlib.Path(datasetDownloadPath).exists()

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

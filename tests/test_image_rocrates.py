import pytest
import httpx
import logging
import pymongo
import pathlib
import time
from fairscape_mds.models.user import UserWriteModel
from urllib.parse import quote_plus

root_url = "http://localhost:8080"

testLogger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
httpxLogger = logging.getLogger('httpx')
httpxLogger.setLevel(logging.WARNING)

def setup_tests():
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
	userCollection.insert_one(
			testUser.model_dump(by_alias=True, mode='json')
	)

testROCratePaths = [
	"/mnt/e/Work/Data/Uploads/upload_06_29_2025/paclitaxel.zip",
	"/mnt/e/Work/Data/Uploads/upload_06_29_2025/untreated.zip",
	"/mnt/e/Work/Data/Uploads/upload_06_29_2025/vorinostat.zip",
]

@pytest.mark.parametrize("rocratePath", testROCratePaths)
def test_upload(rocratePath, caplog):

	setup_tests()
	caplog.set_level(logging.INFO, logger=__name__)

	rocrateName = pathlib.Path(rocratePath).name

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
	testLogger.info("Success: Logged In User")	

	
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

	submissionUUID = uploadResponseJSON.get("guid")
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
	while not completed or wait>600:
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




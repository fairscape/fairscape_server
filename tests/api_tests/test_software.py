import httpx

# root url
root_url = "http://localhost:8080/api/"

# login the user
loginData = {
		"username": "jdoe@example.org",
		"password": "examplepassword"
		}


loginResponse = httpx.post(
		root_url + "login", 
		data=loginData
		)

assert loginResponse.status_code == 200
loginJSON = loginResponse.json()

assert loginJSON 
assert loginJSON.get("access_token")


authHeaders = {
		"Authorization": f"Bearer {loginJSON.get('access_token')}"
		}


softwareMetadata = {
	"@id": "ark:59853/test", # issue with identifier
	"@type": ["prov:Entity", "https://w3id.org/EVI#Software"],
	"name": "example repository",
	"description": "Example software for reproduction",
	"author": "user@example.org",
	"version": "abc123",
	"contentUrl": "http://example.com/repo.git",
	"format": "git",
	"codeRepository": "http://example.com/repo.git",
	"branch": "main",
	"commit": "abc123"
}


def test_software_0_create_software():

	uploadSoftwareResponse = httpx.post(
				root_url + "software",
				json=softwareMetadata,
				headers=authHeaders,
				timeout=600
	)

	assert uploadSoftwareResponse.status_code == 200
	assert uploadSoftwareResponse.content
	
	createdIdentifier = uploadSoftwareResponse.json()

	assert createdIdentifier["@id"] == softwareMetadata["@id"]

	# check that all uploaded metadata is present in the stored identifier	
	for key, value in softwareMetadata.items():
		assert createdIdentifier['metadata'][key] == value



def test_software_1_update_software():
	# update the software
	softwareMetadata["name"] = "updated repository"

	updateSoftwareResponse = httpx.put(
		root_url + softwareMetadata["@id"],
		json=softwareMetadata,
		headers=authHeaders,
		timeout=600
	)

	assert updateSoftwareResponse.status_code == 200
	updatedIdentifier = updateSoftwareResponse.json()
	assert updatedIdentifier['metadata']['name'] == softwareMetadata['name']



def test_software_2_publish_software():
	# update the software
	requestBody = {
		"@id": softwareMetadata["@id"],
		"publicationStatus": "PUBLISHED"
	}

	publishResponse = httpx.put(
		root_url + "publish",
		json=requestBody,
		headers=authHeaders,
		timeout=600
	)

	assert publishResponse.status_code == 200

	# get the identifier metadata without auth headers
	getPublishedResponse = httpx.get(
		root_url + softwareMetadata["@id"],
		timeout=60
	)

	assert getPublishedResponse.status_code == 200


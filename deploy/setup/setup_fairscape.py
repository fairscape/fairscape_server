import time
import minio
import ldap3
from dotenv import dotenv_values
from os import environ
import logging 
import pathlib
import requests
import docker
import time


# setup logger
ldapSetupLogger = logging.getLogger('ldapSetup')
seedDataLogger = logging.getLogger('seedData')

# environment variables
configValues = {
	**dotenv_values(dotenv_path="./setup.env"),
	**environ
} 
fairscapeRootURL = configValues['FAIRSCAPE_API_URL']
userEmail = configValues['USER_EMAIL']
userPassword = configValues['USER_PASSWORD']
ldapServerURI = f"ldap://{configValues['FAIRSCAPE_LDAP_HOST']}:{configValues['FAIRSCAPE_LDAP_PORT']}"
ldapBaseDN = configValues['FAIRSCAPE_LDAP_BASE_DN']
ldapUsersDN = configValues['FAIRSCAPE_LDAP_USERS_DN']
ldapGroupsDN = configValues['FAIRSCAPE_LDAP_GROUPS_DN']
configAdminDN = configValues['FAIRSCAPE_LDAP_CONFIG_ADMIN_DN']
configAdminPassword = configValues['FAIRSCAPE_LDAP_CONFIG_ADMIN_PASSWORD']
adminDN = configValues['FAIRSCAPE_LDAP_ADMIN_DN']
adminPassword = configValues['FAIRSCAPE_LDAP_ADMIN_PASSWORD']


class FairscapeRequest():

	def __init__(self, username: str, password: str, rootURL: str):
		self.username = username
		self.password = password
		self.rootURL = rootURL
		self.tokenString = None

	def getTokenString(self):
		"""
		Login a User to Fairscape
		"""
		loginResponse = requests.post(
			self.rootURL + 'login', 
			data={
				"username": self.username, 
				"password": self.password
				})
		loginResponseBody = loginResponse.json()
		self.tokenString = loginResponseBody['access_token']

	def uploadCrate(self, cratePath: pathlib.Path):
		fileName = cratePath.name
		crateUpload = {
			'crate': (fileName, open(str(cratePath), 'rb'), 'application/zip')
			}
    
		uploadResponse = requests.post(
        self.rootURL + 'rocrate/upload-async',
        files=crateUpload,
        headers={"Authorization": f"Bearer {self.tokenString}"}
    )
		return uploadResponse

	def checkUploadStatus(self, requestID: str) -> dict:
		uploadStatusResponse = requests.get(
        self.rootURL + 'rocrate/upload/status/' + requestID,
        headers={
					"Authorization": f"Bearer {self.tokenString}"
					}
    )
		return uploadStatusResponse.json()

def waitForSetup():
	client = docker.from_env()


def uploadCrates():
	configValues = {
		**dotenv_values(dotenv_path="./setup.env"),
		**environ
	} 

	# poll for fairscape api to be live
	apiLive = False
	loops = 0

	while not apiLive and loops<5:
		try:
			r = requests.get(fairscapeRootURL + 'docs')
			if r.status_code == 200:
				apiLive = True
		except:
			print("polling fairscape-api")
			time.sleep(5)
			loops += 1

	fairscapeAPI = FairscapeRequest(
		username=userEmail,
		password=userPassword,
		rootURL=fairscapeRootURL
	)

	# get a fairscape token
	fairscapeAPI.getTokenString()

	# list all crates and upload
	uploadedCrates = pathlib.Path('/data/rocrates')
	allUploadedCrates = list(pathlib.Path(uploadedCrates).glob('*.zip'))
	transactionIDs = []

	for crate in allUploadedCrates:
		uploadResponse = fairscapeAPI.uploadCrate(crate)

		# check for success
		# assert uploadResponse.status_code == 201
		print(f"upload success: {uploadResponse.status_code == 201} status_code: {uploadResponse.status_code}")

		uploadJSON = uploadResponse.json()
		transactionIDs.append(uploadJSON.get("transactionFolder"))

	# TODO check all request guids are finished

	for transaction in transactionIDs:
		uploadStatus = fairscapeAPI.checkUploadStatus(transaction)
		
		if uploadStatus.get("completed"):	
				
			if uploadStatus.get("success"):
				# TODO log success
				seedDataLogger.info(f"Upload Success: {transaction}")

			else:
				# TODO log failure
				seedDataLogger.error(f"Upload Failure: {transaction}")	

	# TODO: check that downloads are functional for ROcrates

	# TODO: check that access controls apply

	# TODO: check that api endpoints work

	pass

	

# helper utilities



class LDAPConnectionError(Exception):
	def __init__(self,exception: Exception | None=None, message: str="failed to connect to ldap"):

		self.message = message
		self.exception = exception
		super().__init__(self.message)


def connectLDAP(userDN, userPassword):
	try:
		server = ldap3.Server(ldapServerURI, get_info=ldap3.ALL)
		connection = ldap3.Connection(server,          
			user=userDN, 
			password=userPassword,
			lazy=False
			) 
		bind_response = connection.bind()
		if bind_response:
			ldapSetupLogger.info(f"msg: 'connected to LDAP server'\tserver: '{ldapServerURI}'\tuserDN: '{userDN}'")
		return connection
	
	except ldap3.core.exceptions.LDAPBindError as bindError:
		ldapSetupLogger.error(f"msg: 'ldap bind error'\tserver: '{ldapServerURI}'\tuserDN: '{userDN}'")
		raise LDAPConnectionError(
			exception=bindError, 
			message="ldap bind error"
			) 

	except ldap3.core.exceptions.LDAPSocketOpenError as connError:
		ldapSetupLogger.error(f"msg: 'ldap failed to open connection'\tserver: '{ldapServerURI}'\tuserDN: '{userDN}'")
		raise LDAPConnectionError(
			exception=connError, 
			message=f"ldap connection error: failed to open connection at {ldapServerURI}"
			) 

	except ldap3.core.exceptions.LDAPException as ldapException:
		ldapSetupLogger.error(f"msg: 'ldap exception'\tserver: '{ldapServerURI}'\tuserDN: '{userDN}'")
		raise LDAPConnectionError(
			exception=ldapException, 
			message=f"ldap error: {str(ldapException)}"
			)


def setupOverlay():
    # connect as the config admin
    configAdminConnection =connectLDAP(
        userDN=configAdminDN,
        userPassword=configAdminPassword
        )

    # add the module list to the config database 
    addModuleList = configAdminConnection.add(
        dn="cn=module,cn=config",
        attributes={
            "objectClass": "olcModuleList",
            "olcModuleLoad": [ "memberof.so", "refint.so"],
            "olcModulePath": "/opt/bitnami/openldap/lib/openldap"
        }
    )

    if not addModuleList:
        ldapSetupLogger.error(msg="msg: failed to add module list to config database")
        raise Exception("Setup Failed")

    configAdminConnection.rebind()

    try:
        ldap3.ObjectDef(['olcMemberOf'], configAdminConnection)

    except ldap3.core.exceptions.LDAPSchemaError as schemaError:
        ldapSetupLogger.error("failed to find olcMemberOf schema")
        raise Exception("Setup Failed")


    memberOfOverlayDN="olcOverlay={0}memberof,olcDatabase={2}mdb,cn=config"
    memberOfOverlayAttributes={
        "objectClass": [ "olcOverlayConfig", "olcMemberOf"],
        "olcOverlay": "memberof",
        "olcMemberOfRefInt": "TRUE",
    }

    addOverlay = configAdminConnection.add(
        dn=memberOfOverlayDN,
        attributes=memberOfOverlayAttributes,
    )


    configAdminConnection.rebind()

    if not addOverlay:
        overlayAppliedResult = configAdminConnection.search(
            search_base="olcDatabase={2}mdb,cn=config",
            search_scope=ldap3.SUBTREE,
            search_filter="(objectClass=olcOverlayConfig)"
        )

        if not overlayAppliedResult:
            ldapSetupLogger.error("failed to find applied memberOf overlay")
            raise Exception("ldap setup failure")

# add user to fairscape
def addFairscapeUser(
    ldapConnection, 
    userCN: str, 
    userSN: str, 
    userGN: str, 
    userMail: str,
    userPassword: str,
):

    return ldapConnection.add(
        dn=f"cn={userCN},ou=users,{ldapBaseDN}",
        attributes={
            "objectClass": ["inetOrgPerson", "Person"],
            "cn": userCN,
            "sn": userSN,
            "gn": userGN,
            "mail": userMail,
            "userPassword": userPassword
        }    
    )


# iterate over user data
def loadUsers():
	""" read all users in user data file 
	"""
	with open("/data/ldap/user_data.csv", "r") as csvFile:

		# ignore the first line
		userData = csvFile.read()

	userList = []

	lines = userData.splitlines()
	for userLine in lines[1::]:
		userData = userLine.replace(" ", "").split(",")

		user = {
			"firstName": userData[0],
			"lastName": userData[1],
			"dn": userData[2],
			"email": userData[3],
			"password": userData[4]
		}

		userList.append(user)
	return userList


def loadGroups():
	""" load all groups from the local files
	"""
	with open("/data/ldap/group_data.csv", "r") as csvFile:
		groupData = csvFile.read()

	groupList = []

	lines = groupData.splitlines()
	for groupLine in lines[1::]:
		groupData = groupLine.replace(" ", "").split(",")

		group = {
			"dn": groupData[0],
			"members": [ f"cn={groupMember},ou=users,{ldapBaseDN}" for groupMember in groupData[1].split(";")],
		}

		groupList.append(group)
	return groupList


def createUsers(passedLDAPConnection, userList):
	''' add all users into the LDAP tree
	'''
	for userElem in userList[1::]:
		addSuccess = addFairscapeUser(
			ldapConnection=passedLDAPConnection, 
			userCN=userElem['dn'],
			userSN=userElem['lastName'],
			userGN=userElem['firstName'],
			userMail=userElem['email'],
			userPassword=userElem['password']
			)
		ldapSetupLogger.info(f"msg: added user\tsuccess: {addSuccess}\tuser: {userElem['dn']}")


def createGroups(ldapConnection, groupList):
	''' add all loaded groups to the LDAP tree
	'''

	for groupElem in groupList:
		addGroup = ldapConnection.add(
			dn=f"cn={groupElem['dn']},ou=groups,dc=fairscape,dc=net",
			attributes={
			"objectClass": "groupOfNames", 
				"cn": groupElem['dn'], 
				"member": groupElem['members']
			}
		)
		ldapSetupLogger.info(f"added group\tsuccess:{addGroup}\tgroup: {groupElem['dn']}")

def setupFairscapeUsers():

	# form admin connection to main database
	adminConnection = connectLDAP(
		adminDN,
		adminPassword
	)

	# add groups OU
	adminConnection.add(
		dn="ou=groups,dc=fairscape,dc=net",
		attributes={
			"objectClass": "organizationalUnit", 
			"ou": "groups", 
			"description": "organizational unit of fairscape groups"
			}
	) 

	userList= loadUsers()
	groupList= loadGroups()

	# create users
	createUsers(adminConnection, userList)
	createGroups(adminConnection, groupList)

def setupMinio():
	minioClient = minio.Minio(
		endpoint=f"{configValues['FAIRSCAPE_MINIO_URI']}:{configValues['FAIRSCAPE_MINIO_PORT']}",
		access_key=configValues['FAIRSCAPE_MINIO_ACCESS_KEY'],
		secret_key=configValues['FAIRSCAPE_MINIO_SECRET_KEY'],
		secure=False,
		cert_check=False
	)

	defaultBucketName = configValues['FAIRSCAPE_MINIO_DEFAULT_BUCKET']
	rocrateBucketName = configValues['FAIRSCAPE_MINIO_ROCRATE_BUCKET']

	if not minioClient.bucket_exists(defaultBucketName):
		minioClient.make_bucket(defaultBucketName)

	if not minioClient.bucket_exists(rocrateBucketName):
		minioClient.make_bucket(rocrateBucketName)


if __name__ =="__main__":
	time.sleep(10)
	try:
		setupOverlay()
	except:
		ldapSetupLogger.error("Error setting up overlay")

	try:
		setupFairscapeUsers()
	except:
		ldapSetupLogger.error("Error setting up users overlay")

	# initalize minio
	setupMinio()

	# seed data
	#uploadCrates()
# fast api routers
from fastapi import FastAPI, Depends, HTTPException, Path, UploadFile
from fastapi.security import OAuth2PasswordRequestForm, OAuth2PasswordBearer
from fastapi.responses import JSONResponse, StreamingResponse
from typing import Annotated

from fairscape_mds.backend.backend import *
from fairscape_mds.backend.models import *
from fairscape_mds.worker import processROCrate

from fairscape_models.dataset import Dataset
from fairscape_models.software import Software
from fairscape_models.schema import Schema
from fairscape_models.sample import Sample
from fairscape_models.computation import Computation
from fairscape_models.biochem_entity import BioChemEntity
from fairscape_models.medical_condition import MedicalCondition
from fairscape_models.rocrate import ROCrateV1_2

app = FastAPI()

OAuthScheme = OAuth2PasswordBearer(tokenUrl="token")

userRequest = FairscapeUserRequest(
  minioClient=s3,
  minioBucket=minioDefaultBucket,
	userCollection=userCollection,
	identifierCollection=identifierCollection,
	asyncCollection=asyncCollection,
  jwtSecret=jwtSecret
)
resolverRequest = FairscapeResolverRequest(
  minioClient=s3,
  minioBucket=minioDefaultBucket,
	userCollection=userCollection,
	identifierCollection=identifierCollection,
	asyncCollection=asyncCollection,
)
datasetRequest = FairscapeDatasetRequest(
	minioClient=s3,
	minioBucket=minioDefaultBucket,
	identifierCollection=identifierCollection,
	userCollection=userCollection,
	asyncCollection=asyncCollection
)
softwareRequest = FairscapeSoftwareRequest(
	minioClient=s3,
	minioBucket=minioDefaultBucket,
	identifierCollection=identifierCollection,
	userCollection=userCollection,
	asyncCollection=asyncCollection
)
computationRequest = FairscapeComputationRequest(
	minioClient=s3,
	minioBucket=minioDefaultBucket,
	identifierCollection=identifierCollection,
	userCollection=userCollection,
	asyncCollection=asyncCollection
)
rocrateRequest = FairscapeROCrateRequest(
	minioClient=s3,
	minioBucket=minioDefaultBucket,
#	minioDefaultPath="fairscape",
	identifierCollection=identifierCollection,
	userCollection=userCollection,
	rocrateCollection=rocrateCollection,
	asyncCollection=asyncCollection	
)

def getCurrentUser(
	token: Annotated[str, Depends(OAuthScheme)]
	):

	try:
		foundUser = userRequest.getUserBySession(token)
		return foundUser
	except Exception as e:
		raise HTTPException(
			status_code=401,
			detail=f"Authorization Error Decoding Token\terror: {str(e)}"
		)

@app.post("/login")
def form(form_data: Annotated[OAuth2PasswordRequestForm, Depends()]):

	token = userRequest.loginUser(form_data.username, form_data.password)

	if not token:
		return JSONResponse(
			status_code=401,
			content={"message": "unrecognized username password combination"}
		)
	
	return {
	"access_token": str(token), 
	"token_type": "bearer"
	}
          

@app.get("/admin")
def admin(
	currentUser: Annotated[UserWriteModel, Depends(getCurrentUser)]
	):

	print(currentUser)

	if not currentUser:
		return JSONResponse(
			status_code=401,
			content={"message": "unauthorized"}
		)
	else:
		return {"message": "secret handshake", "currentUserEmail": currentUser.email}


@app.post("/dataset")
def createDataset(
	currentUser: Annotated[UserWriteModel, Depends(getCurrentUser)],
	datasetMetadata: Dataset,
	#uploadFile: Optional[UploadFile]
):

	response = datasetRequest.createDataset(
		userInstance=currentUser,
		inputDataset=datasetMetadata,
		#datasetContent=uploadFile
	)

	return response


@app.get("/dataset/ark:{naan}/{postfix}")
def getDatasetMetadata(
	naan: str,
	postfix: str
):

	datasetGUID = f"ark:{naan}/{postfix}"
	return datasetRequest.getDatasetMetadata(datasetGUID)


@app.get("/dataset/ark:{naan}/{postfix}?download")
def getDatasetContent(
	naan: str,
	postfix: str,
	currentUser: Annotated[UserWriteModel, Depends(getCurrentUser)]
):

	datasetGUID = f"ark:{naan}/{postfix}"
	datasetResponse = datasetRequest.getDatasetContent(
		userInstance=currentUser,
		datasetGUID=datasetGUID
	)

	if datasetResponse.success:

		# TODO set the download file name as the filename download
		# TODO set the content type based on the metadata
		zipHeaders = {
			"Content-Type": "application/zip",
			"Content-Disposition": "attachment;filename=downloaded-rocrate.zip"
    }

		def iterfile():
			with open(datasetResponse.fileResponse['Body'], "r") as datasetFile:
				yield from datasetFile

		return StreamingResponse(
			iterfile(),
			headers=zipHeaders
		)

	else:
		return JSONResponse(
			status_code=datasetResponse.statusCode,
			content=datasetResponse.error
		)


@app.delete("/dataset/ark:{NAAN}/{postfix}")
def deleteDataset(
	NAAN: str,
	postfix: str,
	currentUser: Annotated[UserWriteModel, Depends(getCurrentUser)],
):

	datasetGUID = f"ark:{NAAN}/{postfix}"

	response = datasetRequest.deleteDataset(
		currentUser,
		datasetGUID
	)

	if response.success:
		return response.model

	else:
		return JSONResponse(
			status_code = response.statusCode,
			content = response.error
		)


@app.post("/rocrate")
def uploadROCrate(
	currentUser: Annotated[UserWriteModel, Depends(getCurrentUser)],
	rocrate: UploadFile
):

	uploadOperation = rocrateRequest.uploadROCrate(
		userInstance=currentUser,
		rocrate=rocrate
	)

	if uploadOperation.success:

		uploadJob = uploadOperation.model

		# start backend job
		processROCrate.apply_async(args=(uploadJob.guid))

		return uploadJob

	else:
		return JSONResponse(
			status_code=400,
			content={"error": uploadOperation.error}
		)


@app.get("/rocrate/upload/{submissionUUID}")
def getUploadStatus(
	currentUser: Annotated[UserWriteModel, Depends(getCurrentUser)],
	submissionUUID: str
):

	response = rocrateRequest.getUploadMetadata(
		currentUser,
		submissionUUID
	)

	if response.success:
		return response.model

	else:
		return JSONResponse(
			status_code = response.statusCode,
			content = response.error
		)


@app.get("/rocrate/download/ark:{NAAN}/{postfix}")
def getROCrateArchive(
	currentUser: Annotated[UserWriteModel, Depends(getCurrentUser)],
	NAAN: str,
	postfix: str
):

	rocrateGUID = f"ark:{NAAN}/{postfix}"

	response = rocrateRequest.downloadROCrateArchive(
		currentUser,
		rocrateGUID
	)
	
	if response.success:
		# TODO set the rocrate name as the filename download
		zipHeaders = {
			"Content-Type": "application/zip",
			"Content-Disposition": "attachment;filename=downloaded-rocrate.zip"
    }

		def iterfile():
			with open(response.fileResponse['Body'], "rb'") as rocrateFile:
				yield from rocrateFile

		return StreamingResponse(
			iterfile(),
			headers=zipHeaders
		)

	else:
		return JSONResponse(
			status_code = response.statusCode,
			content = response.error
		)


@app.get("/ark:{NAAN}/{postfix}")
def resolveARK(
	NAAN: str,
	postfix: str
):

	guid = f"ark:{NAAN}/{postfix}"

	response = resolverRequest.resolveIdentifier(guid)

	if response.success:
		return response.model

	else:
		return JSONResponse(
			status_code = response.statusCode,
			content = response.error
		)


@app.post("/software")
def createSoftware(
	currentUser: Annotated[UserWriteModel, Depends(getCurrentUser)],
	softwareInstance: Software
):
	response = softwareRequest.createSoftware(
		currentUser,
		softwareInstance
	)

	if response.success:
		return response.model

	else:
		return JSONResponse(
			status_code = response.statusCode,
			content = response.error
		)


@app.delete("/software/ark:{NAAN}/{postfix}")
def deleteSoftware(
	NAAN: str,
	postfix: str,
	currentUser: Annotated[UserWriteModel, Depends(getCurrentUser)],
):
	softwareGUID = f"ark:{NAAN}/{postfix}"

	response = softwareRequest.deleteSoftware(
		currentUser,
		softwareGUID
	)

	if response.success:
		return response.model

	else:
		return JSONResponse(
			status_code = response.statusCode,
			content = response.error
		)


@app.post("/computation")
def createComputation(
	currentUser: Annotated[UserWriteModel, Depends(getCurrentUser)],
	computationInstance: Computation 
):

	response = computationRequest.createComputation(
		currentUser,
		computationInstance
	)

	if response.success:
		return response.model

	else:
		return JSONResponse(
			status_code = response.statusCode,
			content = response.error
		)


@app.delete("/computation/ark:{NAAN}/{postfix}")
def deleteComputation(
	NAAN: str,
	postfix: str,
	currentUser: Annotated[UserWriteModel, Depends(getCurrentUser)],
):

	computationGUID = f"ark:{NAAN}/{postfix}"

	response = computationRequest.deleteComputation(
		currentUser,
		computationGUID
	)

	if response.success:
		return response.model

	else:
		return JSONResponse(
			status_code = response.statusCode,
			content = response.error
		)
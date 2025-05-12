# fast api routers
from fastapi import FastAPI, Depends, HTTPException, Path, UploadFile
from fastapi.security import OAuth2PasswordRequestForm, OAuth2PasswordBearer
from fastapi.responses import JSONResponse, StreamingResponse
from typing import Annotated

from fairscape_mds.backend.backend import *
from fairscape_mds.backend.models import *
from fairscape_mds.worker import processROCrate

from fairscape_models.dataset import Dataset
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

datasetRequest = FairscapeDatasetRequest(
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
		headers = {
			"Content-Type": "application/zip",
			"Content-Disposition": "attachment;filename=downloaded-rocrate.zip"
    }
  	#return StreamingResponse(
		#	, 
		#	headers=headers, 
		#	media_type="application/zip"
		#	)

		# return streaming response
		return None
		#return StreamingResponse()

	else:
		return JSONResponse(
			status_code=datasetResponse.statusCode,
			content=datasetResponse.error
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
		return response.fileResponse

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
	pass

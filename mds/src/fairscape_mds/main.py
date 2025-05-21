# fast api routers
from fastapi import FastAPI, Depends, HTTPException, Path, UploadFile, Form, File
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import HTTPException
from fastapi.security import OAuth2PasswordRequestForm, OAuth2PasswordBearer
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware 
from typing import Annotated

from pydantic import ValidationError

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


try:
    s3.create_bucket(Bucket=minioDefaultBucket)
except:
    pass


app = FastAPI(
	root_path="/api",
	title="Fairscape API",
	description="Backend Fairscape API for storing EVI Providence Graphs and rich provenance metadata"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"], 
    allow_headers=["*"],
)

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
schemaRequest = FairscapeSchemaRequest(
	minioClient=s3,
	minioBucket=minioDefaultBucket,
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
  
from fairscape_mds.backend.for_now.credentitals_router import router as credentials_router
app.include_router(credentials_router)
from fairscape_mds.backend.for_now.evidence_graph_router import router as evidence_graph_router
app.include_router(evidence_graph_router)

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


def parseDataset(datasetMetadata: str = Form(...)):
	try:
		return Dataset.model_validate_json(datasetMetadata)
	except ValidationError as e:
		raise HTTPException(
			detail=jsonable_encoder(e.errors()),
			status_code=422
		)

@app.post("/dataset")
def createDataset(
	currentUser: Annotated[UserWriteModel, Depends(getCurrentUser)],
	datasetMetadata: Annotated[Dataset, Depends(parseDataset)],
	uploadFile: Optional[UploadFile] = None
):

	response = datasetRequest.createDataset(
		userInstance=currentUser,
		inputDataset=datasetMetadata,
		datasetContent=uploadFile
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

@app.post("/rocrate/upload-async")
def uploadROCrate(
	currentUser: Annotated[UserWriteModel, Depends(getCurrentUser)],
	crate: UploadFile
):

	uploadOperation = rocrateRequest.uploadROCrate(
		userInstance=currentUser,
		rocrate=crate
	)

	if uploadOperation.success:

		uploadJob = uploadOperation.model

		# start backend job
		processROCrate.apply_async(args=(uploadJob.guid,))

		return uploadJob

	else:
		return JSONResponse(
			status_code=400,
			content={"error": uploadOperation.error}
		)
  
@app.post(
    "/rocrate/metadata",
    summary="Mint metadata-only ROCrate records without file content",
    status_code=201
)
def publishMetadataOnly(
    currentUser: Annotated[UserWriteModel, Depends(getCurrentUser)],
    crateMetadata: ROCrateV1_2
):
    try:
        # Call the mintMetadataOnlyROCrate method on the existing rocrateRequest
        result = rocrateRequest.mintMetadataOnlyROCrate(
            requestingUser=currentUser,
            crateModel=crateMetadata
        )
        
        if result.success:
            return JSONResponse(
                content=result.model,
                status_code=result.statusCode
            )
        else:
            return JSONResponse(
                content=result.error,
                status_code=result.statusCode
            )
    except Exception as e:
        return JSONResponse(
            content={
                "message": "Error minting metadata-only ROCrate identifiers",
                "error": str(e)
            },
            status_code=500
        )
        
@app.get(
    "/rocrate",
    summary="List all ROCrates accessible by the current user",
    response_description="A list of RO-Crates with their basic metadata"
)
def list_rocrates_endpoint(
    currentUser: Annotated[UserWriteModel, Depends(getCurrentUser)],
):
    fairscape_response = rocrateRequest.list_crates(requestingUser=currentUser)

    if fairscape_response.success:
        return JSONResponse(
            status_code=fairscape_response.statusCode,
            content=fairscape_response.model
        )
    else:
        raise HTTPException(
            status_code=fairscape_response.statusCode,
            detail=fairscape_response.error
        )


@app.get("/rocrate/upload/status/{submissionUUID}")
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

@app.get("/rocrate/ark:{NAAN}/{postfix}")
def getROCrateMetadata(
	NAAN: str,
	postfix: str
):
	guid = f"ark:{NAAN}/{postfix}"
	response = rocrateRequest.getROCrateMetadata(guid)

	if response.success:
		return response.model

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


@app.get("/software/ark:{NAAN}/{postfix}")
def getSoftware(
	NAAN: str,
	postfix: str
):
	softwareGUID = f"ark:{NAAN}/{postfix}"
	response = softwareRequest.getSoftware(softwareGUID)

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


@app.post("/schema")
def createSchema(
	currentUser: Annotated[UserWriteModel, Depends(getCurrentUser)],
	schemaInstance: Schema
):
	response = schemaRequest.createSchema(
		currentUser,
		schemaInstance
	)

	if response.success:
		return response.model

	else:
		return JSONResponse(
			status_code = response.statusCode,
			content = response.error
		)


@app.get("/schema/ark:{NAAN}/{postfix}")
def getSchema(
	NAAN: str,
	postfix: str
):
	schemaGUID = f"ark:{NAAN}/{postfix}"
	response = schemaRequest.getSchema(schemaGUID)

	if response.success:
		return response.model

	else:
		return JSONResponse(
			status_code = response.statusCode,
			content = response.error
		)


@app.delete("/schema/ark:{NAAN}/{postfix}")
def deleteSchema(
	NAAN: str,
	postfix: str,
	currentUser: Annotated[UserWriteModel, Depends(getCurrentUser)],
):
	schemaGUID = f"ark:{NAAN}/{postfix}"

	response = schemaRequest.deleteSchema(
		currentUser,
		schemaGUID
	)

	if response.success:
		return response.model

	else:
		return JSONResponse(
			status_code = response.statusCode,
			content = response.error
		)

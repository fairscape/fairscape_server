
from fastapi import (
	APIRouter, 
	Depends, 
	HTTPException, 
	Form, 
	UploadFile
)
from pydantic import ValidationError
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.encoders import jsonable_encoder
from typing import Optional, Annotated
import mimetypes

from fairscape_mds.models.user import UserWriteModel
from fairscape_mds.crud.dataset import FairscapeDatasetRequest
from fairscape_mds.core.config import appConfig
from fairscape_models.dataset import Dataset
from fairscape_mds.deps import getCurrentUser



datasetRequest = FairscapeDatasetRequest(appConfig)

datasetRouter = APIRouter(prefix="", tags=['dataset'])



def parseDataset(datasetMetadata: str = Form(...)):
	try:
		return Dataset.model_validate_json(datasetMetadata)
	except ValidationError as e:
		raise HTTPException(
			detail=jsonable_encoder(e.errors()),
			status_code=422
		)


@datasetRouter.post("/dataset")
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


@datasetRouter.get("/dataset/ark:{naan}/{postfix}")
def getDatasetMetadata(
	naan: str,
	postfix: str
):

	datasetGUID = f"ark:{naan}/{postfix}"
	return datasetRequest.getDatasetMetadata(datasetGUID)



@datasetRouter.get("/dataset/download/ark:{naan}/{postfix}")
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

		dataset_instance = datasetResponse.model
		object_key = dataset_instance.distribution.location.path
		filename = object_key.split("/")[-1]
  
		content_type, _ = mimetypes.guess_type(filename)

		if content_type is None:
			content_type = "application/octet-stream"

		download_headers = {
			"Content-Type": content_type,
			"Content-Disposition": f'attachment; filename="{filename}"'
   		}

		return StreamingResponse(
			datasetResponse.fileResponse['Body'],
			headers=download_headers
		)

	else:
		return JSONResponse(
			status_code=datasetResponse.statusCode,
			content=datasetResponse.error
		)


@datasetRouter.delete("/dataset/ark:{NAAN}/{postfix}")
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
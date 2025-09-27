from fastapi import (
	APIRouter, 
	Depends, 
	HTTPException, 
	Form, 
)
from fastapi.responses import JSONResponse, StreamingResponse
from typing import Annotated
import mimetypes

from fairscape_mds.crud.identifier import IdentifierRequest
from fairscape_mds.core.config import appConfig
from fairscape_mds.deps import getCurrentUser
from fairscape_mds.models.user import UserWriteModel
from fairscape_mds.models.identifier import UpdatePublishRequest

identifierRequestFactory = IdentifierRequest(appConfig)
publishRouter = APIRouter(prefix="")


@publishRouter.put(path="/publish")
def updatePublicationStatus(
	currentUser: Annotated[UserWriteModel, Depends(getCurrentUser)],
	publicationChangeRequest: UpdatePublishRequest 
):
	response = identifierRequestFactory.updatePublicationStatus(
		publicationChangeRequest,
		currentUser
	)

	if response.success:
		return JSONResponse(
			status_code=response.statusCode,
			content=response.jsonResponse
		)
	else:
		return JSONResponse(
			status_code=response.statusCode,
			content=response.error
		)


@publishRouter.get(path="/view/ark:{NAAN}/{postfix}")
def viewContent(
	NAAN: str,
	postfix: str
):

	guid = f"ark:{NAAN}/{postfix}"
	response = identifierRequestFactory.getContent(guid)

	if response.success:

		dataset_instance = response.model
		object_key = dataset_instance.distribution.location.path
		filename = object_key.split("/")[-1]
  
		content_type, _ = mimetypes.guess_type(filename)

		if content_type is None:
			content_type = "application/octet-stream"

		download_headers = {
			"Content-Type": content_type,
			"Content-Disposition": "inline"
   		}

		return StreamingResponse(
			response.fileResponse['Body'],
			headers=download_headers
		)

	else:
		return JSONResponse(
			content=response.error,
			status_code=response.statusCode
		)


@publishRouter.get(path="/download/ark:{NAAN}/{postfix}")
def downloadContent(
	NAAN: str,
	postfix: str
):

	guid = f"ark:{NAAN}/{postfix}"
	response = identifierRequestFactory.getContent(guid)

	if response.success:

		dataset_instance = response.model
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
			response.fileResponse['Body'],
			headers=download_headers
		)

	else:
		return JSONResponse(
			content=response.error,
			status_code=response.statusCode
		)
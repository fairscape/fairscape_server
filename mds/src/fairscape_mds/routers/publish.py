from fastapi import (
	APIRouter, 
	Depends, 
	HTTPException, 
	Form, 
)
from fastapi.responses import JSONResponse

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

@publishRouter.get(path="/ark:{NAAN}/{postfix}?content")
def resolveContent():
	pass
from fastapi import (
	APIRouter,
	Depends,
	HTTPException,
	UploadFile
)
from fastapi.responses import JSONResponse
from typing import Annotated, Optional

from fairscape_mds.core.config import appConfig

from fairscape_mds.models.identifier import MetadataTypeEnum
from fairscape_mds.models.user import UserWriteModel
from fairscape_mds.deps import getCurrentUser
from fairscape_mds.crud.identifier import IdentifierRequest

from fairscape_models.model_card import ModelCard

mlModelRouter = APIRouter(prefix="", tags=["ml model"])
identifierRequestFactory = IdentifierRequest(appConfig)

@mlModelRouter.post("/mlmodel")
def createMLModel(
	currentUser: Annotated[UserWriteModel, Depends(getCurrentUser)],
	MLModelMetadata: ModelCard,
	MLModelContent: Optional[UploadFile],
):
	response = identifierRequestFactory.UploadMLModel(
		userInstance=currentUser,
		mlModelMetadata=MLModelMetadata,
		mlModelContent=MLModelContent
	)

	if response.success:
		return JSONResponse(
			content=response.model.model_dump(by_alias=True, mode='json'),
			status_code=response.statusCode
		)

	else:
		return JSONResponse(
			content=response.error,
			status_code=response.statusCode
		)


@mlModelRouter.get("/mlmodel")
def listMLModel(
	currentUser: Annotated[UserWriteModel, Depends(getCurrentUser)],
):
	response = identifierRequestFactory.listType(
		requestType=MetadataTypeEnum.ML_MODEL,
		user=currentUser
	)

	if response.success:
		return JSONResponse(
			content=response.model,
			status_code=response.statusCode
		)

	else:
		return JSONResponse(
			content=response.error,
			status_code=response.statusCode
		)



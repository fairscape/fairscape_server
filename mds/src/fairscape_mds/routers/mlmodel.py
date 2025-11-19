from fastapi import (
	APIRouter,
	Depends,
	HTTPException,
	UploadFile,
	Form
)
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from typing import Annotated, Optional

from fairscape_mds.core.config import appConfig

from fairscape_mds.models.identifier import MetadataTypeEnum
from fairscape_mds.models.user import UserWriteModel
from fairscape_mds.deps import getCurrentUser
from fairscape_mds.crud.identifier import IdentifierRequest

from fairscape_models.model_card import ModelCard
from pydantic import ValidationError

mlModelRouter = APIRouter(prefix="", tags=["ml model"])
identifierRequestFactory = IdentifierRequest(appConfig)


def parseMLModel(metadata: str = Form(...)):
	try:
		return ModelCard.model_validate_json(metadata)
	except ValidationError as e:
		raise HTTPException(
			detail=jsonable_encoder(e.errors()),
			status_code=422
		)

@mlModelRouter.post("/mlmodel")
def createMLModel(
	currentUser: Annotated[UserWriteModel, Depends(getCurrentUser)],
	metadata: Annotated[ModelCard, Depends(parseMLModel)],
	content: Optional[UploadFile]=None,
):
	response = identifierRequestFactory.UploadMLModel(
		userInstance=currentUser,
		mlModelMetadata=metadata,
		mlModelContent=content
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



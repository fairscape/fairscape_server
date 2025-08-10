from typing import Annotated

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from fairscape_mds.crud.schema import FairscapeSchemaRequest
from fairscape_mds.models.user import UserWriteModel
from fairscape_mds.core.config import appConfig
from fairscape_mds.deps import getCurrentUser
from fairscape_models.schema import Schema

schemaRequest = FairscapeSchemaRequest(appConfig)

schemaRouter = APIRouter(prefix="", tags=['evi', 'schema'])


@schemaRouter.post("/schema")
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


@schemaRouter.get("/schema/ark:{NAAN}/{postfix}")
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


@schemaRouter.delete("/schema/ark:{NAAN}/{postfix}")
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
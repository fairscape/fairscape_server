
from typing import Annotated
from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from fairscape_mds.backend.models import *
from fairscape_mds.deps import getCurrentUser
from fairscape_mds.core.config import appConfig
from fairscape_models.computation import Computation

computationRequest = FairscapeComputationRequest(appConfig)

computationRouter = APIRouter(prefix="", tags=['evi', 'computation'])


@computationRouter.post("/computation")
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


@computationRouter.get("/computation/ark:{NAAN}/{postfix}")
def getComputationMetadata(
	NAAN: str,
	postfix: str,
):

	computationGUID = f"ark:{NAAN}/{postfix}"

	response = computationRequest.getComputation(
		computationGUID
	)
	
	if response.success:
		return response.model

	else:
		return JSONResponse(
			status_code = response.statusCode,
			content = response.error
		)


@computationRouter.delete("/computation/ark:{NAAN}/{postfix}")
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
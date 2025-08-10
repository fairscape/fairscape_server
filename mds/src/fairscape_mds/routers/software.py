from fairscape_mds.crud.software import FairscapeSoftwareRequest
from fairscape_mds.models.user import UserWriteModel
from fairscape_mds.deps import getCurrentUser
from fairscape_mds.core.config import appConfig
from fairscape_models.software import Software

from typing import Annotated
from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

softwareRequest = FairscapeSoftwareRequest(appConfig)

softwareRouter = APIRouter(prefix="", tags=['evi', 'software'])

@softwareRouter.post("/software")
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


@softwareRouter.get("/software/ark:{NAAN}/{postfix}")
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


@softwareRouter.delete("/software/ark:{NAAN}/{postfix}")
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

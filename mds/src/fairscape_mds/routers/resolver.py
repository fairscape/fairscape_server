from fastapi import (
	APIRouter
)
from fastapi.responses import JSONResponse
from fairscape_mds.backend.models import *
from fairscape_mds.core.config import appConfig


resolverRequest = FairscapeResolverRequest(appConfig)
resolverRouter = APIRouter(prefix="/", tags=['evi', 'rocrate'])

@resolverRouter.get("/ark:{NAAN}/{postfix}")
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

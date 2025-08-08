
from typing import Annotated
from fastapi import APIRouter, HTTPException, Depends
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.responses import JSONResponse
from fairscape_mds.backend.models import *
from fairscape_mds.core.config import appConfig

authRouter = APIRouter(prefix="/", tags=['auth'])
userRequest = FairscapeUserRequest(appConfig)



@authRouter.post("/login")
def form(form_data: Annotated[OAuth2PasswordRequestForm, Depends()]):

	response = userRequest.loginUser(form_data.username, form_data.password)

	if not response.success:
		return JSONResponse(
			status_code=401,
			content={"message": "unrecognized username password combination"}
		)
	
	return JSONResponse(
		status_code=response.statusCode,
		content=response.jsonResponse
	)
from typing import Annotated
from fastapi import Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer
from fairscape_mds.core.config import appConfig
from fairscape_mds.crud.user import FairscapeUserRequest

userRequest = FairscapeUserRequest(appConfig)

OAuthScheme = OAuth2PasswordBearer(tokenUrl="token")


def getCurrentUser(
	token: Annotated[str, Depends(OAuthScheme)]
	):
	""" Middleware to return the current user from request authorization headers
	"""

	try:
		foundUser = userRequest.getUserBySession(token)
		return foundUser
	except Exception as e:
		raise HTTPException(
			status_code=401,
			detail=f"Authorization Error Decoding Token\terror: {str(e)}"
		)
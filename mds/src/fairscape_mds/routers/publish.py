from fastapi import (
	APIRouter, 
	Depends, 
	HTTPException, 
	Form, 
)

from fairscape_mds.crud.publish import FairscapePublishRequest
from fairscape_mds.core.config import appConfig


publishRequestFactory = FairscapePublishRequest(appConfig)
publishRouter = APIRouter(prefix="/")

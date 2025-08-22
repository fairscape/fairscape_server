# fairscape_mds/routers/credentials_router.py
from fastapi import APIRouter, Depends, Path, HTTPException
from fastapi.responses import JSONResponse
from typing import Annotated, List


from fairscape_mds.models.user import UserWriteModel

from mds.src.fairscape_mds.crud.credentials import (
    FairscapeCredentialsRequest,
    UserToken,
    UserTokenUpdate
)

from fairscape_mds.core.config import appConfig
from fairscape_mds.main import getCurrentUser

router = APIRouter(
    prefix="/profile/credentials", 
    tags=["User Profile API Tokens"]
)


credentials_request_handler = FairscapeCredentialsRequest(appConfig)

@router.get("", response_model=List[UserToken], summary="Get all API tokens for the current user")
def get_user_api_tokens_route(
   current_user: Annotated[UserWriteModel, Depends(getCurrentUser)],
):
    """
    Retrieves all API tokens (e.g., for Dataverse, Zenodo) registered
    by the currently authenticated user.
    """
    response = credentials_request_handler.get_user_api_tokens(user_instance=current_user)
    if response.success:
        return response.model
    else:
        raise HTTPException(status_code=response.statusCode, detail=response.error)

@router.post("", status_code=201, summary="Add a new API token for the current user")
def add_user_api_token_route(
   new_token_data: UserToken,
   current_user: Annotated[UserWriteModel, Depends(getCurrentUser)],
):
    """
    Adds a new API token for the currently authenticated user.
    The `tokenUID` should be a user-chosen unique identifier for this token (e.g., "my-dataverse-key").
    `endpointURL` specifies the service this token is for (e.g., "https://dataverse.example.edu").
    """
    response = credentials_request_handler.add_user_api_token(
        user_instance=current_user,
        token_instance=new_token_data
    )
    if response.success:
        return JSONResponse(content=response.model, status_code=response.statusCode)
    else:
        raise HTTPException(status_code=response.statusCode, detail=response.error)

@router.delete("/{tokenUID}", summary="Delete an API token for the current user")
def delete_user_api_token_route(
    tokenUID: Annotated[str, Path(title="The unique ID (tokenUID) of the API token to delete")],
    current_user: Annotated[UserWriteModel, Depends(getCurrentUser)],
):
    """
    Deletes a specific API token belonging to the currently authenticated user,
    identified by its `tokenUID`.
    """
    response = credentials_request_handler.delete_user_api_token(
        user_instance=current_user,
        token_uid=tokenUID
    )
    if response.success:
        return JSONResponse(content=response.model, status_code=response.statusCode)
    else:
        raise HTTPException(status_code=response.statusCode, detail=response.error)

@router.put("", summary="Update an existing API token for the current user")
def update_user_api_token_route(
   token_update_data: UserTokenUpdate,
   current_user: Annotated[UserWriteModel, Depends(getCurrentUser)],
):
    """
    Updates an existing API token for the currently authenticated user.
    The token to update is identified by `tokenUID` in the request body.
    Only provided fields (tokenValue, endpointURL, description) will be updated.
    """
    response = credentials_request_handler.update_user_api_token(
        user_instance=current_user,
        token_update=token_update_data
    )
    if response.success:
        return JSONResponse(content=response.model, status_code=response.statusCode)
    else:
        raise HTTPException(status_code=response.statusCode, detail=response.error)
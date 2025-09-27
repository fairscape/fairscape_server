from fastapi import APIRouter, Depends, HTTPException, Query, Body, Path as FastApiPath
from fastapi.responses import JSONResponse
from typing import Annotated, Optional, Dict

from fairscape_mds.models.user import UserWriteModel
from fairscape_mds.main import getCurrentUser 

from fairscape_mds.crud.publish import FairscapePublishRequest
from fairscape_mds.models.publish import DEFAULT_DATAVERSE_URL 

from fairscape_mds.core.config import appConfig


router = APIRouter(
    prefix="/publish",
    tags=["Publishing"],
)

publish_request_handler = FairscapePublishRequest(appConfig)

@router.post(
    "/create/ark:{NAAN}/{postfix}",
    summary="Create a dataset on a publishing platform for an ROCrate",
    response_description="Details of the created dataset and ROCrate update status"
)
async def create_dataset_endpoint(
    NAAN: Annotated[str, FastApiPath(description="NAAN of the ROCrate ARK identifier.")],
    postfix: Annotated[str, FastApiPath(description="Postfix of the ROCrate ARK identifier.")],
    current_user: Annotated[UserWriteModel, Depends(getCurrentUser)],
    user_provided_metadata: Annotated[Dict, Body(
        default={}, embed=True,
        description="User-provided metadata to augment or override ROCrate metadata for platform."
    )] = {},
    platform_url: Annotated[str, Query(
        description="Base URL of the publishing platform (e.g., Dataverse, Zenodo, Figshare)."
    )] = DEFAULT_DATAVERSE_URL,
    database: Annotated[Optional[str], Query(
        description="Platform-specific database/collection (e.g., Dataverse alias)."
    )] = None
):
    """
    Creates a dataset on an external platform (Dataverse, Zenodo, Figshare) using an ROCrate's metadata.
    The ROCrate's record in Fairscape is updated with the new persistent ID from the platform.
    """
    rocrate_guid = f"ark:{NAAN}/{postfix}"

    response = await publish_request_handler.create_dataset_on_platform(
        current_user=current_user,
        rocrate_guid=rocrate_guid,
        user_provided_metadata=user_provided_metadata,
        platform_url=platform_url,
        database=database
    )

    if not response.success:
        raise HTTPException(status_code=response.statusCode, detail=response.error)

    return JSONResponse(status_code=response.statusCode, content=response.model)

@router.post(
    "/upload/ark:{NAAN}/{postfix}",
    summary="Upload ROCrate archive to an existing platform dataset",
    response_description="Details of the file upload transaction."
)
async def upload_dataset_files_endpoint(
    NAAN: Annotated[str, FastApiPath(description="NAAN of the ROCrate ARK identifier.")],
    postfix: Annotated[str, FastApiPath(description="Postfix of the ROCrate ARK identifier.")],
    current_user: Annotated[UserWriteModel, Depends(getCurrentUser)], # Updated User Model
    platform_url: Annotated[str, Query(
        description="Base URL of the publishing platform."
    )] = DEFAULT_DATAVERSE_URL
):
    """
    Uploads the ROCrate's archive (zip file from Minio) to a previously created dataset
    on an external platform. Requires 'transaction_identifier' from the create step to be
    present in the ROCrate's metadata.
    """
    rocrate_guid = f"ark:{NAAN}/{postfix}"

    response = await publish_request_handler.upload_files_to_platform(
        current_user=current_user,
        rocrate_guid=rocrate_guid,
        platform_url=platform_url
    )

    if not response.success:
        raise HTTPException(status_code=response.statusCode, detail=response.error)

    return JSONResponse(status_code=response.statusCode, content=response.model)
from typing import (
	Annotated
)
from fastapi import (
	APIRouter, 
	Depends, 
	HTTPException, 
	Request, 
	UploadFile
)
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.encoders import jsonable_encoder
from fairscape_mds.crud.rocrate import FairscapeROCrateRequest

from fairscape_mds.models.user import UserWriteModel
from fairscape_mds.core.config import appConfig
from fairscape_models.rocrate import ROCrateV1_2, ROCrateMetadataElem
from fairscape_mds.deps import getCurrentUser
from fairscape_mds.worker import processROCrate

from fairscape_models.conversion.converter import ROCToTargetConverter
from fairscape_models.conversion.mapping.croissant import MAPPING_CONFIGURATION as CROISSANT_MAPPING

import pathlib

rocrateRequest = FairscapeROCrateRequest(appConfig)

rocrateRouter = APIRouter(prefix="", tags=['evi', 'rocrate'])


@rocrateRouter.post("/rocrate/upload-async")
def uploadROCrate(
	currentUser: Annotated[UserWriteModel, Depends(getCurrentUser)],
	crate: UploadFile
):

	uploadOperation = rocrateRequest.uploadROCrate(
		userInstance=currentUser,
		rocrate=crate
	)

	if uploadOperation.success:

		uploadJob = uploadOperation.model

		# start backend job
		processROCrate.apply_async(args=(uploadJob.guid,), )

		return uploadJob

	else:
		return JSONResponse(
			status_code=400,
			content={"error": uploadOperation.error}
		)
  
@rocrateRouter.post(
    "/rocrate/metadata",
    summary="Mint metadata-only ROCrate records without file content",
    status_code=201
)
def publishMetadataOnly(
    currentUser: Annotated[UserWriteModel, Depends(getCurrentUser)],
    crateMetadata: ROCrateV1_2
):
    try:
        # Call the mintMetadataOnlyROCrate method on the existing rocrateRequest
        result = rocrateRequest.mintMetadataOnlyROCrate(
            requestingUser=currentUser,
            crateModel=crateMetadata
        )
        
        if result.success:
            return JSONResponse(
                content=result.model,
                status_code=result.statusCode
            )
        else:
            return JSONResponse(
                content=result.error,
                status_code=result.statusCode
            )
    except Exception as e:
        return JSONResponse(
            content={
                "message": "Error minting metadata-only ROCrate identifiers",
                "error": str(e)
            },
            status_code=500
        )
        
@rocrateRouter.get(
    "/rocrate",
    summary="List all ROCrates accessible by the current user",
    response_description="A list of RO-Crates with their basic metadata"
)
def list_rocrates_endpoint(
    currentUser: Annotated[UserWriteModel, Depends(getCurrentUser)],
):
    fairscape_response = rocrateRequest.list_crates(requestingUser=currentUser)

    if fairscape_response.success:
        return JSONResponse(
            status_code=fairscape_response.statusCode,
            content=fairscape_response.model
        )
    else:
        raise HTTPException(
            status_code=fairscape_response.statusCode,
            detail=fairscape_response.error
        )


@rocrateRouter.get("/rocrate/upload/status/{submissionUUID}")
def getUploadStatus(
	currentUser: Annotated[UserWriteModel, Depends(getCurrentUser)],
	submissionUUID: str
):

	response = rocrateRequest.getUploadMetadata(
		currentUser,
		submissionUUID
	)

	if response.success:
		return response.model

	else:
		return JSONResponse(
			status_code = response.statusCode,
			content = response.error
		)


@rocrateRouter.get("/rocrate/download/ark:{NAAN}/{postfix}")
def getROCrateArchive(
	currentUser: Annotated[UserWriteModel, Depends(getCurrentUser)],
	NAAN: str,
	postfix: str
):

	rocrateGUID = f"ark:{NAAN}/{postfix}"

	response = rocrateRequest.downloadROCrateArchive(
		currentUser,
		rocrateGUID
	)
	
	if response.success:

		object_key = response.model.distribution.location.path
		filename = pathlib.Path(object_key).name

		zip_headers = {
			"Content-Type": "application/zip",
			"Content-Disposition": f'attachment; filename="{filename}"'
    	}
        
		return StreamingResponse(
			response.fileResponse,
			headers=zip_headers
		)

	else:
		return JSONResponse(
			status_code = response.statusCode,
			content = response.error
		)

@rocrateRouter.get("/rocrate/ark:{NAAN}/{postfix}")
def getROCrateMetadata(
    request: Request,
    NAAN: str,
    postfix: str,
):
    """
    Retrieve RO-Crate metadata.  
    Supports content negotiation:  
    - `application/json` (default, raw RO-Crate JSON)  
    - `application/vnd.mlcommons-croissant+json` (Croissant JSON-LD)  
    """
    guid = f"ark:{NAAN}/{postfix}"
    response = rocrateRequest.getROCrateMetadata(guid)

    if not response.success:
        return JSONResponse(
            status_code=response.statusCode,
            content=response.error
        )

    accept_header = request.headers.get("accept", "application/json")

    if "application/vnd.mlcommons-croissant+json" in accept_header.lower():
        try:
            source_crate = ROCrateV1_2(**response.model["metadata"])
            croissant_converter = ROCToTargetConverter(source_crate, CROISSANT_MAPPING)
            croissant_result = croissant_converter.convert()

            return JSONResponse(
                status_code=200,
                content=croissant_result.model_dump(by_alias=True, exclude_none=True),
                media_type="application/vnd.mlcommons-croissant+json"
            )
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Error converting RO-Crate to Croissant: {str(e)}"
            )

    return JSONResponse(
        status_code=200,
        content=response.model
    )

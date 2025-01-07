from fastapi import APIRouter, Depends, HTTPException, Query, Body
from fastapi.responses import JSONResponse
from typing import Annotated
from pathlib import Path
from datetime import datetime

from fairscape_mds.models.user import UserLDAP
from fairscape_mds.auth.oauth import getCurrentUser
from fairscape_mds.config import get_fairscape_config
from fairscape_mds.models.fairscape_base import FairscapeBaseModel
from fairscape_mds.auth.ldap import getUserTokens

from fairscape_mds.models.publish import PublishingService, DataversePublisher, ZenodoPublisher, DEFAULT_DATAVERSE_URL, DEFAULT_DATAVERSE_DB

router = APIRouter()

# Initialize services
fairscapeConfig = get_fairscape_config()
mongoClient = fairscapeConfig.CreateMongoClient()
mongoDB = mongoClient[fairscapeConfig.mongo.db]
rocrateCollection = mongoDB[fairscapeConfig.mongo.rocrate_collection]
minioClient = fairscapeConfig.CreateMinioClient()

# Initialize publishing service
publishing_service = PublishingService()
publishing_service.register_publisher(
    "dataverse",
    DataversePublisher(DEFAULT_DATAVERSE_URL, DEFAULT_DATAVERSE_DB)
)
publishing_service.register_publisher(
    "zenodo",
    ZenodoPublisher()
)

@router.post("/publish/create/ark:{NAAN}/{postfix}")
async def create_dataset(
    currentUser: Annotated[UserLDAP, Depends(getCurrentUser)],
    NAAN: str,
    postfix: str,
    userProvidedMetadata: dict = Body(default={}),
    platform_url: str = Query(default=DEFAULT_DATAVERSE_URL, description="Platform URL"),
    database: str | None = Query(default=None, description="Custom database name")
):
    """Create a dataset on the specified platform"""
    
    rocrateGUID = f"ark:{NAAN}/{postfix}"
    rocrateMetadata = rocrateCollection.find_one({"@id": rocrateGUID}, projection={"_id": 0})
    
    if rocrateMetadata is None:
        return JSONResponse(
            status_code=404,
            content={
                "@id": f"{fairscapeConfig.url}/{rocrateGUID}",
                "error": f"unable to find record for RO-Crate: {rocrateGUID}"
            }
        )
    
    # Authorization check
    rocrateGroup = rocrateMetadata.get("permissions", {}).get("group")
    if rocrateGroup not in currentUser.memberOf and fairscapeConfig.ldap.adminDN not in currentUser.memberOf:
        raise HTTPException(status_code=401, detail="User not authorized to publish this ROCrate")
    
    # Get the appropriate publisher
    publisher, _ = publishing_service.get_publisher(platform_url)
    
    # Get platform-specific token
    tokens = getUserTokens(fairscapeConfig.ldap.connectAdmin(), currentUser.dn)
    api_token = None
    for token in tokens:
        if token.endpointURL == platform_url:
            api_token = token.tokenValue
            break
    
    if not api_token:
        raise HTTPException(
            status_code=401,
            detail=f"No token found for platform URL: {platform_url}. Please add your token first."
        )
    
    # Extract group CN for affiliation
    group_affiliation = None
    for group in currentUser.memberOf:
        if group.startswith('cn='):
            # Extract the CN value between 'cn=' and first comma
            group_cn = group.split(',')[0].replace('cn=', '')
            group_affiliation = group_cn
            break
    
    # Prepare metadata
    metadata = rocrateMetadata | userProvidedMetadata
    metadata.update({
        "contactName": currentUser.cn,
        "contactEmail": currentUser.email,
        "authorAffiliation": group_affiliation
    })
    
    try:
        # Create the dataset
        dataset_info = await publisher.create_dataset(metadata, api_token)

        # Update ROCrate with new identifier
        rocrate = FairscapeBaseModel(
            guid=rocrateGUID,
            metadataType=rocrateMetadata.get("@type"),
            name=rocrateMetadata.get("name"),
            identifier=dataset_info["persistent_id"]
        )
        
        update_result = rocrate.update(rocrateCollection)
        if not update_result.success:
            print(f"Failed to update ROCrate with identifier: {update_result.message}")
        
        return JSONResponse(
            status_code=201,
            content={
                **dataset_info,
                "rocrate_update": update_result.success
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/publish/upload/ark:{NAAN}/{postfix}")
async def upload_dataset(
    currentUser: Annotated[UserLDAP, Depends(getCurrentUser)],
    NAAN: str,
    postfix: str,
    platform_url: str = Query(default=DEFAULT_DATAVERSE_URL, description="Platform URL")
):
    """Upload files to an existing dataset"""
    
    rocrateGUID = f"ark:{NAAN}/{postfix}"
    rocrateMetadata = rocrateCollection.find_one({"@id": rocrateGUID}, projection={"_id": 0})
    
    if rocrateMetadata is None:
        return JSONResponse(
            status_code=404,
            content={
                "@id": f"{fairscapeConfig.url}/{rocrateGUID}",
                "error": f"unable to find record for RO-Crate: {rocrateGUID}"
            }
        )
    
    # Authorization check
    rocrateGroup = rocrateMetadata.get("permissions", {}).get("group")
    if rocrateGroup not in currentUser.memberOf and fairscapeConfig.ldap.adminDN not in currentUser.memberOf:
        raise HTTPException(status_code=401, detail="User not authorized to publish this ROCrate")
    
    # Get the appropriate publisher
    publisher, platform = publishing_service.get_publisher(platform_url)
    
    # Check if the ROCrate has a platform identifier
    persistent_id = rocrateMetadata.get("identifier")
    if not persistent_id:
        raise HTTPException(
            status_code=400,
            detail=f"Dataset has not been created yet. Please create the dataset first."
        )
    
    # Get platform-specific token
    tokens = getUserTokens(fairscapeConfig.ldap.connectAdmin(), currentUser.dn)
    api_token = None
    for token in tokens:
        if token.endpointURL == platform_url:
            api_token = token.tokenValue
            break
    
    if not api_token:
        raise HTTPException(
            status_code=401,
            detail=f"No token found for platform URL: {platform_url}. Please add your token first."
        )
    
    # Get the file path and upload
    file_path = rocrateMetadata.get("distribution", {}).get("archivedObjectPath")
    if not file_path:
        raise HTTPException(status_code=404, detail="No file associated with this ROCrate")
    
    try:
        file_data = minioClient.get_object(
            bucket_name=fairscapeConfig.minio.rocrate_bucket,
            object_name=file_path
        )
        
        upload_info = await publisher.upload_files(
            persistent_id,
            file_data.read(),
            Path(file_path).name,
            api_token
        )
        
        return JSONResponse(
            status_code=200,
            content={
                **upload_info,
                "persistent_id": persistent_id
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error during file upload: {str(e)}")
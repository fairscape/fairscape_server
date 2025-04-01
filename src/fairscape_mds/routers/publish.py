from fastapi import APIRouter, Depends, HTTPException, Query, Body
from fastapi.responses import JSONResponse
from typing import Annotated
from pathlib import Path

from fairscape_mds.models.user import UserLDAP
from fairscape_mds.auth.oauth import getCurrentUser
from fairscape_mds.config import get_fairscape_config
from fairscape_models.fairscape_base import FairscapeBaseModel
from fairscape_mds.auth.ldap import getUserTokens

from fairscape_mds.models.publish import PublishingService, DataversePublisher, ZenodoPublisher, FigsharePublisher, DEFAULT_DATAVERSE_URL, DEFAULT_DATAVERSE_DB

router = APIRouter()

# Initialize services
fairscapeConfig = get_fairscape_config()
mongoClient = fairscapeConfig.CreateMongoClient()
mongoDB = mongoClient[fairscapeConfig.mongo.db]
rocrateCollection = mongoDB[fairscapeConfig.mongo.rocrate_collection]
minioClient = fairscapeConfig.CreateMinioClient()

# Initialize publishing service with dynamic publishers
publishing_service = PublishingService()

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
    
    # Get or create the appropriate publisher
    if "dataverse" in platform_url.lower():
        db = database or DEFAULT_DATAVERSE_DB
        publisher = DataversePublisher(platform_url, db)
        platform = "dataverse"
    elif "zenodo" in platform_url.lower():
        publisher = ZenodoPublisher(platform_url)
        platform = "zenodo"
    elif "figshare" in platform_url.lower():
        publisher = FigsharePublisher(platform_url)
        platform = "figshare"
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported platform URL: {platform_url}")
    
    # Get platform-specific token
    tokens = getUserTokens(fairscapeConfig.ldap.connectAdmin(), currentUser.dn)
    api_token = next((token.tokenValue for token in tokens if token.endpointURL == platform_url), None)
    
    if not api_token:
        raise HTTPException(
            status_code=401,
            detail=f"No token found for platform URL: {platform_url}. Please add your token first."
        )
    
    # Extract group CN for affiliation
    group_affiliation = next((
        group.split(',')[0].replace('cn=', '')
        for group in currentUser.memberOf
        if group.startswith('cn=')
    ), None)
    
    # Prepare metadata
    metadata = rocrateMetadata | userProvidedMetadata
    metadata.update({
        "contactName": currentUser.cn,
        "contactEmail": currentUser.email,
        "authorAffiliation": group_affiliation
    })
    
    # Create the dataset
    dataset_info = await publisher.create_dataset(metadata, api_token)
    
    # Handle platform-specific transaction IDs
    if platform == 'dataverse':
        dataset_info['transaction_id'] = dataset_info["persistent_id"]
    elif platform in ['zenodo', 'figshare']:
        # Both Zenodo and Figshare already provide transaction_id in dataset_info
        pass
    
    print("DATASET INFO")
    print(dataset_info)
    # Update ROCrate with new identifier
    rocrate = FairscapeBaseModel(
        guid=rocrateGUID,
        metadataType=rocrateMetadata.get("@type"),
        name=rocrateMetadata.get("name"),
        identifier=dataset_info.get("persistent_id"),
        transaction_identifier=dataset_info.get("transaction_id","")
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
    
    # Get or create the appropriate publisher
    if "dataverse" in platform_url.lower():
        publisher = DataversePublisher(platform_url, DEFAULT_DATAVERSE_DB)
        platform = "dataverse"
    elif "zenodo" in platform_url.lower():
        publisher = ZenodoPublisher(platform_url)
        platform = "zenodo"
    elif "figshare" in platform_url.lower():
        publisher = FigsharePublisher(platform_url)
        platform = "figshare"
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported platform URL: {platform_url}")
    
    # Check if the ROCrate has a platform identifier
    persistent_id = rocrateMetadata.get("transaction_identifier")
    if not persistent_id:
        raise HTTPException(
            status_code=400,
            detail=f"Dataset has not been created yet. Please create the dataset first."
        )

    # Get platform-specific token
    tokens = getUserTokens(fairscapeConfig.ldap.connectAdmin(), currentUser.dn)
    api_token = next((token.tokenValue for token in tokens if token.endpointURL == platform_url), None)
    
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
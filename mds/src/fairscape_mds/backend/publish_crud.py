# fairscape_mds/crud/publish_crud.py

from typing import Dict, Any, Optional
from pathlib import Path

from pymongo.collection import Collection

from fairscape_mds.backend.models import (
    FairscapeRequest,
    FairscapeResponse,
    UserWriteModel,
    Permissions, 
    checkPermissions
)
from fairscape_mds.backend.credentials_crud import (
    FairscapeCredentialsRequest,
    UserToken 
)

from fairscape_mds.backend.publish import (
    PublishingService,
    DataversePublisher,
    ZenodoPublisher,
    FigsharePublisher,
    DEFAULT_DATAVERSE_DB
)


class FairscapePublishRequest(FairscapeRequest):
    def __init__(
        self,
        config
    ):
        self.config = config
        super().__init__(config)

        self.credentials_request_handler = FairscapeCredentialsRequest(config)
        self.publishing_service = PublishingService()

    async def create_dataset_on_platform(
        self,
        current_user: UserWriteModel,
        rocrate_guid: str,
        user_provided_metadata: dict,
        platform_url: str,
        database: Optional[str] = None
    ) -> FairscapeResponse:

        rocrate_metadata_doc = self.config.rocrateCollection.find_one({"@id": rocrate_guid}) # Get full doc
        if rocrate_metadata_doc is None:
            return FairscapeResponse(
                success=False,
                statusCode=404,
                error={
                    "@id": f"ark://{rocrate_guid}", 
                    "message": f"ROCrate not found: {rocrate_guid}"
                } 
            )

        rocrate_permissions_dict = rocrate_metadata_doc.get("permissions")
        if not rocrate_permissions_dict:
            return FairscapeResponse(
                success=False, 
                statusCode=403, 
                error={"message": "ROCrate has no permission metadata."}
                )
        
        try:
            rocrate_permissions = Permissions.model_validate(rocrate_permissions_dict)
        except Exception as e:
            return FairscapeResponse(
                success=False, 
                statusCode=500, 
                error={"message": "Invalid ROCrate permission format."}
            )

        if not checkPermissions(rocrate_permissions, current_user):
            return FairscapeResponse(
                success=False,
                statusCode=401,
                error={"message": "User not authorized to publish this ROCrate"}
            )

        try:
            if "dataverse" in platform_url.lower():
                db = database or DEFAULT_DATAVERSE_DB
                publisher = DataversePublisher(platform_url, db)
            elif "zenodo" in platform_url.lower():
                publisher = ZenodoPublisher(platform_url)
            elif "figshare" in platform_url.lower():
                publisher = FigsharePublisher(platform_url)
            else:
                return FairscapeResponse(
                    success=False, 
                    statusCode=400, 
                    error={
                        "message": f"Unsupported platform URL: {platform_url}"
                        }
                    )

        except Exception as e:
            return FairscapeResponse(
                success=False, 
                statusCode=500, 
                error={"message": f"Error setting up publisher: {str(e)}"}
            )

        # Get User's API tokens for the platform
        token_response = self.credentials_request_handler.get_user_api_tokens(user_instance=current_user)
        if not token_response.success:
            return FairscapeResponse(success=False, statusCode=token_response.statusCode, error=token_response.error)

        user_api_tokens: list[UserToken] = token_response.model
        api_token_value = next((token.tokenValue for token in user_api_tokens if token.endpointURL == platform_url), None)

        if not api_token_value:
            return FairscapeResponse(success=False, statusCode=401, error={"message": f"No API token found for platform URL: {platform_url}."})

        group_affiliation = current_user.groups[0] if current_user.groups else "FAIRSCAPE"


        # Remove MongoDB's _id before passing to publisher, and merge with user metadata
        rocrate_metadata_cleaned = {k: v for k, v in rocrate_metadata_doc.items() if k != "_id"}
        final_metadata = {**rocrate_metadata_cleaned, **user_provided_metadata}
        final_metadata.update({
            "contactName": f"{current_user.firstName} {current_user.lastName}",
            "contactEmail": current_user.email,
            "authorAffiliation": group_affiliation,
            "name": final_metadata.get('name', f"Dataset for ROCrate {rocrate_guid}"),
            "description": final_metadata.get('description', 'FAIRSCAPE published dataset.'),
            "author": final_metadata.get('author', f"{current_user.firstName} {current_user.lastName}")
        })

        try:
            dataset_info = await publisher.create_dataset(final_metadata, api_token_value)
        except Exception as e:
            detail = getattr(e, 'detail', str(e))
            status = getattr(e, 'status_code', 500) 
            return FairscapeResponse(success=False, statusCode=status, error={"message": f"Platform error: {detail}"})

        update_doc = {
            "identifier": dataset_info.get("persistent_id"), 
            # Some publishers use a transaction in addition to a persistent ID
            "transaction_identifier": dataset_info.get("transaction_id", dataset_info.get("persistent_id"))
        }
        update_result = self.config.rocrateCollection.update_one({"@id": rocrate_guid}, {"$set": update_doc})

        rocrate_update_status = "failed"
        rocrate_update_message = f"ROCrate {rocrate_guid} not found for update."
        if update_result.matched_count > 0:
            if update_result.modified_count > 0:
                rocrate_update_status = "success"
                rocrate_update_message = f"ROCrate {rocrate_guid} updated with platform identifiers."
            else:
                rocrate_update_status = "no_change"
                rocrate_update_message = f"ROCrate {rocrate_guid} already had these platform identifiers."
        
        dataset_info["rocrate_update_status"] = rocrate_update_status
        dataset_info["rocrate_update_message"] = rocrate_update_message

        return FairscapeResponse(success=True, statusCode=201, model=dataset_info)

    async def upload_files_to_platform(
        self,
        current_user: UserWriteModel,
        rocrate_guid: str,
        platform_url: str
    ) -> FairscapeResponse:

        rocrate_metadata_doc = self.config.rocrateCollection.find_one({"@id": rocrate_guid})
        if rocrate_metadata_doc is None:
            return FairscapeResponse(success=False, statusCode=404, error={"message": f"ROCrate not found: {rocrate_guid}"})

        rocrate_permissions_dict = rocrate_metadata_doc.get("permissions")
        if not rocrate_permissions_dict:
            return FairscapeResponse(success=False, statusCode=403, error={"message": "ROCrate has no permission metadata."})
        
        try:
            rocrate_permissions = Permissions.model_validate(rocrate_permissions_dict)
        except Exception as e:
            return FairscapeResponse(success=False, statusCode=500, error={"message": "Invalid ROCrate permission format."})

        if not checkPermissions(rocrate_permissions, current_user):
            return FairscapeResponse(success=False, statusCode=401, error={"message": "User not authorized"})

        try:
            if "dataverse" in platform_url.lower():
                publisher = DataversePublisher(platform_url, DEFAULT_DATAVERSE_DB)
            elif "zenodo" in platform_url.lower():
                publisher = ZenodoPublisher(platform_url)
            elif "figshare" in platform_url.lower():
                publisher = FigsharePublisher(platform_url)
            else:
                return FairscapeResponse(success=False, statusCode=400, error={"message": f"Unsupported platform URL: {platform_url}"})
        except Exception as e:
            return FairscapeResponse(success=False, statusCode=500, error={"message": f"Error setting up publisher for upload: {str(e)}"})

        platform_dataset_id = rocrate_metadata_doc.get("transaction_identifier")
        # Use transaction_identifier which is specific to the deposit
        if not platform_dataset_id: 
            return FairscapeResponse(success=False, statusCode=400, error={"message": "Dataset not yet created on platform or missing transaction ID."})

        token_response = self.credentials_request_handler.get_user_api_tokens(user_instance=current_user)
        if not token_response.success:
            return FairscapeResponse(success=False, statusCode=token_response.statusCode, error=token_response.error)
        
        user_api_tokens: list[UserToken] = token_response.model
        api_token_value = next((token.tokenValue for token in user_api_tokens if token.endpointURL == platform_url), None)

        if not api_token_value:
            return FairscapeResponse(success=False, statusCode=401, error={"message": f"No API token for platform: {platform_url}"})

        dist = rocrate_metadata_doc.get("distribution", {})
        file_path_in_minio = dist.get("location", {}).get("path")
        
        if not file_path_in_minio:
            return FairscapeResponse(success=False, statusCode=400, error={"message": "No file path found in ROCrate distribution."})


        try:
            minio_object = self.config.minioClient.get_object(
                Bucket=self.config.minioBucket, 
                Key=file_path_in_minio
                )
            file_data = minio_object['Body'].read()
            file_name_for_upload = Path(file_path_in_minio).name
        except Exception as e:
            return FairscapeResponse(
                success=False, 
                statusCode=500, 
                error={"message": f"Minio download error: {str(e)}"}
            )

        try:
            upload_info = await publisher.upload_files(
                platform_dataset_id, file_data, file_name_for_upload, api_token_value
            )
        except Exception as e:
            detail = getattr(e, 'detail', str(e))
            status = getattr(e, 'status_code', 500)
            return FairscapeResponse(
                success=False, 
                statusCode=status, 
                error={"message": f"Platform upload error: {detail}"}
            )
        
        upload_info_response = {
            **upload_info, 
            "rocrate_guid": rocrate_guid, 
            "platform_dataset_id": platform_dataset_id
        }

        return FairscapeResponse(
            success=True, 
            statusCode=200, 
            model=upload_info_response
        )
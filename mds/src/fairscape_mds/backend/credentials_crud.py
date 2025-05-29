# fairscape_mds/backend/credentials_crud.py
from pymongo.collection import Collection
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field

# Assuming models are in fairscape_mds.backend.model
from fairscape_mds.backend.models import (  
    FairscapeResponse,
    FairscapeRequest,
    UserWriteModel,
    #UserToken,
    #UserTokenUpdate
)

##########################
# Move to models later
##########################
class UserToken(BaseModel):
    tokenUID: str
    tokenValue: str
    endpointURL: str
    description: Optional[str] = Field(default=None)

class UserTokenUpdate(BaseModel):
    tokenUID: str
    tokenValue: Optional[str] = Field(default=None)
    endpointURL: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)

class FairscapeCredentialsRequest(FairscapeRequest):

    def add_user_api_token(
        self,
        user_instance: UserWriteModel,
        token_instance: UserToken  
    ) -> FairscapeResponse:
        """
        Adds a new user API token to the MongoDB collection.
        Checks if a token with the same tokenUID already exists for the user.
        """
        existing_token = self.config.tokensCollection.find_one(
            {"user_email": user_instance.email, "tokenUID": token_instance.tokenUID}
        )
        if existing_token:
            return FairscapeResponse(
                success=False,
                statusCode=409,  
                error={"message": f"An API token with UID '{token_instance.tokenUID}' already exists for this user."}
            )

        token_document = token_instance.model_dump()
        token_document["user_email"] = user_instance.email  


        try:
            result = self.config.tokensCollection.insert_one(token_document)
            if result.acknowledged:
                return FairscapeResponse(
                    success=True,
                    statusCode=201,
                    model={"uploaded": {"tokenUID": token_instance.tokenUID}}
                )
            else:
                return FairscapeResponse(
                    success=False,
                    statusCode=500,
                    error={"message": "Failed to insert API token into database (not acknowledged)."}
                )
        except Exception as e:
            return FairscapeResponse(
                success=False,
                statusCode=500,
                error={"message": f"An unexpected error occurred while adding token: {str(e)}"}
            )

    def get_user_api_tokens(
        self,
        user_instance: UserWriteModel
    ) -> FairscapeResponse:
        """
        Retrieves all API tokens for a given user from MongoDB.
        """
        try:
            token_docs = self.config.tokensCollection.find({"user_email": user_instance.email})
            token_list = [UserToken.model_validate(doc) for doc in token_docs]
            return FairscapeResponse(
                success=True,
                statusCode=200,
                model=token_list
            )
        except Exception as e:
            return FairscapeResponse(
                success=False,
                statusCode=500,
                error={"message": f"An unexpected error occurred while retrieving API tokens: {str(e)}"}
            )

    def delete_user_api_token(
        self,
        user_instance: UserWriteModel,
        token_uid: str
    ) -> FairscapeResponse:
        """
        Deletes a specific API token for a user from MongoDB.
        """
        try:
            result = self.config.tokensCollection.delete_one(
                {"user_email": user_instance.email, "tokenUID": token_uid}
            )
            if result.deleted_count > 0:
                return FairscapeResponse(
                    success=True,
                    statusCode=200,
                    model={"deleted": {"tokenUID": token_uid}}
                )
            else:
                return FairscapeResponse(
                    success=False,
                    statusCode=404,
                    error={"message": f"API Token with UID '{token_uid}' not found for this user."}
                )
        except Exception as e:
            return FairscapeResponse(
                success=False,
                statusCode=500,
                error={"message": f"An unexpected error occurred while deleting the API token: {str(e)}"}
            )

    def update_user_api_token(
        self,
        user_instance: UserWriteModel,
        token_update: UserTokenUpdate 
    ) -> FairscapeResponse:
        """
        Updates an existing user API token in MongoDB.
        """
        update_fields: Dict[str, Any] = {}
        if token_update.tokenValue is not None:
            update_fields["tokenValue"] = token_update.tokenValue
        if token_update.endpointURL is not None:
            update_fields["endpointURL"] = token_update.endpointURL
        if token_update.description is not None:
            update_fields["description"] = token_update.description

        if not update_fields:
            return FairscapeResponse(
                success=False,
                statusCode=400,
                error={"message": "No update fields provided for the API token."}
            )

        try:
            result = self.config.tokensCollection.update_one(
                {"user_email": user_instance.email, "tokenUID": token_update.tokenUID},
                {"$set": update_fields}
            )
            if result.matched_count == 0:
                return FairscapeResponse(
                    success=False,
                    statusCode=404,
                    error={"message": f"API Token with UID '{token_update.tokenUID}' not found for this user."}
                )
            return FairscapeResponse(
                success=True,
                statusCode=200,
                model={"updated": {"tokenUID": token_update.tokenUID}}
            )
        except Exception as e:
            return FairscapeResponse(
                success=False,
                statusCode=500,
                error={"message": f"An unexpected error occurred while updating the API token: {str(e)}"}
            )
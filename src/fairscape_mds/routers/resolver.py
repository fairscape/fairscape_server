from fastapi import APIRouter, Depends, Path
from fastapi.responses import JSONResponse, RedirectResponse
from typing import Annotated

from fairscape_mds.config import (
    get_fairscape_config,
    get_mongo_client
)

ResolverRouter = APIRouter()


fairscapeConfig = get_fairscape_config()
mongo_client = get_mongo_client()
mongo_db = mongo_client[fairscapeConfig.mongo.db]
identifierCollection = mongo_db[fairscapeConfig.mongo.identifier_collection]

@ResolverRouter.get(
    "/ark:{NAAN}/{postfix}",
    summary="Retrieve metadata for a specified ARK",
    response_description="ARK metadata",
    responses={
        200: {"description": "Successful Response", "content": {"application/json": {}}},
        404: {"description": "ARK not found"},
        500: {"description": "Server error"}
    }
)
def resolve(
    NAAN: Annotated[str, Path(title="ARK Name Assigning Authority Number")],
    postfix: Annotated[str, Path(title="Persistent Identifier")]
):
    """Resolve Identifier Metadata and Return in JSON-LD"""
    if len(NAAN) != 5:
        return JSONResponse(
            status_code=404,
            content={"error": "NAAN must be 5 digits"}
        )
    
    if NAAN != fairscapeConfig.NAAN:
        return RedirectResponse(f'https://n2t.net/ark:{NAAN}/{postfix}')
        
    arkGUID = f"ark:{NAAN}/{postfix}"
    arkMetadata = identifierCollection.find_one(
    {"$or": [
        {"@id": arkGUID},
        {"@id": f"{arkGUID}/"}
    ]},
    projection={"_id": False}
    )
    
    if arkMetadata is None:
        return JSONResponse(
            status_code=404,
            content={"@id": arkGUID, "error": "ARK not found"}
        )
    
    # Remove _id from top level
    arkMetadata.pop("_id", None)
    
    # Process @graph section if it exists
    if "@graph" in arkMetadata:
        for item in arkMetadata["@graph"]:
            if isinstance(item, dict):
                item.pop("_id", None)
    
    return JSONResponse(content=arkMetadata)

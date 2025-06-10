# File: mds/src/fairscape_mds/backend/search_router.py
from fastapi import APIRouter, Query, HTTPException
from typing import Annotated

from fairscape_mds.backend.search_crud import FairscapeSearchRequest
from fairscape_mds.backend.search_models import SearchResults
from fairscape_mds.backend.backend import config

router = APIRouter(
    prefix="/search",
    tags=["Search"]
)

search_request_handler = FairscapeSearchRequest(config)

@router.get("/basic", response_model=SearchResults, summary="Perform a basic keyword search")
def basic_search_route(
    query: Annotated[str, Query(description="The search query string.")]
):
    """
    Performs a basic case-insensitive search across 'name', 'description', 
    and 'keywords' fields of metadata associated with FAIRSCAPE identifiers.
    """
    if not query:
        raise HTTPException(status_code=400, detail="Query parameter cannot be empty.")
        
    response = search_request_handler.basic_search(query_string=query)

    if response.success:
        return response.model
    else:
        raise HTTPException(status_code=response.statusCode, detail=response.error)
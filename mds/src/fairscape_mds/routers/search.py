from fastapi import APIRouter, Query, HTTPException
from typing import Annotated
from mds.src.fairscape_mds.crud.search import FairscapeSearchRequest
from mds.src.fairscape_mds.models.search import SearchResults, SearchResultItem
from fairscape_mds.core.config import appConfig
import httpx
import asyncio

router = APIRouter(
    prefix="/search",
    tags=["Search"]
)

search_request_handler = FairscapeSearchRequest(appConfig)

@router.get("/basic", response_model=SearchResults, summary="Perform a basic keyword search")
def basic_search_route(
    query: Annotated[str, Query(description="The search query string.")]
):
    if not query:
        raise HTTPException(status_code=400, detail="Query parameter cannot be empty.")
    
    response = search_request_handler.basic_search(query_string=query)
    if response.success:
        return response.model
    else:
        raise HTTPException(status_code=response.statusCode, detail=response.error)

@router.get("/semantic", response_model=SearchResults, summary="Perform a semantic search")
async def semantic_search_route(
    query: Annotated[str, Query(description="The search query string.")]
):
    if not query:
        raise HTTPException(status_code=400, detail="Query parameter cannot be empty.")
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                "http://test-fairscape-search-service:5050/api/search/semantic",
                params={"query": query},
                timeout=30.0
            )
            response.raise_for_status()
            data = response.json()
            
            return {
                "query": data["query"],
                "total_results": data["total_results"],
                "results": [
                    {
                        "@id": result["id"],
                        "type": None,
                        "name": result.get("name", ""),
                        "description": result.get("description", ""),
                        "keywords": result.get("keywords", []),
                        "score": result.get("score", 0.0)
                    }
                    for result in data["results"]
                ],
                "time_taken_ms": data["time_taken"] * 1000
            }
    except httpx.RequestError as e:
        raise HTTPException(status_code=503, detail=f"External search service unavailable: {str(e)}")
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=f"Search service error: {e.response.text}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Semantic search failed: {str(e)}")
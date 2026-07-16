from fastapi import APIRouter, Query, HTTPException
from typing import Annotated
from fairscape_mds.crud.search import FairscapeSearchRequest
from fairscape_mds.models.search import SearchResults, SearchResultItem
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
    query: Annotated[str, Query(description="The search query string.")],
    limit: Annotated[int, Query(ge=1, le=200, description="Maximum number of results to return.")] = 50,
    offset: Annotated[int, Query(ge=0, description="Number of results to skip, for pagination.")] = 0
):
    if not query:
        raise HTTPException(status_code=400, detail="Query parameter cannot be empty.")

    response = search_request_handler.basic_search(query_string=query, limit=limit, offset=offset)
    if response.success:
        return response.model
    else:
        raise HTTPException(status_code=response.statusCode, detail=response.error)

@router.post("/backfill-summaries", summary="Backfill contentSummary for RO-Crates that predate it")
def backfill_summaries_route():
    """Rebuild the stored contentSummary (from metadata.hasPart) for every
    RO-Crate identifier missing one, so pre-contentSummary crates rank and
    label correctly (Release vs RO-Crate) in basic search. Idempotent."""
    response = search_request_handler.backfill_content_summaries()
    if response.success:
        return response.model
    else:
        raise HTTPException(status_code=response.statusCode, detail=response.error)


@router.get("/semantic", response_model=SearchResults, summary="Perform a semantic search")
async def semantic_search_route(
    query: Annotated[str, Query(description="The search query string.")],
    limit: Annotated[int, Query(ge=1, le=200, description="Maximum number of results to return.")] = 50
):
    if not query:
        raise HTTPException(status_code=400, detail="Query parameter cannot be empty.")

    params = {"query": query, "n_results": limit}
    if appConfig.semanticSearchCollection:
        params["collection"] = appConfig.semanticSearchCollection

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{appConfig.semanticSearchUrl}/api/search/semantic",
                params=params,
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
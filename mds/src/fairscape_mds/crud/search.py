import re
import time
from typing import List

from fairscape_mds.crud.fairscape_request import FairscapeRequest
from fairscape_mds.crud.fairscape_response import FairscapeResponse
from fairscape_mds.core.config import FairscapeConfig
from mds.src.fairscape_mds.models.search import SearchResultItem, SearchResults


class FairscapeSearchRequest(FairscapeRequest):
    def __init__(self, config: FairscapeConfig):
        super().__init__(config)

    def basic_search(self, query_string: str, limit: int = 10) -> FairscapeResponse:
        start_time = time.time()
        
        if not query_string:
            return FairscapeResponse(
                success=False,
                statusCode=400,
                error={"message": "Query string cannot be empty."}
            )

        # Case-insensitive regex pattern
        pattern = re.compile(f".*{re.escape(query_string)}.*", re.IGNORECASE)

        try:
            # Search in MongoDB's identifierCollection
            # Looking into the 'metadata' subdocument for name, description, and keywords
            results_cursor = self.config.identifierCollection.find(
                {
                    "$or": [
                        {"metadata.name": pattern},
                        {"metadata.description": pattern},
                        {"metadata.keywords": pattern}  # Works if keywords is an array of strings or a single string
                    ]
                },
                # Projection to get necessary fields
                projection={"_id": False, "@id": True, "@type": True, "metadata.name": True, "metadata.description": True, "metadata.keywords": True}
            ).limit(limit)

            search_results_list: List[SearchResultItem] = []
            
            raw_results = list(results_cursor) # Consume cursor to get all results

            for i, doc in enumerate(raw_results):
                metadata = doc.get("metadata", {})
                
                # Handle keywords, ensuring it's always a list
                keywords = metadata.get("keywords", [])
                if keywords:
                    if not isinstance(keywords, list):
                        keywords = [keywords]
                else:
                    keywords = []

                data_for_validation = {
                    "@id": doc.get("@id", "N/A"), 
                    "@type": doc.get("@type"),    
                    "name": metadata.get("name"),
                    "description": metadata.get("description"),
                    "keywords": keywords,
                    "score": 1
                }
                search_item = SearchResultItem.model_validate(data_for_validation)
                search_results_list.append(search_item)

            end_time = time.time()
            time_taken_ms = (end_time - start_time) * 1000

            search_response_model = SearchResults(
                query=query_string,
                total_results=len(search_results_list), # This is total returned, not total in DB
                results=search_results_list,
                time_taken_ms=time_taken_ms
            )

            return FairscapeResponse(
                success=True,
                statusCode=200,
                model=search_response_model
            )

        except Exception as e:
            return FairscapeResponse(
                success=False,
                statusCode=500,
                error={"message": f"Search failed: {str(e)}"}
            )
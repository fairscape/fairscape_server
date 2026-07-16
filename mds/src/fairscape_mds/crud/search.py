import re
import time
from typing import List

from fairscape_mds.crud.fairscape_request import FairscapeRequest
from fairscape_mds.crud.fairscape_response import FairscapeResponse
from fairscape_mds.core.config import FairscapeConfig
from fairscape_mds.models.search import SearchResultItem, SearchResults
from fairscape_mds.models.rocrate import (
    ROCrateContentSummary,
    ContentSummaryItem,
    ContentCounts,
)


def _categorize(doc) -> tuple:
    """Display label and sort rank for a result: Releases first, then
    RO-Crates, then Datasets, then everything else. A release is an RO-Crate
    whose contentSummary reports nested sub-crates."""
    doc_type = doc.get("@type")
    if isinstance(doc_type, list):
        type_string = " ".join(str(t) for t in doc_type)
    else:
        type_string = str(doc_type or "")
    if "ROCrate" in type_string:
        counts = (doc.get("contentSummary") or {}).get("counts") or {}
        if counts.get("rocrates"):
            return "Release", 0
        return "RO-Crate", 1
    if "Dataset" in type_string:
        return "Dataset", 2
    # Friendly label from the most specific type,
    # e.g. "https://w3id.org/EVI#Software" -> "Software"
    raw_types = doc_type if isinstance(doc_type, list) else [doc_type]
    last = str(raw_types[-1]) if raw_types and raw_types[-1] else ""
    label = last.split("#")[-1].split("/")[-1] or "Other"
    return label, 3


def _summary_from_has_part(has_part: list) -> ROCrateContentSummary:
    """Rebuild a contentSummary from the {@id, @type, name} element list stored
    in metadata.hasPart, mirroring buildContentSummary's categorization."""
    rocrates, datasets, software, computations = [], [], [], []
    schemas, samples, ml_models, other = [], [], [], []

    for elem in has_part:
        if not isinstance(elem, dict):
            continue
        elem_type = elem.get("@type")
        if isinstance(elem_type, list):
            type_str = str(elem_type[-1]) if elem_type else ""
        else:
            type_str = str(elem_type) if elem_type else ""

        item = ContentSummaryItem.model_validate({
            "@id": elem.get("@id", ""),
            "name": elem.get("name") or "Unnamed",
            "@type": type_str,
        })

        if "ROCrate" in type_str:
            rocrates.append(item)
        elif "Dataset" in type_str:
            datasets.append(item)
        elif "Software" in type_str:
            software.append(item)
        elif "Computation" in type_str:
            computations.append(item)
        elif "Schema" in type_str:
            schemas.append(item)
        elif "Sample" in type_str:
            samples.append(item)
        elif "MLModel" in type_str:
            ml_models.append(item)
        elif "CreativeWork" in type_str:
            continue
        else:
            other.append(item)

    counts = ContentCounts(
        datasets=len(datasets),
        software=len(software),
        computations=len(computations),
        schemas=len(schemas),
        samples=len(samples),
        mlModels=len(ml_models),
        rocrates=len(rocrates),
        other=len(other),
        total=len(datasets) + len(software) + len(computations) +
              len(schemas) + len(samples) + len(ml_models) + len(rocrates) + len(other)
    )

    return ROCrateContentSummary(
        datasets=datasets,
        software=software,
        computations=computations,
        schemas=schemas,
        samples=samples,
        mlModels=ml_models,
        rocrates=rocrates,
        other=other,
        counts=counts,
    )


class FairscapeSearchRequest(FairscapeRequest):
    def __init__(self, config: FairscapeConfig):
        super().__init__(config)

    def backfill_content_summaries(self) -> FairscapeResponse:
        """Build contentSummary for RO-Crate identifiers that predate it, from
        the element list stored in metadata.hasPart. Idempotent: only touches
        docs that have no contentSummary."""
        try:
            cursor = self.config.identifierCollection.find(
                {"@type": {"$regex": "ROCrate"}, "contentSummary": None},
                projection={"_id": True, "@id": True, "metadata.hasPart": True}
            )

            scanned = 0
            backfilled = 0
            skipped_no_has_part = 0
            for doc in cursor:
                scanned += 1
                has_part = (doc.get("metadata") or {}).get("hasPart")
                if not isinstance(has_part, list):
                    skipped_no_has_part += 1
                    continue
                summary = _summary_from_has_part(has_part)
                self.config.identifierCollection.update_one(
                    {"_id": doc["_id"]},
                    {"$set": {"contentSummary": summary.model_dump(mode="json", by_alias=True)}}
                )
                backfilled += 1

            return FairscapeResponse(
                success=True,
                statusCode=200,
                model={
                    "scanned": scanned,
                    "backfilled": backfilled,
                    "skippedNoHasPart": skipped_no_has_part,
                }
            )
        except Exception as e:
            return FairscapeResponse(
                success=False,
                statusCode=500,
                error={"message": f"Backfill failed: {str(e)}"}
            )

    def basic_search(self, query_string: str, limit: int = 50, offset: int = 0) -> FairscapeResponse:
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
                projection={"_id": False, "@id": True, "@type": True, "metadata.name": True, "metadata.description": True, "metadata.keywords": True, "contentSummary.counts.rocrates": True}
            )

            search_results_list: List[SearchResultItem] = []

            # Fetch all matches so Releases/RO-Crates sort to the top before paginating
            raw_results = list(results_cursor)
            total_matches = len(raw_results)
            raw_results.sort(key=lambda doc: _categorize(doc)[1])
            raw_results = raw_results[offset:offset + limit]

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
                    "score": 1,
                    "category": _categorize(doc)[0]
                }
                search_item = SearchResultItem.model_validate(data_for_validation)
                search_results_list.append(search_item)

            end_time = time.time()
            time_taken_ms = (end_time - start_time) * 1000

            search_response_model = SearchResults(
                query=query_string,
                total_results=total_matches,
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
"""Server-side shim for evidence-graph models.

The core Pydantic types (`EvidenceGraph`, `EvidenceNode`, `EvidenceGraphCreate`)
and the pure projection helpers live in `fairscape_graph_tools`. This
module re-exports them straight and keeps the Mongo/Celery-bound
artifacts that only matter server-side:

- `EvidenceGraphBuildRequest` -- Celery task envelope (stays here per the
  Phase 0 decision log in `EVIDENCE_GRAPH_MIGRATION.md`).
- `list_evidence_graphs_from_db` -- Mongo-backed listing helper.

The Phase 1 temporary `EvidenceGraph` subclass (which carried a legacy
Mongo-aware `build_graph` method) was removed in Phase 3 once
`crud/evidence_graph.py` started calling `EvidenceGraphBuilder`.
"""

import datetime
from typing import Any, Dict, Optional

import pymongo
from pydantic import BaseModel, Field

from fairscape_graph_tools.models.evidence_graph import (
    EvidenceGraph,
    EvidenceGraphCreate,
    EvidenceNode,
)

from fairscape_mds.crud.fairscape_response import FairscapeResponse


def list_evidence_graphs_from_db(
    mongo_collection: pymongo.collection.Collection,
) -> FairscapeResponse:
    from fairscape_mds.models.identifier import StoredIdentifier
    try:
        cursor = mongo_collection.find({"@type": "evi:EvidenceGraph"}, {"_id": 0})
        graphs = [StoredIdentifier.model_validate(graph_data) for graph_data in cursor]
        return FairscapeResponse(success=True, statusCode=200, model=graphs)
    except Exception as e:
        return FairscapeResponse(success=False, statusCode=500, error={"message": f"Error listing evidence graphs: {str(e)}"})


class EvidenceGraphBuildRequest(BaseModel):
    guid: str
    task_type: str = Field(default="EvidenceGraphBuild")
    owner_email: str
    naan: str
    postfix: str
    status: str = Field(default="PENDING")
    time_created: datetime.datetime = Field(default_factory=datetime.datetime.utcnow)
    time_started: Optional[datetime.datetime] = None
    time_finished: Optional[datetime.datetime] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[Dict[str, Any]] = None

    class Config:
        populate_by_name = True


__all__ = [
    "EvidenceGraph",
    "EvidenceGraphBuildRequest",
    "EvidenceGraphCreate",
    "EvidenceNode",
    "list_evidence_graphs_from_db",
]

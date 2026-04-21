"""Server-side shim for evidence-graph models.

The core Pydantic types (`EvidenceGraph`, `EvidenceNode`, `EvidenceGraphCreate`)
and the pure projection helpers now live in `fairscape_graph_tools`. This
module re-exports them and keeps the Mongo/Celery-bound artifacts that
only matter server-side:

- `EvidenceGraphBuildRequest` — Celery task envelope (stays here per the
  Phase 0 decision log in `EVIDENCE_GRAPH_MIGRATION.md`).
- `list_evidence_graphs_from_db` — Mongo-backed listing helper.
- `EvidenceGraph.build_graph` — a temporary Mongo-aware method kept on a
  server-side subclass of the shared `EvidenceGraph` so
  `crud/evidence_graph.py` keeps working between Phase 1 and Phase 3.
  Phase 3 will rewire that CRUD to call `EvidenceGraphBuilder` and drop
  this subclass.
"""

import datetime
from typing import Any, Dict, List, Optional

import pymongo
from pydantic import BaseModel, Field

from fairscape_graph_tools.models.evidence_graph import (
    EvidenceGraph as _SharedEvidenceGraph,
    EvidenceGraphCreate,
    EvidenceNode,
)
from fairscape_graph_tools.pipeline.condense import condense_evidence_graph_cache
from fairscape_graph_tools.pipeline.evidence_graph import (
    _build_node_from_cache,
    _extract_referenced_ids,
    _flatten_metadata,
    _get_rocrate_outputs,
    _is_rocrate,
)

from fairscape_mds.crud.fairscape_response import FairscapeResponse


class EvidenceGraph(_SharedEvidenceGraph):
    """Server-side subclass keeping the legacy Mongo-aware `build_graph`.

    Phase 3 removes this subclass once `crud/evidence_graph.py` calls
    `EvidenceGraphBuilder` instead.
    """

    def build_graph(
        self,
        start_node_id: str,
        mongo_collection: pymongo.collection.Collection,
        condense: bool = True,
        condense_threshold: int = 5,
    ):
        graph_dict: Dict[str, Dict] = {}
        output_nodes: List[Dict[str, str]] = []

        start_node = mongo_collection.find_one({"@id": start_node_id}, {"_id": 0})

        if not start_node:
            output_nodes.append({"@id": start_node_id})
            graph_dict[start_node_id] = {"@id": start_node_id, "error": "not found"}
            self.outputs = output_nodes
            self.graph = graph_dict
            return

        start_node = _flatten_metadata(start_node)
        node_cache = {start_node_id: start_node}

        node_type = start_node.get("@type", "")
        start_rocrate_id: Optional[str] = None
        start_rocrate_outputs: Optional[List[Dict]] = None
        if _is_rocrate(node_type):
            rocrate_outputs = _get_rocrate_outputs(start_node)
            start_rocrate_outputs = list(rocrate_outputs) if rocrate_outputs else []
            traversal_outputs = list(start_rocrate_outputs)
            traversal_outputs.append({"@id": start_node_id})
            start_rocrate_id = start_node_id
            if traversal_outputs:
                for output_ref in traversal_outputs:
                    if output_ref.get("@id"):
                        output_nodes.append({"@id": output_ref.get("@id")})
            else:
                output_nodes.append({"@id": start_node_id})
        else:
            output_nodes.append({"@id": start_node_id})

        current_level = {node_id["@id"] for node_id in output_nodes}
        processed_ids: set = set()

        while current_level:
            next_level: set = set()

            ids_to_fetch = current_level - processed_ids
            if ids_to_fetch:
                ids_not_in_cache = [nid for nid in ids_to_fetch if nid not in node_cache]

                if ids_not_in_cache:
                    cursor = mongo_collection.find({"@id": {"$in": ids_not_in_cache}}, {"_id": 0})
                    fetched = {node["@id"]: _flatten_metadata(node) for node in cursor}
                    node_cache.update(fetched)

                    for nid in ids_not_in_cache:
                        if nid not in node_cache:
                            node_cache[nid] = {"@id": nid, "error": "not found"}

                for node_id in ids_to_fetch:
                    if node_id not in processed_ids:
                        processed_ids.add(node_id)
                        node = node_cache.get(node_id)
                        if node and "error" not in node:
                            referenced_ids = _extract_referenced_ids(node)
                            next_level.update(referenced_ids)

            current_level = next_level

        if condense:
            self.condensation_stats = condense_evidence_graph_cache(
                node_cache, condense_threshold
            )

        for output_node in output_nodes:
            output_id = output_node.get("@id")
            if output_id:
                _build_node_from_cache(
                    output_id,
                    node_cache,
                    graph_dict,
                    start_rocrate_id,
                    start_rocrate_outputs,
                )

        self.outputs = output_nodes
        self.graph = graph_dict


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
    condense: bool = Field(default=True)
    condense_threshold: int = Field(default=5)
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

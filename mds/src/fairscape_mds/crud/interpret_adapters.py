"""MongoDB-backed adapters for the fairscape_graph_tools ports.

Four adapters bridge `fairscape_graph_tools`'s port protocols to the
Mongo-and-FastAPI world of mds_python:

  - `MongoGraphSource`    -> `GraphSource`
  - `MongoResultSink`     -> `ResultSink`
  - `MongoTaskTracker`    -> `TaskTracker`
  - `ServerSoftwareFetcher` -> `SoftwareFetcher`

`MongoGraphSource` and `MongoResultSink` serve both the
`Interpreter` (annotated-evidence-graph pipeline) and the
`EvidenceGraphBuilder` (standard evidence-graph pipeline). The two
orchestrators share port contracts, so the same adapter classes cover
both — which is why these types have methods for condensed RO-Crates,
AnnotatedEvidenceGraphs, *and* standard EvidenceGraphs.

The shared package has no knowledge of MongoDB, StoredIdentifier,
Permissions, Celery, or the `/software/download/` endpoint -- all of
that lives here. See `fairscape_graph_tools/ports.py` for the port
contracts this file satisfies.
"""

from __future__ import annotations

import datetime
import logging
from typing import Iterable, List

import httpx

from fairscape_mds.core.config import FairscapeConfig
from fairscape_mds.crud.condensation import FairscapeCondensationRequest
from fairscape_mds.crud.fairscape_request import FairscapeRequest
from fairscape_mds.models.identifier import (
    MetadataTypeEnum,
    PublicationStatusEnum,
    StoredIdentifier,
)
from fairscape_mds.models.user import Permissions

from fairscape_graph_tools.models.annotated_computation import AnnotatedComputation
from fairscape_graph_tools.models.annotated_evidence_graph import AnnotatedEvidenceGraph
from fairscape_graph_tools.models.evidence_graph import EvidenceGraph
from fairscape_graph_tools.pipeline.evidence_graph import (
    _flatten_metadata as _flatten_for_evidence_graph,
)
from fairscape_graph_tools.pipeline.github import (
    MAX_SOFTWARE_BYTES,
    prefetch_software_code,
)

logger = logging.getLogger(__name__)


def _flatten_metadata(doc: dict) -> dict:
    """Unwrap a StoredIdentifier doc to the bare RO-Crate-node shape the
    shared pipeline expects (mirror of condensation.py's helper)."""
    flattened: dict = {}
    if "metadata" in doc and isinstance(doc["metadata"], dict):
        flattened.update(doc["metadata"])
    if "@id" in doc:
        flattened["@id"] = doc["@id"]
    if "@type" in doc and "@type" not in flattened:
        flattened["@type"] = doc["@type"]
    return flattened


def _extract_annotates_id(ann: AnnotatedComputation) -> str:
    """Pull the @id the annotation targets, handling IdentifierValue /
    dict / bare-string field shapes."""
    annotates = ann.annotates
    if hasattr(annotates, "guid"):
        return annotates.guid
    if isinstance(annotates, dict):
        return annotates.get("@id", "")
    return str(annotates)


class MongoGraphSource(FairscapeRequest):
    """GraphSource adapter backed by `identifierCollection`."""

    def __init__(self, config: FairscapeConfig):
        super().__init__(config)
        self._condensation = FairscapeCondensationRequest(config)

    def find_entity(self, ark_id: str) -> dict | None:
        doc = self.flexibleFind(ark_id)
        if not doc:
            return None
        return _flatten_metadata(doc)

    def find_many(self, ark_ids: Iterable[str]) -> dict[str, dict]:
        ids = list(ark_ids)
        if not ids:
            return {}
        cursor = self.config.identifierCollection.find(
            {"@id": {"$in": ids}}, {"_id": 0}
        )
        # Use the pipeline's flatten (lifts `metadata.*` up while keeping
        # sibling storage fields) to stay byte-identical with the
        # pre-refactor shim BFS in `models/evidence_graph.py`. The
        # module-local `_flatten_metadata` strips those sibling fields,
        # which is correct for the interpret pipeline but wrong here.
        return {doc["@id"]: _flatten_for_evidence_graph(doc) for doc in cursor}

    def find_dataset_stats(self, ark_ids: Iterable[str]) -> dict[str, dict]:
        ids = list(ark_ids)
        if not ids:
            return {}
        cursor = self.config.identifierCollection.find(
            {"@id": {"$in": ids}},
            {"@id": 1, "descriptiveStatistics": 1, "splitStatistics": 1},
        )
        stats_cache: dict[str, dict] = {}
        for doc in cursor:
            ds_id = doc.get("@id")
            desc_stats = doc.get("descriptiveStatistics")
            split_stats = doc.get("splitStatistics")
            if ds_id and (desc_stats or split_stats):
                stats_cache[ds_id] = {
                    "descriptiveStatistics": desc_stats or {},
                    "splitStatistics": split_stats or {},
                }
        logger.info(
            f"Pre-fetched dataset statistics: {len(stats_cache)} of {len(ids)} "
            f"datasets have stats"
        )
        return stats_cache

    def build_full_graph(self, rocrate_id: str) -> list[dict]:
        return self._condensation.build_full_graph_for_rocrate(rocrate_id)


class MongoResultSink:
    """ResultSink adapter: writes StoredIdentifier docs + back-pointers."""

    def __init__(
        self,
        config: FairscapeConfig,
        *,
        owner_email: str = "system@fairscape.org",
        owner_groups: list[str] | None = None,
    ):
        self.config = config
        self.owner_email = owner_email
        self.owner_groups = list(owner_groups) if owner_groups else []
        self.last_stats: dict = {}

    def persist_condensed(
        self,
        condensed_id: str,
        condensed_metadata: dict,
        source_rocrate_id: str,
        stats: dict,
    ) -> str:
        self.last_stats = stats or {}
        now = datetime.datetime.utcnow()
        stored_doc = {
            "@id": condensed_id,
            "@type": MetadataTypeEnum.ROCRATE.value,
            "metadata": condensed_metadata,
            "publicationStatus": PublicationStatusEnum.PUBLISHED.value,
            "permissions": {"owner": self.owner_email, "group": None},
            "distribution": None,
            "descriptiveStatistics": {},
            "contentSummary": None,
            "dateCreated": now,
            "dateModified": now,
        }
        self.config.identifierCollection.insert_one(stored_doc)
        self.config.identifierCollection.update_one(
            {"@id": source_rocrate_id},
            {"$set": {"metadata.hasCondensedROCrate": {"@id": condensed_id}}},
        )
        return condensed_id

    def persist_evidence_graph(
        self,
        evidence_graph: EvidenceGraph,
        source_node_id: str,
    ) -> str:
        group = self.owner_groups[0] if self.owner_groups else None
        permissions = Permissions(owner=self.owner_email, group=group)
        now = datetime.datetime.utcnow()

        stored = StoredIdentifier(
            guid=evidence_graph.guid,
            metadataType=MetadataTypeEnum.EVIDENCE_GRAPH,
            metadata=evidence_graph,
            publicationStatus=PublicationStatusEnum.PUBLISHED,
            permissions=permissions,
            distribution=None,
            descriptiveStatistics={},
            dateCreated=now,
            dateModified=now,
        )
        self.config.identifierCollection.insert_one(
            stored.model_dump(by_alias=True, mode="json")
        )
        self.config.identifierCollection.update_one(
            {"@id": source_node_id},
            {"$set": {"metadata.hasEvidenceGraph": {"@id": evidence_graph.guid}}},
        )
        logger.info(f"Stored EvidenceGraph {evidence_graph.guid}")
        return evidence_graph.guid

    def persist_aeg(
        self,
        aeg: AnnotatedEvidenceGraph,
        rocrate_id: str,
        step_annotations: List[AnnotatedComputation],
    ) -> str:
        aeg_id = aeg.guid
        permissions = Permissions(owner=self.owner_email, group="", acl=[])
        now = datetime.datetime.utcnow()

        stored = StoredIdentifier.model_validate({
            "@id": aeg_id,
            "@type": MetadataTypeEnum.ANNOTATED_EVIDENCE_GRAPH.value,
            "metadata": aeg.model_dump(by_alias=True, mode="json"),
            "permissions": permissions.model_dump(),
            "publicationStatus": PublicationStatusEnum.DRAFT,
            "dateCreated": now,
            "dateModified": now,
            "distribution": None,
        })
        self.config.identifierCollection.insert_one(
            stored.model_dump(by_alias=True, mode="json")
        )
        self.config.identifierCollection.update_one(
            {"@id": rocrate_id},
            {"$set": {"metadata.hasAnnotatedEvidenceGraph": {"@id": aeg_id}}},
        )
        for ann in step_annotations:
            comp_id = _extract_annotates_id(ann)
            if comp_id:
                self.config.identifierCollection.update_one(
                    {"@id": comp_id},
                    {"$addToSet": {"metadata.evi:annotatedBy": {"@id": ann.guid}}},
                )
        logger.info(f"Stored AnnotatedEvidenceGraph {aeg_id}")
        return aeg_id


class MongoTaskTracker:
    """TaskTracker adapter: writes to `asyncCollection` for the Celery
    task document identified by `task_guid`."""

    def __init__(self, config: FairscapeConfig, task_guid: str):
        self.config = config
        self.task_guid = task_guid

    def update(self, updates: dict) -> None:
        self.config.asyncCollection.update_one(
            {"guid": self.task_guid},
            {"$set": updates},
        )

    def update_computation_status(self, comp_id: str, updates: dict) -> None:
        set_payload = {f"computation_details.$.{k}": v for k, v in updates.items()}
        self.config.asyncCollection.update_one(
            {"guid": self.task_guid, "computation_details.computation_id": comp_id},
            {"$set": set_payload},
        )

    def increment_completed(self) -> None:
        self.config.asyncCollection.update_one(
            {"guid": self.task_guid},
            {"$inc": {"completed_computations": 1}},
        )

    def push_llm_result(self, label: str, raw_output: dict) -> None:
        self.config.asyncCollection.update_one(
            {"guid": self.task_guid},
            {"$push": {"llm_results": {
                "label": label,
                "timestamp": datetime.datetime.utcnow().isoformat(),
                "output": raw_output,
            }}},
        )


class ServerSoftwareFetcher:
    """SoftwareFetcher adapter for the server.

    Uses the `/software/download/` endpoint with the caller's bearer
    token when the `contentUrl` is Fairscape-hosted (applying the
    configured `baseUrl -> internalUrl` rewrite), and falls back to the
    shared `prefetch_software_code` helper for GitHub / external URLs.
    Never raises -- returns an empty or placeholder string on failure.
    """

    def __init__(self, config: FairscapeConfig, user_token: str = ""):
        self.config = config
        self.user_token = user_token

    def fetch(self, software_node: dict) -> str:
        content_url = software_node.get("contentUrl", "") or ""
        sw_id = software_node.get("@id", "?")

        if "/software/download/" in content_url and self.user_token:
            try:
                internal_url = content_url
                if self.config.internalUrl and self.config.baseUrl:
                    internal_url = content_url.replace(
                        self.config.baseUrl, self.config.internalUrl
                    )
                resp = httpx.get(
                    internal_url,
                    headers={"Authorization": f"Bearer {self.user_token}"},
                    timeout=30.0,
                )
                resp.raise_for_status()
                code = resp.text
                if len(code.encode("utf-8")) > MAX_SOFTWARE_BYTES:
                    code = code[:MAX_SOFTWARE_BYTES] + "\n[...truncated...]"
                logger.info(
                    f"Pre-fetched software {sw_id} from download endpoint: "
                    f"{len(code)} chars"
                )
                return code
            except Exception as e:
                logger.warning(
                    f"Failed to fetch software {sw_id} from download endpoint: {e}"
                )

        code = prefetch_software_code(content_url)
        if not code:
            logger.warning(
                f"Pre-fetched software {sw_id}: returned empty string for "
                f"{content_url!r}"
            )
        else:
            logger.info(f"Pre-fetched software {sw_id}: {len(code)} chars")
        return code

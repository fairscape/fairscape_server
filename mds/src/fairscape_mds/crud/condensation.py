"""
condensation.py -- RO-Crate Graph Condensation for the Fairscape Server

Collects a full provenance graph from MongoDB (resolving cross-crate references
via BFS expansion) and then condenses it by collapsing repetitive provenance
chains into DatasetGroup summary nodes.

All graph-condensation helpers (signature computation, traversal, DatasetGroup
construction, `condense_graph`, etc.) now live in
`fairscape_interpret.pipeline.condense` so the CLI can reuse them. This file
keeps only the MongoDB glue: StoredIdentifier-flattening, BFS across
identifierCollection, and the FairscapeCondensationRequest class.
"""

import datetime
from collections import deque
from typing import Any

from fairscape_mds.crud.fairscape_request import FairscapeRequest
from fairscape_mds.crud.fairscape_response import FairscapeResponse
from fairscape_mds.models.identifier import (
	MetadataTypeEnum,
	StoredIdentifier,
	PublicationStatusEnum,
)
from fairscape_mds.models.user import Permissions

# Pure condensation helpers live in the shared package.
from fairscape_interpret.pipeline.condense import (
	ARK_REF_FIELDS,
	compute_provenance_signature,
	condense_evidence_graph_cache,
	condense_graph,
	create_dataset_group_node,
	traverse_and_condense,
)
from fairscape_interpret.pipeline.graph_utils import (
	EVI_TYPES,
	get_evi_type,
	get_generatedby_ids,
	get_id_list,
	is_computation,
	is_dataset,
	is_rocrate_root,
	is_software,
	make_id_ref,
)


# ---------------------------------------------------------------------------
# MongoDB graph collection (replaces local file merge_additional_crates)
# ---------------------------------------------------------------------------

# Batch size for MongoDB $in queries
_BATCH_SIZE = 500


def _flatten_metadata(doc: dict) -> dict:
	"""Flatten a StoredIdentifier document's metadata sub-dict to top level,
	stripping server-internal StoredIdentifier wrapper fields.

	MongoDB stores entities as {"@id": ..., "@type": ..., "metadata": {...},
	"permissions": ..., "distribution": ..., ...}.
	The condensation algorithm expects clean RO-Crate nodes like
	{"@id": ..., "@type": ..., "name": ..., "generatedBy": [...]}.
	"""
	flattened = {}
	if "metadata" in doc and isinstance(doc["metadata"], dict):
		flattened.update(doc["metadata"])
	if "@id" in doc:
		flattened["@id"] = doc["@id"]
	if "@type" in doc and "@type" not in flattened:
		flattened["@type"] = doc["@type"]
	return flattened


def _extract_all_ark_refs(entity: dict) -> list[str]:
	"""Extract all ARK identifier references from an entity's provenance fields."""
	refs = []
	for field in ARK_REF_FIELDS:
		val = entity.get(field)
		if val is None:
			continue
		if isinstance(val, dict):
			aid = val.get("@id")
			if aid and isinstance(aid, str) and "ark:" in aid:
				refs.append(aid)
		elif isinstance(val, list):
			for item in val:
				if isinstance(item, dict):
					aid = item.get("@id")
					if aid and isinstance(aid, str) and "ark:" in aid:
						refs.append(aid)
				elif isinstance(item, str) and "ark:" in item:
					refs.append(item)
	return refs


# ---------------------------------------------------------------------------
# Main CRUD class
# ---------------------------------------------------------------------------

class FairscapeCondensationRequest(FairscapeRequest):
	"""Handles condensed ROCrate creation, retrieval, and deletion."""

	def build_full_graph_for_rocrate(self, rocrate_id: str) -> list[dict]:
		"""Collect the full provenance graph for an ROCrate from MongoDB.

		Does BFS expansion: starts from the ROCrate's hasPart/outputs and
		recursively resolves all ARK references, even those pointing to
		entities in other crates.

		Returns a flat list[dict] suitable for condense_graph().
		"""
		collected: dict[str, dict | None] = {}
		queue: deque[str] = deque()

		# Seed with the ROCrate root itself
		queue.append(rocrate_id)

		while queue:
			# Drain up to _BATCH_SIZE IDs that we haven't seen yet
			batch: list[str] = []
			while queue and len(batch) < _BATCH_SIZE:
				eid = queue.popleft()
				if eid not in collected:
					batch.append(eid)

			if not batch:
				break

			# Batch fetch from MongoDB
			cursor = self.config.identifierCollection.find(
				{"@id": {"$in": batch}},
				{"_id": 0}
			)

			found_ids: set[str] = set()
			for doc in cursor:
				flat = _flatten_metadata(doc)
				entity_id = flat.get("@id")
				if not entity_id:
					continue
				found_ids.add(entity_id)
				collected[entity_id] = flat

				# Enqueue all referenced ARKs for expansion
				for ref_id in _extract_all_ark_refs(flat):
					if ref_id not in collected:
						queue.append(ref_id)

			# Mark not-found IDs so we don't re-query them
			for eid in batch:
				if eid not in found_ids and eid not in collected:
					collected[eid] = None

		return [v for v in collected.values() if v is not None]

	def condense_rocrate(
		self,
		rocrate_id: str,
		threshold: int = 5,
		max_member_ids: int = 0,
		owner_email: str = "system@fairscape.org",
	) -> FairscapeResponse:
		"""Build the full graph, condense it, store the result.

		Thin wrapper over `fairscape_interpret.Condenser.condense`. The
		Mongo adapters and the shared orchestrator do all the real work;
		this method only maps their exceptions onto the HTTP-shaped
		`FairscapeResponse` the router and Celery worker expect.
		"""
		# Lazy import: `interpret_adapters` imports this class, so a
		# top-level import here would close the cycle at module load.
		from fairscape_mds.crud.interpret_adapters import (
			MongoGraphSource,
			MongoResultSink,
		)
		from fairscape_interpret.condenser import Condenser

		condensed_id = f"{rocrate_id}-condensed"

		# Pre-check preserves the exact 409 error text (no hyphen in
		# "ROCrate") that downstream clients may depend on.
		existing = self.config.identifierCollection.find_one({"@id": condensed_id})
		if existing:
			return FairscapeResponse(
				success=False,
				statusCode=409,
				error={"message": f"Condensed ROCrate {condensed_id} already exists"}
			)

		source = MongoGraphSource(self.config)
		sink = MongoResultSink(self.config, owner_email=owner_email)
		condenser = Condenser(
			source, sink, threshold=threshold, max_member_ids=max_member_ids,
		)

		try:
			persisted_id = condenser.condense(rocrate_id)
		except ValueError as e:
			return FairscapeResponse(
				success=False,
				statusCode=404,
				error={"message": str(e)},
			)
		except Exception as e:
			return FairscapeResponse(
				success=False,
				statusCode=500,
				error={"message": f"Error storing condensed ROCrate: {str(e)}"},
			)

		return FairscapeResponse(
			success=True,
			statusCode=201,
			model={"condensed_id": persisted_id, "stats": sink.last_stats},
		)

	def delete_condensed_rocrate(self, rocrate_id: str) -> FairscapeResponse:
		"""Delete an existing condensed ROCrate and clear the pointer."""
		condensed_id = f"{rocrate_id}-condensed"

		try:
			delete_result = self.config.identifierCollection.delete_one(
				{"@id": condensed_id}
			)

			if delete_result.deleted_count == 0:
				return FairscapeResponse(
					success=False,
					statusCode=404,
					error={"message": f"Condensed ROCrate {condensed_id} not found"}
				)

			self.config.identifierCollection.update_one(
				{"@id": rocrate_id},
				{"$unset": {"metadata.hasCondensedROCrate": ""}}
			)

			return FairscapeResponse(
				success=True,
				statusCode=200,
				model={"message": f"Condensed ROCrate {condensed_id} deleted successfully"}
			)
		except Exception as e:
			return FairscapeResponse(
				success=False,
				statusCode=500,
				error={"message": f"Error deleting condensed ROCrate: {str(e)}"}
			)

"""
condensation.py -- RO-Crate Graph Condensation for the Fairscape Server

Collects a full provenance graph from MongoDB (resolving cross-crate references
via BFS expansion) and then condenses it by collapsing repetitive provenance
chains into DatasetGroup summary nodes.

Ported from fairscape-interpreter/condense_crate.py with a new MongoDB-backed
graph collection layer replacing the local-file reader.
"""

import datetime
from collections import defaultdict, deque
from typing import Any

from fairscape_mds.crud.fairscape_request import FairscapeRequest
from fairscape_mds.crud.fairscape_response import FairscapeResponse
from fairscape_mds.models.identifier import (
	MetadataTypeEnum,
	StoredIdentifier,
	PublicationStatusEnum,
)
from fairscape_mds.models.user import Permissions


# ---------------------------------------------------------------------------
# Type detection helpers (operate on raw JSON dicts)
# ---------------------------------------------------------------------------

EVI_TYPES = {
	"Dataset", "Software", "MLModel", "Computation", "Annotation",
	"Experiment", "ROCrate", "CreativeWork", "Schema",
}


def _extract_short_types(node: dict) -> list[str]:
	"""Extract all short EVI type names from a node's @type field."""
	raw = node.get("@type", [])
	if isinstance(raw, str):
		raw = [raw]
	shorts = []
	for t in raw:
		short = t.split("#")[-1] if "#" in t else t.split(":")[-1] if ":" in t else t
		if short in EVI_TYPES:
			shorts.append(short)
	return shorts


def get_evi_type(node: dict) -> str | None:
	"""Extract the primary EVI type from a node's @type field."""
	shorts = _extract_short_types(node)
	if not shorts:
		return None
	for preferred in ("ROCrate", "Computation", "Software", "MLModel",
					  "Experiment", "Annotation", "Schema"):
		if preferred in shorts:
			return preferred
	return shorts[0]


def is_dataset(node: dict) -> bool:
	types = _extract_short_types(node)
	return "Dataset" in types and "ROCrate" not in types


def is_computation(node: dict) -> bool:
	return get_evi_type(node) == "Computation"


def is_software(node: dict) -> bool:
	return get_evi_type(node) == "Software"


def is_rocrate_root(node: dict) -> bool:
	return "ROCrate" in _extract_short_types(node)


# ---------------------------------------------------------------------------
# Reference helpers
# ---------------------------------------------------------------------------

def get_id_list(node: dict, *fields) -> list[str]:
	"""Extract a list of @id strings from one or more reference fields."""
	ids = []
	for field in fields:
		val = node.get(field, [])
		if val is None:
			continue
		if isinstance(val, dict):
			val = [val]
		if isinstance(val, list):
			for item in val:
				if isinstance(item, dict) and "@id" in item:
					ids.append(item["@id"])
				elif isinstance(item, str):
					ids.append(item)
	return ids


def get_generatedby_ids(dataset: dict) -> list[str]:
	"""Get the @ids of computations that generated this dataset."""
	return get_id_list(dataset, "generatedBy", "prov:wasGeneratedBy")


def make_id_ref(entity_id: str) -> dict:
	"""Create a {"@id": ...} reference."""
	return {"@id": entity_id}


# ---------------------------------------------------------------------------
# Provenance Signature
# ---------------------------------------------------------------------------

def compute_provenance_signature(
	dataset_id: str,
	index: dict[str, dict],
	cache: dict[str, tuple],
) -> tuple:
	"""
	Compute a hashable signature for a dataset's provenance structure.

	Two datasets with the same signature went through identical software
	pipelines, regardless of which specific data/computation instances
	were involved.
	"""
	if dataset_id in cache:
		return cache[dataset_id]

	dataset = index.get(dataset_id)
	if dataset is None:
		sig = ("unknown", (), None)
		cache[dataset_id] = sig
		return sig

	fmt = dataset.get("format", "unknown")
	schema_ids = tuple(sorted(get_id_list(dataset, "evi:Schema")))

	gen_comp_ids = get_generatedby_ids(dataset)

	if not gen_comp_ids:
		sig = (fmt, schema_ids, None)
	else:
		comp_sigs = []
		for comp_id in sorted(gen_comp_ids):
			comp = index.get(comp_id)
			if comp is None:
				comp_sigs.append(((), ()))
				continue

			sw_ids = tuple(sorted(get_id_list(comp, "usedSoftware")))

			input_dataset_ids = get_id_list(comp, "usedDataset")
			input_sigs = tuple(sorted(
				compute_provenance_signature(ds_id, index, cache)
				for ds_id in input_dataset_ids
			))

			comp_sigs.append((sw_ids, input_sigs))

		sig = (fmt, schema_ids, tuple(sorted(comp_sigs)))

	cache[dataset_id] = sig
	return sig


# ---------------------------------------------------------------------------
# Output-first backward traversal with inline condensation
# ---------------------------------------------------------------------------

def traverse_and_condense(
	index: dict[str, dict],
	threshold: int,
	max_member_ids: int = 0,
) -> tuple[set[str], set[str], list[dict], dict[str, list[tuple[list[str], str]]]]:
	"""
	Traverse backward from the primary crate's outputs, keeping everything
	reachable and collapsing large groups of sibling datasets that share the
	same provenance signature.

	Returns:
		keep_ids, remove_ids, group_nodes, comp_updates
	"""
	# Find root crate node
	root_id = None
	for nid, node in index.items():
		if is_rocrate_root(node):
			root_id = nid
			break

	if root_id is None:
		return set(index.keys()), set(), [], {}

	root = index[root_id]

	output_ids = get_id_list(root, "EVI:outputs")
	part_ids = get_id_list(root, "hasPart", "EVI:outputs")

	keep_ids: set[str] = {root_id, "ro-crate-metadata.json"}
	collapsed_ids: set[str] = set()
	group_nodes: list[dict] = []
	comp_updates: dict[str, list[tuple[list[str], str]]] = defaultdict(list)
	sig_cache: dict[str, tuple] = {}

	# Pre-pass: condense repetitive top-level outputs
	output_dataset_ids = [
		oid for oid in output_ids
		if oid in index and is_dataset(index[oid])
	]
	if len(output_dataset_ids) > threshold:
		sig_to_ids: dict[tuple, list[str]] = defaultdict(list)
		for ds_id in output_dataset_ids:
			sig = compute_provenance_signature(ds_id, index, sig_cache)
			sig_to_ids[sig].append(ds_id)

		for sig, member_ids in sig_to_ids.items():
			if len(member_ids) > threshold:
				representative_id = sorted(member_ids)[0]
				non_rep_ids = [mid for mid in member_ids
							   if mid != representative_id]
				collapsed_ids.update(non_rep_ids)

				for mid in non_rep_ids:
					_collect_exclusive_backward(
						mid, representative_id, index, collapsed_ids,
					)

				group = {
					"consuming_comp_id": root_id,
					"signature": sig,
					"member_ids": member_ids,
					"representative_id": representative_id,
				}
				group_node = create_dataset_group_node(group, index, max_member_ids)
				group_nodes.append(group_node)
				comp_updates[root_id].append(
					(member_ids, group_node["@id"])
				)

	# Backward traversal
	visited: set[str] = set()
	stack = list(part_ids)

	while stack:
		current_id = stack.pop()
		if current_id in visited or current_id in collapsed_ids:
			continue
		visited.add(current_id)
		keep_ids.add(current_id)

		node = index.get(current_id)
		if node is None:
			continue

		evi_type = get_evi_type(node)

		if is_dataset(node):
			for comp_id in get_generatedby_ids(node):
				stack.append(comp_id)
			for schema_id in get_id_list(node, "evi:Schema"):
				stack.append(schema_id)

		elif evi_type == "Computation":
			for sw_id in get_id_list(node, "usedSoftware"):
				keep_ids.add(sw_id)
				stack.append(sw_id)

			for ml_id in get_id_list(node, "usedMLModel"):
				keep_ids.add(ml_id)
				stack.append(ml_id)

			input_ids = get_id_list(node, "usedDataset")

			if len(input_ids) > threshold:
				sig_to_ids = defaultdict(list)
				for ds_id in input_ids:
					sig = compute_provenance_signature(ds_id, index, sig_cache)
					sig_to_ids[sig].append(ds_id)

				for sig, member_ids in sig_to_ids.items():
					if len(member_ids) > threshold:
						representative_id = sorted(member_ids)[0]
						non_rep_ids = [mid for mid in member_ids
									   if mid != representative_id]
						collapsed_ids.update(non_rep_ids)

						for mid in non_rep_ids:
							_collect_exclusive_backward(
								mid, representative_id, index, collapsed_ids,
							)

						group = {
							"consuming_comp_id": current_id,
							"signature": sig,
							"member_ids": member_ids,
							"representative_id": representative_id,
						}
						group_node = create_dataset_group_node(group, index, max_member_ids)
						group_nodes.append(group_node)
						comp_updates[current_id].append(
							(member_ids, group_node["@id"])
						)

						stack.append(representative_id)
					else:
						for ds_id in member_ids:
							stack.append(ds_id)
			else:
				for ds_id in input_ids:
					stack.append(ds_id)

			for out_id in get_id_list(node, "generated"):
				stack.append(out_id)

		elif evi_type == "Experiment":
			for ref_id in get_id_list(node, "usedSample", "usedInstrument",
									  "usedTreatment", "usedStain"):
				stack.append(ref_id)

		elif is_software(node):
			pass

	# Keep all software nodes
	for nid, n in index.items():
		if is_software(n):
			keep_ids.add(nid)

	keep_ids -= collapsed_ids

	return keep_ids, collapsed_ids, group_nodes, comp_updates


def _collect_exclusive_backward(
	dataset_id: str,
	representative_id: str,
	index: dict[str, dict],
	collapsed: set[str],
) -> None:
	"""
	Collect backward chain entities of a non-representative dataset that are
	NOT shared with the representative's chain. Add them to collapsed set.
	"""
	rep_chain = _collect_backward_chain(representative_id, index)
	stack = [dataset_id]
	visited = set()

	while stack:
		cid = stack.pop()
		if cid in visited:
			continue
		visited.add(cid)

		if cid in rep_chain:
			continue

		node = index.get(cid)
		if node is None:
			continue

		if is_software(node):
			continue

		collapsed.add(cid)

		if is_dataset(node):
			for comp_id in get_generatedby_ids(node):
				stack.append(comp_id)

		if is_computation(node):
			for ref_id in get_id_list(node, "usedDataset"):
				stack.append(ref_id)


def _collect_backward_chain(dataset_id: str, index: dict[str, dict]) -> set[str]:
	"""Collect all entity @ids in the backward provenance chain."""
	visited = set()
	stack = [dataset_id]
	while stack:
		cid = stack.pop()
		if cid in visited:
			continue
		visited.add(cid)
		node = index.get(cid)
		if node is None:
			continue
		if is_dataset(node):
			for comp_id in get_generatedby_ids(node):
				stack.append(comp_id)
		if is_computation(node):
			for ref_id in get_id_list(node, "usedDataset", "usedSoftware",
									  "usedMLModel"):
				stack.append(ref_id)
	return visited


# ---------------------------------------------------------------------------
# Build condensed graph
# ---------------------------------------------------------------------------

def create_dataset_group_node(
	group: dict,
	index: dict[str, dict],
	max_member_ids: int = 0,
) -> dict:
	"""Create a DatasetGroup summary node for a group of similar datasets."""
	representative = index[group["representative_id"]]
	member_ids = group["member_ids"]
	count = len(member_ids)
	sig = group["signature"]

	common_sw_ids = []
	if sig[2]:
		for comp_sig in sig[2]:
			sw_ids, _ = comp_sig
			common_sw_ids.extend(sw_ids)
	common_sw_ids = sorted(set(common_sw_ids))

	fmt = sig[0]

	consuming_node = index.get(group["consuming_comp_id"], {})
	if is_rocrate_root(consuming_node):
		crate_name = consuming_node.get("name", "unknown").lower().replace(" ", "-")
		group_id = f"ark:group/{crate_name}-{fmt.replace('/', '_').lstrip('.')}-outputs"
	else:
		comp_name = consuming_node.get("name", "unknown").lower().replace(" ", "-")
		group_id = f"ark:group/{comp_name}-{fmt.replace('/', '_').lstrip('.')}-inputs"

	sw_names = []
	for sw_id in common_sw_ids:
		sw = index.get(sw_id, {})
		sw_names.append(sw.get("name", sw_id))

	description = f"{count} {fmt} files with identical provenance structure."
	if sw_names:
		description += f" All processed by {', '.join(sw_names)}."

	schema_ids = list(sig[1]) if sig[1] else []

	node = {
		"@id": group_id,
		"@type": ["prov:Entity", "https://w3id.org/EVI#DatasetGroup"],
		"name": f"{representative.get('name', fmt + ' files')} (and {count - 1} similar)",
		"description": description,
		"format": fmt,
		"evi:memberCount": count,
		"evi:representativeDataset": make_id_ref(group["representative_id"]),
		"evi:commonFormat": fmt,
		"evi:commonSoftware": [make_id_ref(sw_id) for sw_id in common_sw_ids],
		"evi:provenanceSignature": str(sig),
		"evi:memberIds": _truncate_member_ids(sorted(member_ids), max_member_ids),
	}

	if schema_ids:
		node["evi:commonSchema"] = [make_id_ref(sid) for sid in schema_ids]

	return node


def _truncate_member_ids(ids: list[str], max_ids: int) -> list[str]:
	"""Truncate member ID list if max_ids > 0, appending a summary entry."""
	if max_ids <= 0 or len(ids) <= max_ids:
		return ids
	excluded = len(ids) - max_ids
	return ids[:max_ids] + [f"... and {excluded} more (total: {len(ids)})"]


def condense_graph(
	graph: list[dict],
	threshold: int,
	max_member_ids: int = 0,
) -> tuple[list[dict], dict]:
	"""
	Condense an RO-Crate @graph by collapsing repetitive provenance.

	Returns (condensed_graph, stats).
	"""
	index: dict[str, dict] = {}
	for node in graph:
		node_id = node.get("@id")
		if node_id:
			index[node_id] = node

	original_count = len(graph)

	keep_ids, collapsed_ids, group_nodes, comp_updates = \
		traverse_and_condense(index, threshold, max_member_ids)

	if not group_nodes:
		stats = {
			"condensed": False,
			"originalEntityCount": original_count,
			"condensedEntityCount": original_count,
			"datasetGroupCount": 0,
			"note": "No repetitive provenance found above threshold.",
		}
		return graph, stats

	new_graph = []
	for node in graph:
		node_id = node.get("@id")

		if node_id not in keep_ids:
			continue

		if node_id in comp_updates:
			node = dict(node)
			for member_ids, group_id in comp_updates[node_id]:
				member_set = set(member_ids)

				if "usedDataset" in node and node["usedDataset"]:
					kept = [ref for ref in node["usedDataset"]
							if ref.get("@id") not in member_set]
					kept.append(make_id_ref(group_id))
					node["usedDataset"] = kept

				if "prov:used" in node and node["prov:used"]:
					kept = [ref for ref in node["prov:used"]
							if ref.get("@id") not in member_set]
					kept.append(make_id_ref(group_id))
					node["prov:used"] = kept

		if is_rocrate_root(node):
			node = dict(node)
			condensed_count = len(keep_ids) + len(group_nodes)
			node["evi:condensed"] = True
			node["evi:condensationThreshold"] = threshold
			node["evi:condensationDate"] = str(datetime.date.today())
			node["evi:originalEntityCount"] = original_count
			node["evi:condensedEntityCount"] = condensed_count
			node["evi:datasetGroupCount"] = len(group_nodes)

			total_collapsed = len(collapsed_ids)
			node["evi:condensationNote"] = (
				f"Condensed from {original_count} entities to "
				f"{condensed_count}. "
				f"{len(group_nodes)} dataset group(s) created by collapsing "
				f"{total_collapsed} datasets with identical provenance signatures "
				f"(same software chain). Full member lists preserved in evi:memberIds."
			)

			if "hasPart" in node and node["hasPart"]:
				kept = [ref for ref in node["hasPart"]
						if ref.get("@id") not in collapsed_ids]
				for gn in group_nodes:
					kept.append(make_id_ref(gn["@id"]))
				node["hasPart"] = kept

			if node_id in comp_updates:
				for member_ids, group_id in comp_updates[node_id]:
					member_set = set(member_ids)
					if "EVI:outputs" in node and node["EVI:outputs"]:
						kept = [ref for ref in node["EVI:outputs"]
								if ref.get("@id") not in member_set]
						kept.append(make_id_ref(group_id))
						node["EVI:outputs"] = kept

		new_graph.append(node)

	new_graph.extend(group_nodes)

	# Clean up dangling references
	final_ids = {n.get("@id") for n in new_graph if "@id" in n}
	ref_fields = ("usedDataset", "usedSoftware", "usedMLModel", "generated",
				  "hasPart", "prov:used", "generatedBy", "prov:wasGeneratedBy",
				  "derivedFrom", "prov:wasDerivedFrom", "usedByComputation",
				  "isPartOf", "EVI:outputs")

	for i, node in enumerate(new_graph):
		modified = False
		node_copy = None
		for field in ref_fields:
			val = node.get(field)
			if val is None:
				continue
			if isinstance(val, dict) and "@id" in val:
				if val["@id"] not in final_ids:
					if not modified:
						node_copy = dict(node)
						modified = True
					node_copy[field] = []
			elif isinstance(val, list):
				cleaned = [ref for ref in val
						   if not (isinstance(ref, dict) and "@id" in ref
								   and ref["@id"] not in final_ids)]
				if len(cleaned) != len(val):
					if not modified:
						node_copy = dict(node)
						modified = True
					node_copy[field] = cleaned
		if modified:
			new_graph[i] = node_copy

	condensed_count = len(new_graph)
	groups_info = []
	for gn in group_nodes:
		groups_info.append({
			"memberCount": gn["evi:memberCount"],
			"format": gn["format"],
			"groupId": gn["@id"],
		})

	stats = {
		"condensed": True,
		"originalEntityCount": original_count,
		"condensedEntityCount": condensed_count,
		"datasetGroupCount": len(group_nodes),
		"entitiesRemoved": len(collapsed_ids),
		"groups": groups_info,
	}

	return new_graph, stats


# ---------------------------------------------------------------------------
# MongoDB graph collection (replaces local file merge_additional_crates)
# ---------------------------------------------------------------------------

# Fields that contain ARK references to other entities
_ARK_REF_FIELDS = (
	"hasPart", "EVI:outputs", "outputs",
	"generatedBy", "prov:wasGeneratedBy",
	"usedDataset", "usedSoftware", "usedMLModel",
	"derivedFrom", "prov:wasDerivedFrom",
	"evi:Schema",
	"usedSample", "usedInstrument", "usedTreatment", "usedStain",
	"generated", "prov:used",
	"usedByComputation",
	"isPartOf",
)

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
	# Start with the metadata sub-dict (the actual RO-Crate fields)
	if "metadata" in doc and isinstance(doc["metadata"], dict):
		flattened.update(doc["metadata"])
	# Ensure @id and @type are present (from top-level StoredIdentifier)
	if "@id" in doc:
		flattened["@id"] = doc["@id"]
	if "@type" in doc and "@type" not in flattened:
		flattened["@type"] = doc["@type"]
	return flattened


def _extract_all_ark_refs(entity: dict) -> list[str]:
	"""Extract all ARK identifier references from an entity's provenance fields."""
	refs = []
	for field in _ARK_REF_FIELDS:
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
		"""Build the full graph, condense it, and store the result.

		Returns a FairscapeResponse with the condensed ROCrate's StoredIdentifier.
		"""
		condensed_id = f"{rocrate_id}-condensed"

		# Check if already exists
		existing = self.config.identifierCollection.find_one({"@id": condensed_id})
		if existing:
			return FairscapeResponse(
				success=False,
				statusCode=409,
				error={"message": f"Condensed ROCrate {condensed_id} already exists"}
			)

		# Collect the full graph from MongoDB
		graph = self.build_full_graph_for_rocrate(rocrate_id)
		if not graph:
			return FairscapeResponse(
				success=False,
				statusCode=404,
				error={"message": f"No metadata found for RO-Crate {rocrate_id}"}
			)

		# Run condensation
		condensed_graph, stats = condense_graph(graph, threshold, max_member_ids)

		if not stats.get("condensed"):
			return FairscapeResponse(
				success=True,
				statusCode=200,
				model={"message": "Nothing to condense", "stats": stats}
			)

		# Find context from the original ROCrate root doc
		rocrate_doc = self.config.identifierCollection.find_one(
			{"@id": rocrate_id}, {"_id": 0}
		)
		rocrate_name = "Unknown RO-Crate"
		if rocrate_doc:
			meta = rocrate_doc.get("metadata", {})
			if isinstance(meta, dict):
				rocrate_name = meta.get("name", rocrate_doc.get("name", rocrate_id))
			else:
				rocrate_name = rocrate_doc.get("name", rocrate_id)

		# Build the condensed ROCrate as a proper RO-Crate structure:
		# @graph[0] = ROCrateMetadataFileElem ("about" this crate)
		# @graph[1] = ROCrateMetadataElem (the root crate node — already in condensed_graph)
		# @graph[2..] = rest of condensed entities

		# The condensed_graph already contains the root ROCrateMetadataElem
		# (with evi:condensed=True set by condense_graph). We need to:
		# 1. Find it and update its @id to the condensed_id
		# 2. Prepend the ROCrateMetadataFileElem

		# Find the root crate element in the condensed graph
		root_idx = None
		for idx, node in enumerate(condensed_graph):
			if is_rocrate_root(node):
				root_idx = idx
				break

		if root_idx is not None:
			# Update the root element with condensed-specific metadata
			root_node = dict(condensed_graph[root_idx])
			root_node["evi:sourceROCrate"] = {"@id": rocrate_id}
			root_node["evi:condensationStats"] = stats
			# Keep original @id so provenance refs stay valid,
			# but add the condensed_id as an alternate
			condensed_graph[root_idx] = root_node

		# Build the ro-crate-metadata.json file descriptor element
		file_elem = {
			"@id": "ro-crate-metadata.json",
			"@type": "CreativeWork",
			"conformsTo": {"@id": "https://w3id.org/ro/crate/1.2-DRAFT"},
			"about": {"@id": rocrate_id},
		}

		# Assemble the @graph: file descriptor first, then rest
		# Remove the root from its current position and place it second
		ordered_graph = [file_elem]
		if root_idx is not None:
			ordered_graph.append(condensed_graph[root_idx])
		for idx, node in enumerate(condensed_graph):
			if idx == root_idx:
				continue
			# Skip any existing file descriptor from the source graph
			if node.get("@id") == "ro-crate-metadata.json":
				continue
			ordered_graph.append(node)

		condensed_metadata = {
			"@context": {"@vocab": "https://schema.org/"},
			"@graph": ordered_graph,
		}

		now = datetime.datetime.utcnow()
		stored_doc = {
			"@id": condensed_id,
			"@type": MetadataTypeEnum.ROCRATE.value,
			"metadata": condensed_metadata,
			"publicationStatus": PublicationStatusEnum.PUBLISHED.value,
			"permissions": {
				"owner": owner_email,
				"group": None,
			},
			"distribution": None,
			"descriptiveStatistics": {},
			"contentSummary": None,
			"dateCreated": now,
			"dateModified": now,
		}

		try:
			self.config.identifierCollection.insert_one(stored_doc)

			# Update original ROCrate with pointer
			self.config.identifierCollection.update_one(
				{"@id": rocrate_id},
				{"$set": {"metadata.hasCondensedROCrate": {"@id": condensed_id}}}
			)

			return FairscapeResponse(
				success=True,
				statusCode=201,
				model={"condensed_id": condensed_id, "stats": stats}
			)
		except Exception as e:
			return FairscapeResponse(
				success=False,
				statusCode=500,
				error={"message": f"Error storing condensed ROCrate: {str(e)}"}
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

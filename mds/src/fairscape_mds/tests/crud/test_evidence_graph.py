"""Mongomock-backed tests for ``crud/evidence_graph.py``.

These cover the thin Phase-3 CRUD wrapper around ``EvidenceGraphBuilder``:

- ``build_evidence_graph_for_node``: 404 on missing source node,
  idempotent 200 on a valid pre-existing back-pointer, fresh 201 on
  first build, stale back-pointer falls through to rebuild, 409 on a
  duplicate-key race.
- quick smokes for ``create_evidence_graph`` / ``get_evidence_graph`` /
  ``delete_evidence_graph`` so the un-refactored methods stay green.

The byte-identical fixture-crate regression (server-side Phase 3
acceptance) is *not* done here -- mongomock only validates structural
correctness, not that the @graph shape matches a pre-refactor snapshot.
That regression needs a real Mongo + fixture and is tracked in
``EVIDENCE_GRAPH_MIGRATION.md``.
"""

from __future__ import annotations

import datetime

import mongomock
import pytest
from unittest.mock import MagicMock

from fairscape_mds.crud.evidence_graph import FairscapeEvidenceGraphRequest
from fairscape_mds.models.evidence_graph import EvidenceGraphCreate
from fairscape_mds.models.user import UserWriteModel


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _mongomock_config() -> MagicMock:
    cfg = MagicMock()
    client = mongomock.MongoClient()
    db = client["fairscape_test"]
    cfg.identifierCollection = db["identifier"]
    cfg.asyncCollection = db["async"]
    cfg.baseUrl = None
    cfg.internalUrl = None
    return cfg


def _user(email: str = "alice@fairscape.org", groups=("team-alpha",)) -> UserWriteModel:
    return UserWriteModel.model_validate({
        "email": email,
        "firstName": "Alice",
        "lastName": "Anderson",
        "password": "pw",
        "@type": "Person",
        "groups": list(groups),
    })


def _insert_source_node(cfg, *, naan: str, postfix: str,
                        extra_metadata: dict | None = None) -> str:
    """Insert a minimal Dataset node as a StoredIdentifier doc. Returns
    the node @id. The evidence-graph builder's BFS will start here."""
    node_id = f"ark:{naan}/{postfix}"
    metadata = {
        "@id": node_id,
        "@type": "Dataset",
        "name": postfix,
        "description": "source node for build_evidence_graph_for_node",
    }
    if extra_metadata:
        metadata.update(extra_metadata)
    now = datetime.datetime.utcnow()
    cfg.identifierCollection.insert_one({
        "@id": node_id,
        "@type": "Dataset",
        "metadata": metadata,
        "permissions": {"owner": "alice@fairscape.org", "group": None},
        "distribution": None,
        "dateCreated": now,
        "dateModified": now,
    })
    return node_id


def _insert_chain(cfg, *, naan: str, postfix: str) -> None:
    """Insert a Dataset-with-generatedBy -> Computation -> Dataset chain
    rooted at `ark:{naan}/{postfix}` so BFS has something to traverse."""
    now = datetime.datetime.utcnow()
    nodes = [
        {
            "@id": f"ark:{naan}/{postfix}",
            "@type": "Dataset",
            "metadata": {
                "@id": f"ark:{naan}/{postfix}",
                "@type": "Dataset",
                "name": "output",
                "description": "x",
                "generatedBy": {"@id": f"ark:{naan}/comp"},
            },
        },
        {
            "@id": f"ark:{naan}/comp",
            "@type": "Computation",
            "metadata": {
                "@id": f"ark:{naan}/comp",
                "@type": "Computation",
                "name": "c",
                "usedDataset": [{"@id": f"ark:{naan}/input"}],
                "usedSoftware": [{"@id": f"ark:{naan}/sw"}],
            },
        },
        {
            "@id": f"ark:{naan}/input",
            "@type": "Dataset",
            "metadata": {
                "@id": f"ark:{naan}/input",
                "@type": "Dataset",
                "name": "input",
            },
        },
        {
            "@id": f"ark:{naan}/sw",
            "@type": "Software",
            "metadata": {
                "@id": f"ark:{naan}/sw",
                "@type": "Software",
                "name": "sw",
            },
        },
    ]
    for n in nodes:
        n["permissions"] = {"owner": "alice@fairscape.org", "group": None}
        n["distribution"] = None
        n["dateCreated"] = now
        n["dateModified"] = now
    cfg.identifierCollection.insert_many(nodes)


# ---------------------------------------------------------------------------
# build_evidence_graph_for_node
# ---------------------------------------------------------------------------


class TestBuildEvidenceGraphForNode:
    def test_missing_source_node_returns_404(self):
        cfg = _mongomock_config()
        req = FairscapeEvidenceGraphRequest(cfg)
        resp = req.build_evidence_graph_for_node(
            _user(), naan="59999", postfix="ghost",
        )
        assert resp.success is False
        assert resp.statusCode == 404
        assert "not found" in resp.error["message"].lower()

    def test_fresh_build_returns_201_and_persists_evidence_graph(self):
        cfg = _mongomock_config()
        _insert_chain(cfg, naan="59852", postfix="ds-out")

        resp = FairscapeEvidenceGraphRequest(cfg).build_evidence_graph_for_node(
            _user(), naan="59852", postfix="ds-out",
        )
        assert resp.success is True
        assert resp.statusCode == 201

        eg_id = "ark:59852/evidence-graph-ds-out"
        # The evidence-graph doc landed in identifierCollection.
        stored = cfg.identifierCollection.find_one({"@id": eg_id})
        assert stored is not None
        assert stored["@type"] == "evi:EvidenceGraph"
        assert stored["permissions"]["owner"] == "alice@fairscape.org"
        assert stored["permissions"]["group"] == "team-alpha"

        # The response envelope round-tripped through get_evidence_graph.
        assert resp.model.guid == eg_id

        # Source node got the back-pointer written.
        src = cfg.identifierCollection.find_one({"@id": "ark:59852/ds-out"})
        assert src["metadata"]["hasEvidenceGraph"] == {"@id": eg_id}

    def test_graph_contains_reachable_nodes(self):
        cfg = _mongomock_config()
        _insert_chain(cfg, naan="59852", postfix="ds-out")
        FairscapeEvidenceGraphRequest(cfg).build_evidence_graph_for_node(
            _user(), naan="59852", postfix="ds-out",
        )
        stored = cfg.identifierCollection.find_one(
            {"@id": "ark:59852/evidence-graph-ds-out"}
        )
        graph = stored["metadata"]["@graph"]
        assert set(graph) == {
            "ark:59852/ds-out",
            "ark:59852/comp",
            "ark:59852/input",
            "ark:59852/sw",
        }

    def test_idempotent_200_when_back_pointer_and_graph_exist(self):
        cfg = _mongomock_config()
        _insert_chain(cfg, naan="59852", postfix="ds-out")
        req = FairscapeEvidenceGraphRequest(cfg)

        # First call: fresh 201.
        first = req.build_evidence_graph_for_node(
            _user(), naan="59852", postfix="ds-out",
        )
        assert first.statusCode == 201
        # Second call: the back-pointer now points at the existing graph,
        # so the CRUD returns the existing envelope with 200.
        second = req.build_evidence_graph_for_node(
            _user(), naan="59852", postfix="ds-out",
        )
        assert second.success is True
        assert second.statusCode == 200
        assert second.model.guid == first.model.guid
        # Only one evidence-graph doc exists in the collection.
        count = cfg.identifierCollection.count_documents(
            {"@id": "ark:59852/evidence-graph-ds-out"}
        )
        assert count == 1

    def test_stale_back_pointer_falls_through_to_rebuild(self):
        """Source node carries `hasEvidenceGraph` but the referenced doc
        isn't actually in the collection -- CRUD rebuilds and returns 201."""
        cfg = _mongomock_config()
        _insert_chain(
            cfg, naan="59852", postfix="ds-out",
        )
        # Seed a dangling back-pointer on the source node.
        cfg.identifierCollection.update_one(
            {"@id": "ark:59852/ds-out"},
            {"$set": {
                "metadata.hasEvidenceGraph": {"@id": "ark:59852/nonexistent-graph"},
            }},
        )

        resp = FairscapeEvidenceGraphRequest(cfg).build_evidence_graph_for_node(
            _user(), naan="59852", postfix="ds-out",
        )
        assert resp.success is True
        assert resp.statusCode == 201
        # Back-pointer now points at the real graph, not the stale id.
        src = cfg.identifierCollection.find_one({"@id": "ark:59852/ds-out"})
        assert src["metadata"]["hasEvidenceGraph"] == {
            "@id": "ark:59852/evidence-graph-ds-out",
        }

    def test_owner_group_none_when_user_has_no_groups(self):
        cfg = _mongomock_config()
        _insert_chain(cfg, naan="59852", postfix="ds-out")

        resp = FairscapeEvidenceGraphRequest(cfg).build_evidence_graph_for_node(
            _user(email="bob@x", groups=()),
            naan="59852",
            postfix="ds-out",
        )
        assert resp.statusCode == 201
        stored = cfg.identifierCollection.find_one(
            {"@id": "ark:59852/evidence-graph-ds-out"}
        )
        assert stored["permissions"]["owner"] == "bob@x"
        assert stored["permissions"]["group"] is None

    def test_duplicate_key_race_returns_409(self):
        cfg = _mongomock_config()
        _insert_chain(cfg, naan="59852", postfix="ds-out")
        # Pre-plant the evidence-graph id so the sink's insert trips
        # DuplicateKeyError. mongomock needs a unique index on @id for
        # that to fire.
        cfg.identifierCollection.create_index("@id", unique=True)
        cfg.identifierCollection.insert_one({
            "@id": "ark:59852/evidence-graph-ds-out",
            "metadata": {"@id": "ark:59852/evidence-graph-ds-out"},
        })

        resp = FairscapeEvidenceGraphRequest(cfg).build_evidence_graph_for_node(
            _user(), naan="59852", postfix="ds-out",
        )
        assert resp.success is False
        assert resp.statusCode == 409


# ---------------------------------------------------------------------------
# Quick smokes for the un-refactored methods
# ---------------------------------------------------------------------------


class TestOtherCrudMethods:
    def test_create_then_get(self):
        cfg = _mongomock_config()
        req = FairscapeEvidenceGraphRequest(cfg)
        user = _user()

        created = req.create_evidence_graph(
            requesting_user=user,
            evi_graph_create_model=EvidenceGraphCreate.model_validate({
                "@id": "ark:59852/manual-eg",
                "description": "manual",
                "name": "Manual EG",
            }),
        )
        assert created.success is True
        assert created.statusCode == 201

        fetched = req.get_evidence_graph("ark:59852/manual-eg")
        assert fetched.success is True
        assert fetched.statusCode == 200
        assert fetched.model.guid == "ark:59852/manual-eg"

    def test_delete_checks_top_level_owner_currently_rejects_legit_owner(self):
        """`delete_evidence_graph` checks `graph_data.get("owner")` against
        the requesting user's email, but `StoredIdentifier` docs only carry
        `owner` under `metadata.owner` / `permissions.owner` -- never at
        top level. So the check fails even when the user *is* the owner.

        Preserving this as a known quirk test rather than fixing it: Phase 3
        explicitly left `delete_evidence_graph` untouched, and the fix
        (reading `graph_data["metadata"]["owner"]` or
        `graph_data["permissions"]["owner"]`) is a follow-up separate from
        the evidence-graph migration."""
        cfg = _mongomock_config()
        req = FairscapeEvidenceGraphRequest(cfg)
        owner = _user(email="owner@x")
        req.create_evidence_graph(
            owner,
            EvidenceGraphCreate.model_validate({
                "@id": "ark:59852/owned",
                "description": "d",
            }),
        )
        resp = req.delete_evidence_graph(owner, "ark:59852/owned")
        assert resp.statusCode == 403, (
            "If this starts returning 200 the delete owner-check was fixed "
            "separately -- flip this assertion."
        )

    def test_create_conflict_on_duplicate_id(self):
        cfg = _mongomock_config()
        req = FairscapeEvidenceGraphRequest(cfg)
        payload = EvidenceGraphCreate.model_validate({
            "@id": "ark:59852/dup",
            "description": "d",
        })
        req.create_evidence_graph(requesting_user=_user(), evi_graph_create_model=payload)
        again = req.create_evidence_graph(requesting_user=_user(), evi_graph_create_model=payload)
        assert again.success is False
        assert again.statusCode == 409

    def test_delete_on_missing_doc_returns_404(self):
        cfg = _mongomock_config()
        req = FairscapeEvidenceGraphRequest(cfg)
        resp = req.delete_evidence_graph(_user(), "ark:59852/ghost")
        assert resp.success is False
        assert resp.statusCode == 404

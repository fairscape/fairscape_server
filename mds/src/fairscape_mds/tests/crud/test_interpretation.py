"""Tests for the server-side AI-mediated interpretation wiring.

After the Phase 1 #11 refactor, the pipeline proper lives in
``fairscape_graph_tools`` and is exercised by that package's own tests.
This file covers the mds_python glue:

- pure helper re-exports (still importable from
  ``fairscape_mds.crud.interpretation``),
- the Mongo adapters in ``crud/interpret_adapters.py``,
- the Condenser orchestrator against a port-shaped fake source, and
- an end-to-end run of the thin ``condense_rocrate`` wrapper against
  a mongomock-backed config.

LLM-driven ``interpret_rocrate`` is not covered here yet — that needs
a ``pydantic_ai.TestModel`` scaffold; noted as followup in
MIGRATION.md.
"""

import datetime

import mongomock
import pytest
from unittest.mock import MagicMock

from fairscape_mds.crud.interpretation import (
    FairscapeInterpretationRequest,
    GraphSynthesisResult,
    _build_index,
    _is_computation,
    _is_rocrate_root,
    _resolve_refs,
    prefetch_software_code,
)
from fairscape_mds.crud.condensation import FairscapeCondensationRequest
from fairscape_mds.crud.interpret_adapters import (
    MongoGraphSource,
    MongoResultSink,
    MongoTaskTracker,
)
from fairscape_mds.models.annotated_computation import AnnotatedComputation
from fairscape_mds.models.annotated_evidence_graph import AnnotatedEvidenceGraph

from fairscape_graph_tools.condenser import Condenser
from fairscape_graph_tools.interpreter import Interpreter, InterpretConfig
from fairscape_graph_tools.pipeline.annotate import build_computation_prompt


# ---------------------------------------------------------------------------
# Minimal condensed RO-Crate fixture
# ---------------------------------------------------------------------------

MINIMAL_CONDENSED_GRAPH = [
    {
        "@id": "ro-crate-metadata.json",
        "@type": "CreativeWork",
        "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2-DRAFT"},
        "about": {"@id": "ark:59853/rocrate-test-pipeline"},
    },
    {
        "@id": "ark:59853/rocrate-test-pipeline",
        "@type": ["Dataset", "https://w3id.org/EVI#ROCrate"],
        "name": "Test Pipeline RO-Crate",
        "description": "A minimal test pipeline for interpretation tests",
        "author": "Test Author",
        "keywords": ["test", "pipeline"],
        "evi:condensed": True,
        "evi:condensationThreshold": 5,
        "evi:originalEntityCount": 20,
        "evi:condensedEntityCount": 8,
        "evi:outputs": [{"@id": "ark:59853/dataset-output-final"}],
        "hasPart": [
            {"@id": "ark:59853/dataset-input-raw"},
            {"@id": "ark:59853/software-preprocess"},
            {"@id": "ark:59853/computation-step1"},
            {"@id": "ark:59853/dataset-intermediate"},
            {"@id": "ark:59853/software-analyze"},
            {"@id": "ark:59853/computation-step2"},
            {"@id": "ark:59853/dataset-output-final"},
        ],
    },
    {
        "@id": "ark:59853/dataset-input-raw",
        "@type": ["Dataset", "https://w3id.org/EVI#Dataset"],
        "name": "Raw Input Data",
        "description": "Raw experimental measurements",
        "format": ".csv",
        "author": "Jane Scientist",
    },
    {
        "@id": "ark:59853/software-preprocess",
        "@type": ["SoftwareSourceCode", "https://w3id.org/EVI#Software"],
        "name": "preprocess.py",
        "description": "Data preprocessing script",
        "contentUrl": "https://github.com/test-org/test-repo/blob/main/preprocess.py",
        "format": ".py",
    },
    {
        "@id": "ark:59853/computation-step1",
        "@type": ["https://w3id.org/EVI#Computation"],
        "name": "Preprocessing Step",
        "description": "Clean and normalize raw data",
        "command": "python preprocess.py",
        "runBy": "researcher@example.org",
        "dateCreated": "2025-01-15T10:00:00",
        "usedSoftware": [{"@id": "ark:59853/software-preprocess"}],
        "usedDataset": [{"@id": "ark:59853/dataset-input-raw"}],
        "generated": [{"@id": "ark:59853/dataset-intermediate"}],
    },
    {
        "@id": "ark:59853/dataset-intermediate",
        "@type": ["Dataset", "https://w3id.org/EVI#Dataset"],
        "name": "Preprocessed Data",
        "description": "Cleaned and normalized dataset",
        "format": ".parquet",
        "generatedBy": [{"@id": "ark:59853/computation-step1"}],
    },
    {
        "@id": "ark:59853/software-analyze",
        "@type": ["SoftwareSourceCode", "https://w3id.org/EVI#Software"],
        "name": "analyze.py",
        "description": "Statistical analysis script",
        "contentUrl": "https://github.com/test-org/test-repo/blob/main/analyze.py",
        "format": ".py",
    },
    {
        "@id": "ark:59853/computation-step2",
        "@type": ["https://w3id.org/EVI#Computation"],
        "name": "Analysis Step",
        "description": "Perform statistical analysis on preprocessed data",
        "command": "python analyze.py",
        "runBy": "researcher@example.org",
        "dateCreated": "2025-01-15T11:00:00",
        "usedSoftware": [{"@id": "ark:59853/software-analyze"}],
        "usedDataset": [{"@id": "ark:59853/dataset-intermediate"}],
        "generated": [{"@id": "ark:59853/dataset-output-final"}],
    },
    {
        "@id": "ark:59853/dataset-output-final",
        "@type": ["Dataset", "https://w3id.org/EVI#Dataset"],
        "name": "Final Analysis Results",
        "description": "Statistical analysis output",
        "format": ".csv",
        "generatedBy": [{"@id": "ark:59853/computation-step2"}],
    },
]


def _mongomock_config() -> MagicMock:
    """Build a minimal FairscapeConfig stand-in backed by mongomock.

    Only the attributes the adapters actually touch are provided:
    `identifierCollection`, `asyncCollection`, and the
    baseUrl/internalUrl rewrite pair used by `ServerSoftwareFetcher`.
    """
    cfg = MagicMock()
    client = mongomock.MongoClient()
    db = client["fairscape_test"]
    cfg.identifierCollection = db["identifier"]
    cfg.asyncCollection = db["async"]
    cfg.baseUrl = None
    cfg.internalUrl = None
    return cfg


def _insert_as_stored_identifiers(collection, graph: list[dict]) -> None:
    """Wrap each RO-Crate node as a StoredIdentifier doc and insert.

    Mirrors how the real server stores crates; the adapters flatten
    `metadata` back to the bare node shape on read.
    """
    now = datetime.datetime.utcnow()
    for node in graph:
        if not node.get("@id"):
            continue
        doc = {
            "@id": node["@id"],
            "@type": node.get("@type", ""),
            "metadata": node,
            "permissions": {"owner": "test@fairscape.org", "group": None},
            "distribution": None,
            "dateCreated": now,
            "dateModified": now,
        }
        collection.insert_one(doc)


# ---------------------------------------------------------------------------
# Helper tests (re-exports from fairscape_graph_tools via interpretation.py)
# ---------------------------------------------------------------------------


class TestHelpers:
    def test_build_index(self):
        index = _build_index(MINIMAL_CONDENSED_GRAPH)
        assert "ark:59853/rocrate-test-pipeline" in index
        assert "ark:59853/computation-step1" in index
        assert len(index) == 9  # including ro-crate-metadata.json

    def test_is_computation(self):
        index = _build_index(MINIMAL_CONDENSED_GRAPH)
        assert _is_computation(index["ark:59853/computation-step1"])
        assert _is_computation(index["ark:59853/computation-step2"])
        assert not _is_computation(index["ark:59853/dataset-input-raw"])
        assert not _is_computation(index["ark:59853/rocrate-test-pipeline"])

    def test_is_rocrate_root(self):
        index = _build_index(MINIMAL_CONDENSED_GRAPH)
        assert _is_rocrate_root(index["ark:59853/rocrate-test-pipeline"])
        assert not _is_rocrate_root(index["ark:59853/computation-step1"])

    def test_resolve_refs_list_of_dicts(self):
        refs = _resolve_refs([{"@id": "ark:1"}, {"@id": "ark:2"}])
        assert refs == ["ark:1", "ark:2"]

    def test_resolve_refs_single_dict(self):
        refs = _resolve_refs({"@id": "ark:1"})
        assert refs == ["ark:1"]

    def test_resolve_refs_none(self):
        assert _resolve_refs(None) == []

    def test_resolve_refs_string(self):
        assert _resolve_refs("ark:1") == ["ark:1"]


class TestFindComputations:
    """Test computation finding without DB."""

    def test_finds_all_computations(self):
        computations = [node for node in MINIMAL_CONDENSED_GRAPH if _is_computation(node)]
        assert len(computations) == 2
        comp_ids = {c["@id"] for c in computations}
        assert "ark:59853/computation-step1" in comp_ids
        assert "ark:59853/computation-step2" in comp_ids


# ---------------------------------------------------------------------------
# Prompt construction — now a free function in fairscape_graph_tools
# ---------------------------------------------------------------------------


class TestPromptBuilding:
    def test_build_computation_prompt(self):
        index = _build_index(MINIMAL_CONDENSED_GRAPH)
        computation = index["ark:59853/computation-step1"]
        software_cache = {
            "ark:59853/software-preprocess": "import pandas as pd\ndf = pd.read_csv('input.csv')"
        }

        prompt = build_computation_prompt(computation, software_cache, index)

        assert "Preprocessing Step" in prompt
        assert "python preprocess.py" in prompt
        assert "Raw Input Data" in prompt
        assert "import pandas" in prompt
        assert "Preprocessed Data" in prompt

    def test_build_prompt_missing_software(self):
        index = _build_index(MINIMAL_CONDENSED_GRAPH)
        computation = index["ark:59853/computation-step1"]
        prompt = build_computation_prompt(computation, {}, index)
        assert "Preprocessing Step" in prompt


# ---------------------------------------------------------------------------
# Condenser orchestrator (replaces the old FairscapeInterpretationRequest
# .ensure_condensed Mongo-coupled tests)
# ---------------------------------------------------------------------------


class _FakeSource:
    """Port-shaped stand-in for `GraphSource`."""

    def __init__(self, entities: dict[str, dict]):
        self._entities = entities

    def find_entity(self, ark_id):
        return self._entities.get(ark_id)

    def find_dataset_stats(self, ark_ids):
        return {}

    def build_full_graph(self, rocrate_id):
        return list(self._entities.values())


class _NoopSink:
    """Port-shaped stand-in for `ResultSink`; returns the id it was handed."""

    def persist_condensed(self, condensed_id, condensed_metadata, source_rocrate_id, stats):
        return condensed_id

    def persist_aeg(self, aeg, rocrate_id, step_annotations):
        return getattr(aeg, "guid", "ark:test/aeg")


class TestCondenserEnsureCondensed:
    def test_already_condensed_crate(self):
        """Root has `evi:condensed: true` → returned in place."""
        source_rocrate = {
            "@id": "ark:59853/rocrate-test",
            "@graph": MINIMAL_CONDENSED_GRAPH,
        }
        source = _FakeSource({"ark:59853/rocrate-test": source_rocrate})
        condenser = Condenser(source, _NoopSink())

        graph, condensed_id, root = condenser.ensure_condensed("ark:59853/rocrate-test")

        assert len(graph) == len(MINIMAL_CONDENSED_GRAPH)
        assert condensed_id == "ark:59853/rocrate-test"
        assert root.get("evi:condensed") is True

    def test_has_condensed_pointer(self):
        """RO-Crate has `hasCondensedROCrate` → Condenser fetches that crate."""
        source_rocrate = {
            "@id": "ark:59853/rocrate-test",
            "hasCondensedROCrate": {"@id": "ark:59853/rocrate-test-condensed"},
            "@graph": [],
        }
        condensed = {
            "@id": "ark:59853/rocrate-test-condensed",
            "@graph": MINIMAL_CONDENSED_GRAPH,
        }
        source = _FakeSource(
            {
                "ark:59853/rocrate-test": source_rocrate,
                "ark:59853/rocrate-test-condensed": condensed,
            }
        )
        condenser = Condenser(source, _NoopSink())

        graph, condensed_id, _ = condenser.ensure_condensed("ark:59853/rocrate-test")

        assert len(graph) == len(MINIMAL_CONDENSED_GRAPH)
        assert condensed_id == "ark:59853/rocrate-test-condensed"

    def test_not_found_raises(self):
        condenser = Condenser(_FakeSource({}), _NoopSink())
        with pytest.raises(ValueError, match="not found"):
            condenser.ensure_condensed("ark:59853/nonexistent")


# ---------------------------------------------------------------------------
# Software prefetching (now in fairscape_graph_tools.pipeline.github)
# ---------------------------------------------------------------------------


class TestPrefetchSoftware:
    """Test software pre-fetching with mocked HTTP."""

    def test_no_content_url(self):
        code = prefetch_software_code("")
        assert "No contentUrl" in code

    def test_non_github_http(self):
        code = prefetch_software_code("https://example.com/script.py")
        assert "External URL" in code

    def test_local_path(self):
        code = prefetch_software_code("scripts/preprocess.py")
        assert "Local/relative" in code


# ---------------------------------------------------------------------------
# Pydantic model shims (re-exports)
# ---------------------------------------------------------------------------


class TestAnnotatedComputationModel:
    def test_create_annotated_computation(self):
        ac = AnnotatedComputation.model_validate(
            {
                "@id": "ark:59853/annotated-computation-test",
                "name": "Test Annotation",
                "description": "Test annotation description",
                "author": "gemini-2.5-flash",
                "evi:annotates": {"@id": "ark:59853/computation-step1"},
                "evi:stepSummary": "This step preprocesses raw data by cleaning and normalizing it.",
                "evi:codeAnalysis": [
                    {
                        "software": {"@id": "ark:59853/software-preprocess"},
                        "name": "preprocess.py",
                        "summary": "Reads CSV, drops NaN, normalizes columns",
                        "keyFunctions": ["clean_data", "normalize"],
                        "assumptions": [
                            {
                                "impact": "MINOR",
                                "name": "Silent drops",
                                "description": "No logging of dropped rows",
                            }
                        ],
                    }
                ],
                "evi:inputSummaries": [
                    {
                        "dataset": {"@id": "ark:59853/dataset-input-raw"},
                        "name": "Raw Input Data",
                        "role": "Primary input",
                        "description": "Raw experimental measurements",
                    }
                ],
                "evi:outputSummaries": [
                    {
                        "dataset": {"@id": "ark:59853/dataset-intermediate"},
                        "name": "Preprocessed Data",
                        "role": "Cleaned output",
                    }
                ],
                "evi:assumptions": [
                    {
                        "impact": "MINOR",
                        "name": "No random seed",
                        "description": "No random seed was set for the normalization step",
                    }
                ],
                "evi:llmModel": "gemini-2.5-flash",
                "evi:llmTemperature": 0.2,
                "dateCreated": "2025-01-15T12:00:00",
            }
        )

        assert ac.stepSummary == "This step preprocesses raw data by cleaning and normalizing it."
        assert len(ac.codeAnalysis) == 1
        assert ac.wasDerivedFrom == [{"@id": "ark:59853/computation-step1"}]
        assert ac.wasAttributedTo == ["gemini-2.5-flash"]


class TestAnnotatedEvidenceGraphModel:
    def test_create_annotated_evidence_graph(self):
        graph_dict = {node["@id"]: node for node in MINIMAL_CONDENSED_GRAPH if "@id" in node}

        aeg = AnnotatedEvidenceGraph.model_validate(
            {
                "@id": "ark:59853/annotated-eg-test",
                "name": "Test Annotated Evidence Graph",
                "description": "Test AEG fixture",
                "author": "gemini-2.5-flash",
                "evi:annotates": {"@id": "ark:59853/rocrate-test-pipeline"},
                "@graph": graph_dict,
                "evi:executiveSummary": "This pipeline preprocesses and analyzes experimental data.",
                "evi:narrativeSummary": "The pipeline starts with raw data and produces analysis results.",
                "evi:keyFindings": ["Data is properly normalized", "No random seeds used"],
                "evi:assumptions": [
                    {
                        "impact": "MINOR",
                        "name": "Reproducibility",
                        "description": "Reproducibility could be improved",
                    }
                ],
                "evi:stepAnnotations": [
                    {"@id": "ark:59853/annotated-computation-1"},
                    {"@id": "ark:59853/annotated-computation-2"},
                ],
                "evi:llmModel": "gemini-2.5-flash",
                "evi:llmTemperature": 0.2,
                "dateCreated": "2025-01-15T12:00:00",
            }
        )

        assert aeg.executiveSummary.startswith("This pipeline")
        assert len(aeg.keyFindings) == 2
        assert len(aeg.graph) == len(graph_dict)
        assert aeg.wasDerivedFrom == [{"@id": "ark:59853/rocrate-test-pipeline"}]


class TestGraphSynthesisResult:
    def test_create_synthesis_result(self):
        result = GraphSynthesisResult(
            executiveSummary="The pipeline processes data.",
            narrativeSummary="Starting from raw data, the pipeline...",
            keyFindings=["Finding 1", "Finding 2"],
            assumptions=[],
        )
        assert result.executiveSummary == "The pipeline processes data."
        assert len(result.keyFindings) == 2


# ---------------------------------------------------------------------------
# Mongo adapter tests (replace the old FairscapeInterpretationRequest-
# scoped status-tracking tests)
# ---------------------------------------------------------------------------


class TestMongoTaskTracker:
    def test_update_sets_status_fields(self):
        cfg = MagicMock()
        tracker = MongoTaskTracker(cfg, "task-123")
        tracker.update({"status": "PROCESSING", "current_step": "CONDENSING"})
        cfg.asyncCollection.update_one.assert_called_once_with(
            {"guid": "task-123"},
            {"$set": {"status": "PROCESSING", "current_step": "CONDENSING"}},
        )

    def test_update_computation_status_uses_positional_filter(self):
        cfg = MagicMock()
        tracker = MongoTaskTracker(cfg, "task-123")
        tracker.update_computation_status("ark:59853/comp-1", {"status": "done"})
        cfg.asyncCollection.update_one.assert_called_once_with(
            {"guid": "task-123", "computation_details.computation_id": "ark:59853/comp-1"},
            {"$set": {"computation_details.$.status": "done"}},
        )

    def test_increment_completed(self):
        cfg = MagicMock()
        tracker = MongoTaskTracker(cfg, "task-123")
        tracker.increment_completed()
        cfg.asyncCollection.update_one.assert_called_once_with(
            {"guid": "task-123"},
            {"$inc": {"completed_computations": 1}},
        )


class TestInterpreterFindComputations:
    """`Interpreter._find_computations` drives both the traversal and
    the per-computation tracker state the old
    `FairscapeInterpretationRequest.find_computations` was asserted
    against."""

    def test_finds_computations_and_pushes_to_tracker(self):
        tracker = MagicMock()
        interpreter = Interpreter(
            graph=MagicMock(),
            sink=MagicMock(),
            tracker=tracker,
            software=MagicMock(),
            condenser=MagicMock(),
            config=InterpretConfig(),
        )

        computations, index = interpreter._find_computations(MINIMAL_CONDENSED_GRAPH)

        assert len(computations) == 2
        assert "ark:59853/computation-step1" in index

        calls = tracker.update.call_args_list
        assert len(calls) >= 2  # TRAVERSING status + total_computations payload
        last_payload = calls[-1][0][0]
        assert last_payload["total_computations"] == 2
        assert len(last_payload["computation_details"]) == 2


# ---------------------------------------------------------------------------
# End-to-end integration: FairscapeCondensationRequest.condense_rocrate
# via mongomock (Phase 1 structural acceptance for the condense path).
# ---------------------------------------------------------------------------


class TestCondenseRocrateIntegration:
    """Exercises the full post-refactor condense path:
    `FairscapeCondensationRequest.condense_rocrate` -> `MongoGraphSource` ->
    `Condenser` -> `MongoResultSink` -> insert into `identifierCollection`.
    """

    def test_condense_creates_condensed_doc_and_pointer(self):
        cfg = _mongomock_config()
        _insert_as_stored_identifiers(cfg.identifierCollection, MINIMAL_CONDENSED_GRAPH)

        rocrate_id = "ark:59853/rocrate-test-pipeline"
        response = FairscapeCondensationRequest(cfg).condense_rocrate(
            rocrate_id=rocrate_id,
            threshold=5,
            max_member_ids=0,
            owner_email="test@fairscape.org",
        )

        assert response.success, response.error
        assert response.statusCode == 201
        assert response.model["condensed_id"] == f"{rocrate_id}-condensed"
        assert "stats" in response.model

        # Condensed StoredIdentifier doc was written
        stored = cfg.identifierCollection.find_one({"@id": f"{rocrate_id}-condensed"})
        assert stored is not None
        metadata = stored["metadata"]
        assert "@graph" in metadata
        # Back-pointer on the source crate was set
        source_doc = cfg.identifierCollection.find_one({"@id": rocrate_id})
        assert source_doc["metadata"]["hasCondensedROCrate"] == {
            "@id": f"{rocrate_id}-condensed"
        }

    def test_condense_409_when_already_exists(self):
        cfg = _mongomock_config()
        rocrate_id = "ark:59853/rocrate-test-pipeline"
        condensed_id = f"{rocrate_id}-condensed"
        cfg.identifierCollection.insert_one(
            {"@id": condensed_id, "@type": "Dataset", "metadata": {}}
        )

        response = FairscapeCondensationRequest(cfg).condense_rocrate(
            rocrate_id=rocrate_id,
        )
        assert response.success is False
        assert response.statusCode == 409
        assert "already exists" in response.error["message"]

    def test_condense_404_when_source_missing(self):
        cfg = _mongomock_config()
        response = FairscapeCondensationRequest(cfg).condense_rocrate(
            rocrate_id="ark:59853/missing",
        )
        assert response.success is False
        assert response.statusCode == 404
        assert "No metadata found" in response.error["message"]


# ---------------------------------------------------------------------------
# Interpreter (LLM-driven) end-to-end coverage is not in scope for Phase 1;
# a pydantic_ai.TestModel scaffold is tracked in MIGRATION.md as followup.
# ---------------------------------------------------------------------------


class TestInterpretRocrateWrapperSmoke:
    """The thin wrapper's control flow around the Interpreter construction
    is exercised without running the LLM pipeline — just verify the
    task-not-found failure path."""

    def test_missing_task_raises(self):
        cfg = _mongomock_config()
        request = FairscapeInterpretationRequest(cfg)
        with pytest.raises(ValueError, match="not found"):
            request.interpret_rocrate("nonexistent-task")

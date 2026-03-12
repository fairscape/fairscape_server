"""Tests for the AI-mediated interpretation pipeline.

Uses a minimal condensed RO-Crate fixture, mongomock for DB isolation,
and PydanticAI TestModel to avoid real LLM calls.
"""

import datetime
import pytest
from unittest.mock import patch, MagicMock

from fairscape_mds.crud.interpretation import (
    FairscapeInterpretationRequest,
    _build_index,
    _is_computation,
    _is_rocrate_root,
    _resolve_refs,
    prefetch_software_code,
    GraphSynthesisResult,
)
from fairscape_mds.models.annotated_computation import AnnotatedComputation
from fairscape_mds.models.annotated_evidence_graph import AnnotatedEvidenceGraph


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


# ---------------------------------------------------------------------------
# Helper tests
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
        index = _build_index(MINIMAL_CONDENSED_GRAPH)
        computations = [node for node in MINIMAL_CONDENSED_GRAPH if _is_computation(node)]
        assert len(computations) == 2
        comp_ids = {c["@id"] for c in computations}
        assert "ark:59853/computation-step1" in comp_ids
        assert "ark:59853/computation-step2" in comp_ids


class TestPromptBuilding:
    """Test prompt construction for computation annotation."""

    def setup_method(self):
        """Create a mock config for FairscapeInterpretationRequest."""
        self.mock_config = MagicMock()
        self.mock_config.identifierCollection = MagicMock()
        self.mock_config.asyncCollection = MagicMock()
        self.request = FairscapeInterpretationRequest(self.mock_config)

    def test_build_computation_prompt(self):
        index = _build_index(MINIMAL_CONDENSED_GRAPH)
        computation = index["ark:59853/computation-step1"]
        software_cache = {"ark:59853/software-preprocess": "import pandas as pd\ndf = pd.read_csv('input.csv')"}

        prompt = self.request._build_computation_prompt(computation, software_cache, index)

        assert "Preprocessing Step" in prompt
        assert "python preprocess.py" in prompt
        assert "Raw Input Data" in prompt
        assert "import pandas" in prompt
        assert "Preprocessed Data" in prompt

    def test_build_prompt_missing_software(self):
        index = _build_index(MINIMAL_CONDENSED_GRAPH)
        computation = index["ark:59853/computation-step1"]
        software_cache = {}  # empty

        prompt = self.request._build_computation_prompt(computation, software_cache, index)

        # Should still work, just no source code section
        assert "Preprocessing Step" in prompt


class TestEnsureCondensed:
    """Test the ensure_condensed logic."""

    def setup_method(self):
        self.mock_config = MagicMock()
        self.mock_config.identifierCollection = MagicMock()
        self.mock_config.asyncCollection = MagicMock()
        self.request = FairscapeInterpretationRequest(self.mock_config)

    def test_already_condensed_crate(self):
        """When the crate itself has evi:condensed: true."""
        self.mock_config.identifierCollection.find_one.return_value = {
            "@id": "ark:59853/rocrate-test",
            "@type": ["Dataset", "https://w3id.org/EVI#ROCrate"],
            "metadata": {
                "@graph": MINIMAL_CONDENSED_GRAPH,
            }
        }

        graph, condensed_id, root = self.request.ensure_condensed("task-123", "ark:59853/rocrate-test")

        assert len(graph) == len(MINIMAL_CONDENSED_GRAPH)
        assert condensed_id == "ark:59853/rocrate-test"
        assert root.get("evi:condensed") is True

    def test_has_condensed_pointer(self):
        """When the crate has hasCondensedROCrate pointer."""
        # First call: get original crate
        # Second call: get condensed crate
        self.mock_config.identifierCollection.find_one.side_effect = [
            {
                "@id": "ark:59853/rocrate-test",
                "@type": ["Dataset", "https://w3id.org/EVI#ROCrate"],
                "metadata": {
                    "hasCondensedROCrate": {"@id": "ark:59853/rocrate-test-condensed"},
                    "@graph": [],
                }
            },
            {
                "@id": "ark:59853/rocrate-test-condensed",
                "metadata": {
                    "@graph": MINIMAL_CONDENSED_GRAPH,
                }
            },
        ]

        graph, condensed_id, root = self.request.ensure_condensed("task-123", "ark:59853/rocrate-test")

        assert len(graph) == len(MINIMAL_CONDENSED_GRAPH)

    def test_not_found_raises(self):
        """When the RO-Crate doesn't exist."""
        self.mock_config.identifierCollection.find_one.return_value = None

        with pytest.raises(ValueError, match="not found"):
            self.request.ensure_condensed("task-123", "ark:59853/nonexistent")


class TestPrefetchSoftware:
    """Test software pre-fetching with mocked HTTP."""

    @patch("fairscape_mds.crud.interpretation.httpx.get")
    def test_github_file_url(self, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "import pandas as pd\nprint('hello')"
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        code = prefetch_software_code("https://github.com/owner/repo/blob/main/script.py")
        assert "import pandas" in code

    def test_no_content_url(self):
        code = prefetch_software_code("")
        assert "No contentUrl" in code

    def test_non_github_http(self):
        code = prefetch_software_code("https://example.com/script.py")
        assert "External URL" in code

    def test_local_path(self):
        code = prefetch_software_code("scripts/preprocess.py")
        assert "Local/relative" in code


class TestAnnotatedComputationModel:
    """Test that AnnotatedComputation can be constructed correctly."""

    def test_create_annotated_computation(self):
        ac = AnnotatedComputation.model_validate({
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
                    "concerns": ["No logging of dropped rows"],
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
            "evi:concerns": ["No random seed set"],
            "evi:llmModel": "gemini-2.5-flash",
            "evi:llmTemperature": 0.2,
            "dateCreated": "2025-01-15T12:00:00",
        })

        assert ac.stepSummary == "This step preprocesses raw data by cleaning and normalizing it."
        assert len(ac.codeAnalysis) == 1
        assert ac.wasDerivedFrom == [{"@id": "ark:59853/computation-step1"}]
        assert ac.wasAttributedTo == ["gemini-2.5-flash"]


class TestAnnotatedEvidenceGraphModel:
    """Test that AnnotatedEvidenceGraph can be constructed correctly."""

    def test_create_annotated_evidence_graph(self):
        graph_dict = {node["@id"]: node for node in MINIMAL_CONDENSED_GRAPH if "@id" in node}

        aeg = AnnotatedEvidenceGraph.model_validate({
            "@id": "ark:59853/annotated-eg-test",
            "name": "Test Annotated Evidence Graph",
            "description": "Test AEG",
            "author": "gemini-2.5-flash",
            "evi:annotates": {"@id": "ark:59853/rocrate-test-pipeline"},
            "@graph": graph_dict,
            "evi:executiveSummary": "This pipeline preprocesses and analyzes experimental data.",
            "evi:narrativeSummary": "The pipeline starts with raw data and produces analysis results.",
            "evi:keyFindings": ["Data is properly normalized", "No random seeds used"],
            "evi:concerns": ["Reproducibility could be improved"],
            "evi:stepAnnotations": [
                {"@id": "ark:59853/annotated-computation-1"},
                {"@id": "ark:59853/annotated-computation-2"},
            ],
            "evi:llmModel": "gemini-2.5-flash",
            "evi:llmTemperature": 0.2,
            "dateCreated": "2025-01-15T12:00:00",
        })

        assert aeg.executiveSummary.startswith("This pipeline")
        assert len(aeg.keyFindings) == 2
        assert len(aeg.graph) == len(graph_dict)
        assert aeg.wasDerivedFrom == [{"@id": "ark:59853/rocrate-test-pipeline"}]


class TestGraphSynthesisResult:
    """Test the synthesis result model."""

    def test_create_synthesis_result(self):
        result = GraphSynthesisResult(
            executiveSummary="The pipeline processes data.",
            narrativeSummary="Starting from raw data, the pipeline...",
            keyFindings=["Finding 1", "Finding 2"],
            concerns=["Concern 1"],
        )
        assert result.executiveSummary == "The pipeline processes data."
        assert len(result.keyFindings) == 2


class TestStatusTracking:
    """Test that status updates are called correctly."""

    def setup_method(self):
        self.mock_config = MagicMock()
        self.mock_config.identifierCollection = MagicMock()
        self.mock_config.asyncCollection = MagicMock()
        self.request = FairscapeInterpretationRequest(self.mock_config)

    def test_update_task(self):
        self.request._update_task("task-123", {"status": "PROCESSING", "current_step": "CONDENSING"})
        self.mock_config.asyncCollection.update_one.assert_called_once_with(
            {"guid": "task-123"},
            {"$set": {"status": "PROCESSING", "current_step": "CONDENSING"}}
        )

    def test_find_computations_updates_status(self):
        self.request.find_computations("task-123", MINIMAL_CONDENSED_GRAPH)

        # Should have been called to update TRAVERSING status and computation details
        calls = self.mock_config.asyncCollection.update_one.call_args_list
        assert len(calls) >= 2  # at least status update + computation details update

        # Check that total_computations was set
        last_call_args = calls[-1][0][1]["$set"]
        assert last_call_args["total_computations"] == 2

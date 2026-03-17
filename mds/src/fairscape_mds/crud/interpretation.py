"""
interpretation.py -- AI-Mediated Interpretation Pipeline

Takes an RO-Crate, ensures it is condensed, finds all Computation nodes,
pre-fetches software source code from GitHub, prompts an LLM per computation
(in parallel via ThreadPoolExecutor), synthesizes a graph-level summary,
and stores the resulting AnnotatedEvidenceGraph.
"""

import asyncio
import datetime
import re
import uuid
import logging
from typing import Any, Dict, List, Optional, Tuple

import httpx
from pydantic_ai import Agent

from fairscape_mds.models.annotated_computation import (
    AnnotatedComputation, CodeAnalysis, DatasetSummary,
    LLMComputationAnnotation, LLMCodeAnalysis, LLMDatasetSummary,
    Concern, LLMConcern, ConcernLevel, normalize_concern,
)
from fairscape_mds.models.annotated_evidence_graph import (
    AnnotatedEvidenceGraph, GraphConcern,
)

from fairscape_mds.crud.fairscape_request import FairscapeRequest
from fairscape_mds.crud.fairscape_response import FairscapeResponse
from fairscape_mds.crud.condensation import FairscapeCondensationRequest
from fairscape_mds.models.identifier import (
    MetadataTypeEnum,
    StoredIdentifier,
    PublicationStatusEnum,
)
from fairscape_mds.models.user import Permissions

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Global Event Loop for Worker
# ---------------------------------------------------------------------------

_worker_loop = None


def run_async(coro):
    """Run an async coroutine using a single, persistent event loop per worker process.

    This prevents 'RuntimeError: bound to a different event loop' caused by
    PydanticAI's globally cached HTTP client.
    """
    global _worker_loop
    if _worker_loop is None or _worker_loop.is_closed():
        _worker_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(_worker_loop)
    return _worker_loop.run_until_complete(coro)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MAX_SOFTWARE_BYTES = 50_000  # 50KB per software entity
CODE_EXTENSIONS = {".py", ".r", ".R", ".sh", ".pl", ".java", ".scala", ".jl", ".m", ".cpp", ".go", ".rs", ".ipynb", ".md"}
GITHUB_REPO_PATTERN = re.compile(r"https?://github\.com/([^/]+)/([^/]+?)(?:\.git)?(?:/.*)?$")
GITHUB_FILE_PATTERN = re.compile(
    r"https?://github\.com/([^/]+)/([^/]+)/blob/([^/]+)/(.*)"
)

# ---------------------------------------------------------------------------
# System Prompt -- Data Science Persona
# ---------------------------------------------------------------------------

DATASCI_SYSTEM_PROMPT = """You are a senior data scientist and methodologist analyzing a single computation step from a scientific provenance graph (RO-Crate).

You will receive:
- The computation's metadata (name, description, command)
- Input dataset metadata (names, descriptions, formats)
- Software source code used by the computation
- Output dataset metadata (names, descriptions, formats)

Your task is to produce a structured annotation of this computation step.

## Analysis Guidelines

### Code Analysis (Deep)
- Libraries and frameworks: Note specific libraries and whether they are standard choices
- Data transformations: Trace transformations applied. Flag hidden assumptions
- Statistical methods: Evaluate appropriateness for the data type and research question
- Hardcoded values: Flag magic numbers and parameters that should be configurable
- Error handling: Note whether edge cases are handled

### Methodology Assessment
- Data leakage: Check for test/validation information leaking into training
- Selection bias: Consider whether filtering steps introduce bias
- Reproducibility: Random seeds, version pinning, parameter documentation

### Concern Severity Levels — READ CAREFULLY
Every concern MUST be assigned exactly one of these three severity levels. Apply them strictly using the decision procedure below.

**CRITICAL — Conclusions cannot be trusted**
The code produces results that are demonstrably wrong, or the methodology is fundamentally unsound such that the conclusions it supports are not warranted. You must be able to point to what the code actually does (or claims) that makes the output unreliable.

Ask: "If a reader trusts this output at face value, will they be misled?"

Applies when:
- The code does X but reports it as Y (e.g., evaluates on training data, labels it "test accuracy")
- A quantity is presented as measured or calibrated but is actually fabricated or an acknowledged guess
- Information from the target/outcome leaks into the features or model inputs
- A metric is used in a way that does not measure what it claims (e.g., treating UMAP distance as a statistical test of significance)

Does NOT apply when:
- A risk exists but the code handles it correctly
- A methodology is claimed but not shown — that is unverifiable, not wrong
- Something *could* go wrong if a downstream user misuses the output

**MODERATE — Methodology has a real, demonstrable weakness**
A reviewer would flag this as a gap that weakens confidence in the results, but the results are not necessarily wrong. The issue is concrete and present in the pipeline — not hypothetical.

Ask: "Does this weaken the strength of evidence, even if the conclusions might still be correct?"

Applies when:
- Stochastic steps lack random seeds, so results are not reproducible across runs
- Only a subset of available data is used without justification (e.g., one replicate of three)
- Evaluation metrics are inappropriate for the data characteristics (e.g., accuracy on imbalanced classes)
- Key parameters are hardcoded without sensitivity analysis and directly affect the results (e.g., similarity thresholds that determine network density)
- No validation or calibration of a method whose accuracy is not self-evident

Does NOT apply when:
- The code does not re-verify something already done correctly upstream
- A parameter is hardcoded but has a reasonable default and low impact
- A claimed process is not shown in code but there is no evidence it was done wrong

**MINOR — Recommendations and best-practice gaps**
Suggestions that would improve rigour, documentation, or robustness, but whose absence does not weaken the actual results or conclusions drawn from this pipeline.

Ask: "Would fixing this change the results or conclusions?" If no, it is MINOR.

Applies when:
- Missing version pinning, dependency documentation, or environment specifications
- No input validation or schema checks
- Hardcoded parameters with low impact on results
- Missing confidence intervals or uncertainty quantification (when results are otherwise sound)
- Documentation gaps, typos, missing docstrings
- Model artifacts not serialised

### Decision procedure
1. Can you point to specific code/output where the result is wrong or misleading? → CRITICAL
2. No, but can you identify a concrete methodological gap that weakens the evidence? → MODERATE
3. No, but you have a recommendation that would improve the work? → MINOR
4. None of the above? → Do not raise a concern.

### Key principles
- There is no minimum number of concerns at any level. Many well-written computation steps will have zero CRITICAL and zero MODERATE concerns. That is a valid and expected outcome — report it as such.
- Do not inflate severity to fill quotas. A concern list with 2 MINOR items is better analysis than one with fabricated CRITICAL findings.
- Grade based on what the code *actually does*, not hypothetical scenarios where someone might misuse its outputs.
- If the code handles something correctly, do not raise a concern about how a future user *could* get it wrong.
- When in doubt between two levels, choose the lower one.

Use ONLY these three levels. Do not invent additional levels like WARNING, INFO, or GOOD.

### Output Requirements
Return a structured annotation with:
- stepSummary: A clear description of what this computation step does and why
- codeAnalysis: For each software entity, provide a summary, key functions, and concerns (each concern as {level, description})
- inputSummaries: For each input dataset, describe its role
- outputSummaries: For each output dataset, describe what it contains
- concerns: List any methodological, statistical, or reproducibility concerns (each as {level, description}). This list may be empty or contain only MINOR items if the step is methodologically sound.

Be precise and evidence-based. Reference specific function names and parameter values.
Acknowledge when methods are well-chosen."""


SYNTHESIS_SYSTEM_PROMPT = """You are a senior data scientist synthesizing annotations from all computation steps in a scientific provenance graph (RO-Crate) into a graph-level summary.

You will receive:
- The RO-Crate name and description
- Step-by-step annotations for each computation in the pipeline
- The pipeline's final outputs

Your task is to produce:
1. executiveSummary: 3-5 sentences covering what the pipeline does, its analytical approach, and the most important observation
2. narrativeSummary: A forward-chronological story of the entire pipeline, starting from origin data and ending at final outputs
3. keyFindings: Bulleted list of important observations, prioritized by severity
4. concerns: Cross-cutting methodological, statistical, or reproducibility concerns that span the pipeline. Each concern must be a structured object with:
   - level: One of CRITICAL, MODERATE, or MINOR (no other levels allowed)
   - description: The concern text

### Concern Severity — Apply the same rubric as step-level analysis

**CRITICAL — Conclusions cannot be trusted.** The pipeline produces results that are demonstrably wrong or misleading. You must trace the flaw through specific computation steps. Ask: "If a reader trusts the final output at face value, will they be misled?"

**MODERATE — Methodology has a real, demonstrable weakness.** A reviewer would flag this as weakening confidence, but results are not necessarily wrong. Ask: "Does this weaken the strength of evidence?"

**MINOR — Recommendations and best-practice gaps.** Would improve the work, but fixing it would not change the results or conclusions.

### Decision procedure
1. Can you trace a specific flaw through the pipeline that makes an output wrong or misleading? → CRITICAL
2. No, but can you identify a concrete methodological gap that weakens the evidence? → MODERATE
3. No, but you have a recommendation? → MINOR

### Key principles
- There is no minimum number of concerns at any level. A pipeline with zero CRITICAL and zero MODERATE concerns is a valid and expected outcome for well-constructed work. Report that clearly rather than manufacturing issues.
- Do NOT escalate step-level concerns. If a step annotation flagged something as MINOR, do not promote it to MODERATE or CRITICAL unless a cross-step interaction demonstrably makes it worse.
- Do NOT re-list every step-level concern. Only surface concerns that matter at the pipeline level — either because they span multiple steps, compound across steps, or are the most important findings.
- Grade based on what the code actually does, not hypothetical misuse scenarios.
- When in doubt between two levels, choose the lower one.

Write as if explaining to a colleague. Be precise and evidence-based."""


# ---------------------------------------------------------------------------
# Graph-level synthesis result model (just the fields we need from LLM)
# ---------------------------------------------------------------------------

from pydantic import BaseModel, Field as PydanticField


class GraphSynthesisResult(BaseModel):
    """Result model for the graph-level synthesis LLM call."""
    executiveSummary: str
    narrativeSummary: str
    keyFindings: List[str] = []
    concerns: List[LLMConcern] = []


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _extract_short_types(node: dict) -> list:
    """Extract short EVI type names from a node's @type field."""
    raw = node.get("@type", [])
    if isinstance(raw, str):
        raw = [raw]
    shorts = []
    for t in raw:
        short = t.split("#")[-1] if "#" in t else t.split(":")[-1] if ":" in t else t
        shorts.append(short)
    return shorts


def _is_computation(node: dict) -> bool:
    return "Computation" in _extract_short_types(node)


def _is_rocrate_root(node: dict) -> bool:
    return "ROCrate" in _extract_short_types(node)


def _resolve_refs(field) -> list:
    """Normalize a reference field to a list of @id strings."""
    if field is None:
        return []
    if isinstance(field, str):
        return [field]
    if isinstance(field, dict):
        return [field.get("@id", "")] if "@id" in field else []
    if isinstance(field, list):
        result = []
        for item in field:
            if isinstance(item, str):
                result.append(item)
            elif isinstance(item, dict) and "@id" in item:
                result.append(item["@id"])
        return result
    return []


def _build_index(graph: list) -> dict:
    """Build {`@id` -> node} index from @graph array."""
    index = {}
    for node in graph:
        node_id = node.get("@id")
        if node_id:
            index[node_id] = node
    return index


# ---------------------------------------------------------------------------
# GitHub source code fetching
# ---------------------------------------------------------------------------

def _fetch_github_file(owner: str, repo: str, branch: str, path: str) -> str:
    """Fetch a single file from GitHub via raw URL."""
    raw_url = f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{path}"
    try:
        resp = httpx.get(raw_url, timeout=15, follow_redirects=True)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        logger.warning(f"Failed to fetch {raw_url}: {e}")
        return ""


def _fetch_github_repo_code(owner: str, repo: str, max_bytes: int = MAX_SOFTWARE_BYTES) -> str:
    """Fetch code files from a GitHub repo up to max_bytes total."""
    try:
        # Get default branch
        api_url = f"https://api.github.com/repos/{owner}/{repo}"
        resp = httpx.get(api_url, timeout=15, follow_redirects=True)
        resp.raise_for_status()
        default_branch = resp.json().get("default_branch", "main")

        # Get tree
        tree_url = f"https://api.github.com/repos/{owner}/{repo}/git/trees/{default_branch}?recursive=1"
        resp = httpx.get(tree_url, timeout=15, follow_redirects=True)
        resp.raise_for_status()
        tree = resp.json().get("tree", [])

        # Filter for code files
        code_files = []
        for item in tree:
            if item.get("type") != "blob":
                continue
            path = item.get("path", "")
            ext = "." + path.rsplit(".", 1)[-1] if "." in path else ""
            if ext.lower() in {e.lower() for e in CODE_EXTENSIONS}:
                code_files.append(path)

        # Sort: prefer top-level files, then by name
        code_files.sort(key=lambda p: (p.count("/"), p))

        # Fetch files up to limit
        collected = []
        total_bytes = 0
        for path in code_files:
            if total_bytes >= max_bytes:
                collected.append(f"\n--- [Truncated: reached {max_bytes} byte limit] ---\n")
                break
            content = _fetch_github_file(owner, repo, default_branch, path)
            if content:
                header = f"\n{'='*60}\n# FILE: {path}\n{'='*60}\n"
                collected.append(header + content)
                total_bytes += len(content.encode("utf-8"))

        return "\n".join(collected)

    except Exception as e:
        logger.warning(f"Failed to fetch repo {owner}/{repo}: {e}")
        return f"[Could not fetch repository code: {e}]"


def prefetch_software_code(content_url: str) -> str:
    """Fetch source code from a contentUrl, handling GitHub URLs."""
    if not content_url:
        return "[No contentUrl available]"

    # Check for GitHub file URL: github.com/owner/repo/blob/branch/path
    file_match = GITHUB_FILE_PATTERN.match(content_url)
    if file_match:
        owner, repo, branch, path = file_match.groups()
        code = _fetch_github_file(owner, repo, branch, path)
        return code if code else f"[Could not fetch file from {content_url}]"

    # Check for GitHub repo URL: github.com/owner/repo
    repo_match = GITHUB_REPO_PATTERN.match(content_url)
    if repo_match:
        owner, repo = repo_match.groups()
        return _fetch_github_repo_code(owner, repo)

    # Non-GitHub HTTP URL
    if content_url.startswith("http"):
        return f"[External URL, not fetched: {content_url}]"

    return f"[Local/relative path: {content_url}]"


# ---------------------------------------------------------------------------
# CRUD Class
# ---------------------------------------------------------------------------

class FairscapeInterpretationRequest(FairscapeRequest):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._condensation = FairscapeCondensationRequest(self.config)

    def _update_task(self, task_guid: str, updates: dict):
        """Update the async task document in MongoDB."""
        self.config.asyncCollection.update_one(
            {"guid": task_guid},
            {"$set": updates}
        )

    def _save_llm_result(self, task_guid: str, label: str, raw_output: dict):
        """Append a raw LLM result to the task document for debugging."""
        self.config.asyncCollection.update_one(
            {"guid": task_guid},
            {"$push": {"llm_results": {
                "label": label,
                "timestamp": datetime.datetime.utcnow().isoformat(),
                "output": raw_output,
            }}}
        )

    # ------------------------------------------------------------------
    # Step 1: Ensure condensed
    # ------------------------------------------------------------------

    def ensure_condensed(self, task_guid: str, rocrate_id: str) -> Tuple[list, str, dict]:
        """Ensure the RO-Crate is condensed. Returns (graph_list, condensed_id, root_node).

        Checks:
        1. metadata.hasCondensedROCrate pointer -> fetch that doc
        2. evi:condensed: true on the crate itself -> it IS condensed
        3. Otherwise, trigger condensation synchronously
        """
        self._update_task(task_guid, {
            "current_step": "CONDENSING",
            "status": "CONDENSING",
        })

        # Fetch the RO-Crate from MongoDB
        entity = self.flexibleFind(rocrate_id)
        if not entity:
            raise ValueError(f"RO-Crate {rocrate_id} not found")

        metadata = entity.get("metadata", {})

        # Case 1: Has pointer to condensed version
        condensed_ref = metadata.get("hasCondensedROCrate")
        if condensed_ref:
            condensed_id = condensed_ref.get("@id") if isinstance(condensed_ref, dict) else condensed_ref
            condensed_doc = self.flexibleFind(condensed_id)
            if condensed_doc:
                condensed_metadata = condensed_doc.get("metadata", {})
                graph = condensed_metadata.get("@graph", [])
                if isinstance(graph, dict):
                    graph = list(graph.values())
                index = _build_index(graph)
                root = next((n for n in graph if _is_rocrate_root(n)), {})
                return graph, condensed_id, root

        # Case 2: The crate itself is condensed
        # Check the root node in the graph for evi:condensed
        graph = metadata.get("@graph", [])
        if isinstance(graph, dict):
            graph = list(graph.values())

        for node in graph:
            if _is_rocrate_root(node):
                if node.get("evi:condensed") is True:
                    return graph, rocrate_id, node

        # Case 3: Need to condense
        logger.info(f"Condensing RO-Crate {rocrate_id}")
        response = self._condensation.condense_rocrate(
            rocrate_id=rocrate_id,
            threshold=5,
            max_member_ids=0,
            owner_email="system@fairscape.org",
        )

        if not response.success:
            raise RuntimeError(f"Condensation failed: {response.error}")

        # Fetch the newly created condensed doc
        condensed_id = f"{rocrate_id}-condensed"
        condensed_doc = self.flexibleFind(condensed_id)
        if not condensed_doc:
            raise RuntimeError(f"Condensed RO-Crate {condensed_id} not found after condensation")

        condensed_metadata = condensed_doc.get("metadata", {})
        graph = condensed_metadata.get("@graph", [])
        if isinstance(graph, dict):
            graph = list(graph.values())
        root = next((n for n in graph if _is_rocrate_root(n)), {})

        self._update_task(task_guid, {"condensed_rocrate_id": condensed_id})
        return graph, condensed_id, root

    # ------------------------------------------------------------------
    # Step 2: Find computations
    # ------------------------------------------------------------------

    def find_computations(self, task_guid: str, graph: list) -> Tuple[List[dict], dict]:
        """Find all Computation nodes in the graph. Returns (computations, index)."""
        self._update_task(task_guid, {
            "current_step": "TRAVERSING",
            "status": "TRAVERSING",
        })

        index = _build_index(graph)
        computations = [node for node in graph if _is_computation(node)]

        # Initialize computation_details for progress tracking
        comp_details = [
            {"computation_id": c.get("@id", ""), "name": c.get("name", ""), "status": "pending"}
            for c in computations
        ]
        self._update_task(task_guid, {
            "total_computations": len(computations),
            "computation_details": comp_details,
        })

        logger.info(f"Found {len(computations)} computation(s) in graph")
        return computations, index

    # ------------------------------------------------------------------
    # Step 3: Pre-fetch software
    # ------------------------------------------------------------------

    def prefetch_all_software(self, task_guid: str, computations: list, index: dict, user_token: str = "") -> Dict[str, str]:
        """Pre-fetch source code for all software referenced by computations.
        Returns {software_id: source_code_text}.
        """
        self._update_task(task_guid, {
            "current_step": "PREFETCHING",
            "status": "PREFETCHING",
        })

        software_cache: Dict[str, str] = {}
        for comp in computations:
            software_refs = _resolve_refs(comp.get("usedSoftware"))
            for sw_id in software_refs:
                if sw_id in software_cache:
                    continue
                sw_node = index.get(sw_id, {})
                content_url = sw_node.get("contentUrl", "")

                # Fairscape-hosted software: call download endpoint with user auth
                if "/software/download/" in content_url and user_token:
                    try:
                        # Rewrite external URLs to internal service URL if configured
                        internal_url = content_url
                        if self.config.internalUrl and self.config.baseUrl:
                            internal_url = content_url.replace(self.config.baseUrl, self.config.internalUrl)
                        resp = httpx.get(
                            internal_url,
                            headers={"Authorization": f"Bearer {user_token}"},
                            timeout=30.0
                        )
                        resp.raise_for_status()
                        code = resp.text
                        if len(code.encode("utf-8")) > MAX_SOFTWARE_BYTES:
                            code = code[:MAX_SOFTWARE_BYTES] + "\n[...truncated...]"
                        software_cache[sw_id] = code
                        logger.info(f"Pre-fetched software {sw_id} from download endpoint: {len(code)} chars")
                        continue
                    except Exception as e:
                        logger.warning(f"Failed to fetch software {sw_id} from download endpoint: {e}")

                # Fall back to GitHub/external URL fetching
                code = prefetch_software_code(content_url)
                software_cache[sw_id] = code
                logger.info(f"Pre-fetched software {sw_id}: {len(code)} chars")

        return software_cache

    # ------------------------------------------------------------------
    # Step 4: Annotate single computation
    # ------------------------------------------------------------------

    def _build_computation_prompt(self, computation: dict, software_cache: dict, index: dict) -> str:
        """Build the prompt for a single computation annotation."""
        parts = []

        # Computation metadata
        parts.append("## Computation")
        parts.append(f"**ID:** {computation.get('@id', 'unknown')}")
        parts.append(f"**Name:** {computation.get('name', 'unnamed')}")
        parts.append(f"**Description:** {computation.get('description', 'No description')}")
        parts.append(f"**Command:** {computation.get('command', 'N/A')}")
        parts.append(f"**Run By:** {computation.get('runBy', 'unknown')}")
        parts.append(f"**Date Created:** {computation.get('dateCreated', 'unknown')}")
        parts.append("")

        # Input datasets
        input_refs = _resolve_refs(computation.get("usedDataset"))
        if input_refs:
            parts.append("## Input Datasets")
            for ds_id in input_refs:
                ds_node = index.get(ds_id, {})
                parts.append(f"- **{ds_node.get('name', ds_id)}** ({ds_node.get('format', 'unknown format')})")
                parts.append(f"  ID: {ds_id}")
                parts.append(f"  Description: {ds_node.get('description', 'No description')}")
                if ds_node.get("keywords"):
                    parts.append(f"  Keywords: {', '.join(ds_node['keywords']) if isinstance(ds_node['keywords'], list) else ds_node['keywords']}")
            parts.append("")

        # Software and source code
        software_refs = _resolve_refs(computation.get("usedSoftware"))
        if software_refs:
            parts.append("## Software")
            for sw_id in software_refs:
                sw_node = index.get(sw_id, {})
                parts.append(f"### {sw_node.get('name', sw_id)}")
                parts.append(f"**ID:** {sw_id}")
                parts.append(f"**Description:** {sw_node.get('description', 'No description')}")
                parts.append(f"**Content URL:** {sw_node.get('contentUrl', 'N/A')}")
                code = software_cache.get(sw_id, "")
                if code and not code.startswith("["):
                    parts.append(f"\n**Source Code:**\n```\n{code}\n```")
                else:
                    parts.append(f"\n**Source Code:** {code}")
            parts.append("")

        # Output datasets
        output_refs = _resolve_refs(computation.get("generated"))
        if output_refs:
            parts.append("## Output Datasets")
            for ds_id in output_refs:
                ds_node = index.get(ds_id, {})
                parts.append(f"- **{ds_node.get('name', ds_id)}** ({ds_node.get('format', 'unknown format')})")
                parts.append(f"  ID: {ds_id}")
                parts.append(f"  Description: {ds_node.get('description', 'No description')}")
            parts.append("")

        return "\n".join(parts)

    def _llm_to_annotated(
        self,
        llm_result: LLMComputationAnnotation,
        comp_id: str,
        llm_model: str,
        temperature: float,
    ) -> AnnotatedComputation:
        """Convert lightweight LLM output into a full AnnotatedComputation."""
        annotation_id = f"{comp_id}-annotation"
        now = datetime.datetime.utcnow().isoformat()

        # Convert LLM code analyses -> CodeAnalysis with IdentifierValue
        code_analyses = [
            CodeAnalysis(
                software={"@id": ca.software_id},
                name=ca.name,
                summary=ca.summary,
                keyFunctions=ca.keyFunctions,
                concerns=[normalize_concern(c) for c in (ca.concerns or [])],
            )
            for ca in (llm_result.codeAnalysis or [])
        ]

        # Convert LLM dataset summaries -> DatasetSummary with IdentifierValue
        input_summaries = [
            DatasetSummary(
                dataset={"@id": ds.dataset_id},
                name=ds.name,
                role=ds.role,
                description=ds.description,
            )
            for ds in (llm_result.inputSummaries or [])
        ]
        output_summaries = [
            DatasetSummary(
                dataset={"@id": ds.dataset_id},
                name=ds.name,
                role=ds.role,
                description=ds.description,
            )
            for ds in (llm_result.outputSummaries or [])
        ]

        return AnnotatedComputation.model_validate({
            "@id": annotation_id,
            "name": f"Annotation of {comp_id}",
            "author": llm_model,
            "description": llm_result.stepSummary[:200] if len(llm_result.stepSummary) >= 10 else llm_result.stepSummary + " " * (10 - len(llm_result.stepSummary)),
            "evi:annotates": {"@id": comp_id},
            "evi:stepSummary": llm_result.stepSummary,
            "evi:codeAnalysis": [ca.model_dump(by_alias=True) for ca in code_analyses],
            "evi:inputSummaries": [ds.model_dump(by_alias=True) for ds in input_summaries],
            "evi:outputSummaries": [ds.model_dump(by_alias=True) for ds in output_summaries],
            "evi:concerns": [normalize_concern(c).model_dump() for c in (llm_result.concerns or [])],
            "evi:llmModel": llm_model,
            "evi:llmTemperature": temperature,
            "dateCreated": now,
        })

    async def _annotate_single_computation(
        self,
        task_guid: str,
        computation: dict,
        software_cache: dict,
        index: dict,
        llm_model: str,
        temperature: float,
    ) -> AnnotatedComputation:
        """Annotate a single computation using PydanticAI."""
        prompt = self._build_computation_prompt(computation, software_cache, index)
        comp_id = computation.get("@id", f"ark:59853/computation-{uuid.uuid4()}")

        agent = Agent(
            llm_model,
            output_type=LLMComputationAnnotation,
            system_prompt=DATASCI_SYSTEM_PROMPT,
            retries=3,
        )

        result = await agent.run(prompt)
        llm_output: LLMComputationAnnotation = result.output

        # Persist raw LLM output for debugging
        self._save_llm_result(
            task_guid,
            f"computation:{comp_id}",
            llm_output.model_dump(mode="json"),
        )

        # Convert lightweight LLM output -> full AnnotatedComputation
        return self._llm_to_annotated(llm_output, comp_id, llm_model, temperature)

    # ------------------------------------------------------------------
    # Step 4b: Parallel computation processing
    # ------------------------------------------------------------------

    async def _annotate_computations_async(
        self,
        task_guid: str,
        computations: list,
        software_cache: dict,
        index: dict,
        llm_model: str,
        temperature: float,
        max_workers: int = 4,
    ) -> List[AnnotatedComputation]:
        """Annotate all computations concurrently via asyncio.gather."""
        self._update_task(task_guid, {
            "current_step": "PROMPTING",
            "status": "PROMPTING",
        })

        sem = asyncio.Semaphore(max_workers)

        async def _annotate_one(comp):
            comp_id = comp.get("@id", "unknown")
            async with sem:
                try:
                    annotated = await self._annotate_single_computation(
                        task_guid, comp, software_cache, index, llm_model, temperature,
                    )
                    self.config.asyncCollection.update_one(
                        {"guid": task_guid, "computation_details.computation_id": comp_id},
                        {"$set": {"computation_details.$.status": "done"}}
                    )
                    return ("ok", comp_id, annotated)
                except Exception as e:
                    logger.error(f"Failed to annotate computation {comp_id}: {e}")
                    self.config.asyncCollection.update_one(
                        {"guid": task_guid, "computation_details.computation_id": comp_id},
                        {"$set": {
                            "computation_details.$.status": "error",
                            "computation_details.$.error": str(e),
                        }}
                    )
                    return ("error", comp_id, str(e))
                finally:
                    self.config.asyncCollection.update_one(
                        {"guid": task_guid},
                        {"$inc": {"completed_computations": 1}}
                    )

        outcomes = await asyncio.gather(*[_annotate_one(c) for c in computations])

        results = [o[2] for o in outcomes if o[0] == "ok"]
        errors = [{"computation_id": o[1], "error": o[2]} for o in outcomes if o[0] == "error"]

        if errors and not results:
            raise RuntimeError(f"All {len(errors)} computation annotations failed. First error: {errors[0]['error']}")
        if errors:
            logger.warning(f"{len(errors)} of {len(computations)} computation annotations failed")

        return results

    def annotate_computations_parallel(
        self,
        task_guid: str,
        computations: list,
        software_cache: dict,
        index: dict,
        llm_model: str,
        temperature: float,
        max_workers: int = 4,
    ) -> List[AnnotatedComputation]:
        """Annotate computations concurrently using async I/O."""
        return run_async(self._annotate_computations_async(
            task_guid, computations, software_cache, index, llm_model, temperature, max_workers,
        ))

    # ------------------------------------------------------------------
    # Step 5: Graph-level synthesis
    # ------------------------------------------------------------------

    def synthesize_graph(
        self,
        task_guid: str,
        root_node: dict,
        step_annotations: List[AnnotatedComputation],
        llm_model: str,
        temperature: float,
    ) -> GraphSynthesisResult:
        """Synthesize graph-level summary from all step annotations."""
        self._update_task(task_guid, {
            "current_step": "SYNTHESIZING",
            "status": "SYNTHESIZING",
        })

        # Build synthesis prompt
        parts = []
        parts.append("## RO-Crate Overview")
        parts.append(f"**Name:** {root_node.get('name', 'Unknown')}")
        parts.append(f"**Description:** {root_node.get('description', 'No description')}")
        parts.append(f"**Author:** {root_node.get('author', 'Unknown')}")
        parts.append(f"**Keywords:** {root_node.get('keywords', '')}")
        parts.append("")

        parts.append("## Step Annotations")
        for i, ann in enumerate(step_annotations, 1):
            parts.append(f"### Step {i}: {ann.annotates}")
            parts.append(f"**Summary:** {ann.stepSummary}")
            if ann.concerns:
                parts.append(f"**Concerns:** {'; '.join(f'[{c.level.value}] {c.description}' for c in ann.concerns)}")
            if ann.codeAnalysis:
                for ca in ann.codeAnalysis:
                    parts.append(f"**Code ({ca.name or ca.software}):** {ca.summary}")
            parts.append("")

        prompt = "\n".join(parts)

        async def _run_synthesis():
            agent = Agent(
                llm_model,
                output_type=GraphSynthesisResult,
                system_prompt=SYNTHESIS_SYSTEM_PROMPT,
                retries=2,
            )
            result = await agent.run(prompt)
            return result.output

        synthesis = run_async(_run_synthesis())

        # Persist raw LLM output for debugging
        self._save_llm_result(
            task_guid,
            "synthesis",
            synthesis.model_dump(mode="json"),
        )

        return synthesis

    # ------------------------------------------------------------------
    # Step 6: Build and store AnnotatedEvidenceGraph
    # ------------------------------------------------------------------

    def build_and_store(
        self,
        task_guid: str,
        rocrate_id: str,
        condensed_id: str,
        graph: list,
        step_annotations: List[AnnotatedComputation],
        synthesis: GraphSynthesisResult,
        llm_model: str,
        temperature: float,
    ) -> str:
        """Assemble, validate, and store the AnnotatedEvidenceGraph."""
        self._update_task(task_guid, {
            "current_step": "STORING",
            "status": "STORING",
        })

        # Build the @graph dict: original entities + annotated computations
        graph_dict = {}
        for node in graph:
            node_id = node.get("@id")
            if node_id:
                graph_dict[node_id] = node

        # Add annotated computations to graph and back-link computations
        for ann in step_annotations:
            ann_dict = ann.model_dump(by_alias=True, exclude_none=True, mode="json")
            graph_dict[ann.guid] = ann_dict

            # Extract comp_id robustly: handle IdentifierValue, dict, or string
            annotates = ann.annotates
            if hasattr(annotates, 'guid'):
                comp_id = annotates.guid
            elif isinstance(annotates, dict):
                comp_id = annotates.get("@id", "")
            else:
                comp_id = str(annotates)

            # Add evi:annotatedBy reverse link on the computation node in the AEG graph
            if comp_id and comp_id in graph_dict:
                existing = graph_dict[comp_id].get("evi:annotatedBy", [])
                if isinstance(existing, dict):
                    existing = [existing]
                elif not isinstance(existing, list):
                    existing = []
                existing.append({"@id": ann.guid})
                graph_dict[comp_id]["evi:annotatedBy"] = existing
            else:
                logger.warning(
                    "Could not back-link annotation %s -> computation %s "
                    "(not found in graph_dict, type=%s)",
                    ann.guid, comp_id, type(annotates).__name__,
                )

        # Build step annotation refs
        step_ann_refs = [{"@id": ann.guid} for ann in step_annotations]

        # Compile graph-level concerns from step annotations with source links
        compiled_concerns = []
        for ann in step_annotations:
            for concern in (ann.concerns or []):
                compiled_concerns.append(GraphConcern(
                    level=concern.level,
                    description=concern.description,
                    sourceAnnotation={"@id": ann.guid},
                ))
        # Add synthesis-level concerns (not tied to a single step)
        for llm_concern in (synthesis.concerns or []):
            normalized = normalize_concern(llm_concern)
            compiled_concerns.append(GraphConcern(
                level=normalized.level,
                description=normalized.description,
                sourceAnnotation={"@id": rocrate_id},
            ))

        # Extract NAAN from rocrate_id for the new identifier
        ark_match = re.match(r"ark:(\d+)/(.*)", rocrate_id)
        if ark_match:
            naan = ark_match.group(1)
            postfix = ark_match.group(2)
            aeg_id = f"ark:{naan}/annotated-eg-{postfix}"
        else:
            aeg_id = f"ark:59853/annotated-eg-{uuid.uuid4()}"

        now = datetime.datetime.utcnow().isoformat()

        # Build the AnnotatedEvidenceGraph
        aeg_data = {
            "@id": aeg_id,
            "name": f"Annotated Evidence Graph: {graph_dict.get(rocrate_id, {}).get('name', rocrate_id)}",
            "description": f"AI-mediated interpretation of RO-Crate {rocrate_id}",
            "author": llm_model,
            "evi:annotates": {"@id": rocrate_id},
            "@graph": graph_dict,
            "evi:executiveSummary": synthesis.executiveSummary,
            "evi:narrativeSummary": synthesis.narrativeSummary,
            "evi:keyFindings": synthesis.keyFindings,
            "evi:concerns": [c.model_dump(by_alias=True, mode="json") for c in compiled_concerns],
            "evi:stepAnnotations": step_ann_refs,
            "evi:llmModel": llm_model,
            "evi:llmTemperature": temperature,
            "dateCreated": now,
        }

        aeg = AnnotatedEvidenceGraph.model_validate(aeg_data)

        # Store as StoredIdentifier
        permissions = Permissions(owner="system@fairscape.org", group="", acl=[])
        stored = StoredIdentifier.model_validate({
            "@id": aeg_id,
            "@type": ["prov:Entity", "https://w3id.org/EVI#EvidenceGraph", "https://w3id.org/EVI#AnnotatedEvidenceGraph"],
            "metadata": aeg.model_dump(by_alias=True, mode="json"),
            "permissions": permissions.model_dump(),
            "publicationStatus": PublicationStatusEnum.DRAFT,
            "dateCreated": datetime.datetime.utcnow(),
            "dateModified": datetime.datetime.utcnow(),
            "distribution": None,
        })

        self.config.identifierCollection.insert_one(
            stored.model_dump(by_alias=True, mode="json")
        )

        # Update original RO-Crate with pointer
        self.config.identifierCollection.update_one(
            {"@id": rocrate_id},
            {"$set": {"metadata.hasAnnotatedEvidenceGraph": {"@id": aeg_id}}}
        )

        # Update each computation's MongoDB document with evi:annotatedBy
        for ann in step_annotations:
            annotates = ann.annotates
            if hasattr(annotates, 'guid'):
                comp_id = annotates.guid
            elif isinstance(annotates, dict):
                comp_id = annotates.get("@id", "")
            else:
                comp_id = str(annotates)

            if comp_id:
                self.config.identifierCollection.update_one(
                    {"@id": comp_id},
                    {"$addToSet": {"metadata.evi:annotatedBy": {"@id": ann.guid}}}
                )

        self._update_task(task_guid, {"annotated_evidence_graph_id": aeg_id})
        logger.info(f"Stored AnnotatedEvidenceGraph {aeg_id}")
        return aeg_id

    # ------------------------------------------------------------------
    # Main orchestrator
    # ------------------------------------------------------------------

    def interpret_rocrate(self, task_guid: str, user_token: str = "") -> str:
        """Main pipeline orchestrator. Returns the AnnotatedEvidenceGraph @id."""
        import os
        # Load task config
        task_doc = self.config.asyncCollection.find_one({"guid": task_guid})
        if not task_doc:
            raise ValueError(f"Task {task_guid} not found")

        rocrate_id = task_doc["rocrate_id"]
        llm_model = task_doc.get("llm_model", "google-gla:gemini-2.5-flash-lite")
        temperature = task_doc.get("llm_temperature", 0.2)

        # # Diagnostic: confirm which API key and model are in use
        # raw_key = os.environ.get("ANTHROPIC_API_KEY", "")
        # masked_key = f"{raw_key[:8]}...{raw_key[-4:]}" if len(raw_key) > 12 else ("(not set)" if not raw_key else "(too short)")
        # logger.info(f"[interpret_rocrate] task={task_guid} model={llm_model!r} ANTHROPIC_API_KEY={masked_key}")

        self._update_task(task_guid, {
            "status": "PROCESSING",
            "time_started": datetime.datetime.utcnow(),
        })

        try:
            # Step 1: Ensure condensed
            graph, condensed_id, root_node = self.ensure_condensed(task_guid, rocrate_id)

            # Step 2: Find computations
            computations, index = self.find_computations(task_guid, graph)

            if not computations:
                raise ValueError(f"No Computation nodes found in RO-Crate {rocrate_id}")

            # Step 3: Pre-fetch software
            # Use token from argument, or fall back to task doc
            effective_token = user_token or task_doc.get("user_token", "")
            software_cache = self.prefetch_all_software(task_guid, computations, index, user_token=effective_token)

            # Step 4: Annotate computations in parallel
            step_annotations = self.annotate_computations_parallel(
                task_guid, computations, software_cache, index, llm_model, temperature,
            )

            # Step 5: Graph-level synthesis
            synthesis = self.synthesize_graph(
                task_guid, root_node, step_annotations, llm_model, temperature,
            )

            # Step 6: Build and store
            aeg_id = self.build_and_store(
                task_guid, rocrate_id, condensed_id, graph,
                step_annotations, synthesis, llm_model, temperature,
            )

            # Success
            self._update_task(task_guid, {
                "status": "SUCCESS",
                "current_step": "COMPLETE",
                "time_finished": datetime.datetime.utcnow(),
            })

            return aeg_id

        except Exception as e:
            import traceback
            logger.error(f"Interpretation failed for task {task_guid}: {e}")
            traceback.print_exc()
            self._update_task(task_guid, {
                "status": "FAILURE",
                "error": {"message": str(e), "error_type": type(e).__name__},
                "time_finished": datetime.datetime.utcnow(),
            })
            raise

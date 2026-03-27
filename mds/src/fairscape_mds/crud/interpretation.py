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
import time
import uuid
import logging
from collections import deque
from typing import Any, Dict, List, Optional, Tuple

import httpx
from pydantic_ai import Agent

from fairscape_mds.models.annotated_computation import (
    AnnotatedComputation, CodeAnalysis, DatasetSummary,
    LLMComputationAnnotation, LLMCodeAnalysis, LLMDatasetSummary,
    Assumption, LLMAssumption, AssumptionImpact, normalize_assumption,
)
from fairscape_mds.models.annotated_evidence_graph import (
    AnnotatedEvidenceGraph, GraphAssumption, AudiencePerspective, DataOverview,
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
# Async Rate Limiter
# ---------------------------------------------------------------------------

class AsyncRateLimiter:
    """Sliding-window rate limiter for async contexts.

    Allows at most `max_requests` within any rolling `window_seconds` period.
    Callers await `acquire()` before making an API call.
    """

    def __init__(self, max_requests: int = 2, window_seconds: float = 10.0):
        self._max = max_requests
        self._window = window_seconds
        self._lock = asyncio.Lock()
        self._timestamps: deque = deque()

    async def acquire(self):
        async with self._lock:
            now = time.monotonic()
            # Evict timestamps outside the window
            while self._timestamps and (now - self._timestamps[0]) >= self._window:
                self._timestamps.popleft()
            # If at capacity, sleep until the oldest timestamp exits the window
            if len(self._timestamps) >= self._max:
                sleep_for = self._window - (now - self._timestamps[0])
                if sleep_for > 0:
                    logger.info(f"Rate limiter: sleeping {sleep_for:.1f}s")
                    await asyncio.sleep(sleep_for)
                self._timestamps.popleft()
            self._timestamps.append(time.monotonic())


MAX_API_RETRIES = 5
API_RETRY_BASE_DELAY = 10.0  # seconds; doubles each retry


async def _run_agent_with_retry(agent, prompt, retries=MAX_API_RETRIES, base_delay=API_RETRY_BASE_DELAY):
    """Run agent.run() with exponential backoff on overloaded/rate-limit errors."""
    for attempt in range(retries + 1):
        try:
            return await agent.run(prompt)
        except Exception as e:
            err_str = str(e)
            is_retryable = "529" in err_str or "overloaded" in err_str.lower() or "rate" in err_str.lower()
            if not is_retryable or attempt == retries:
                raise
            delay = base_delay * (2 ** attempt)
            logger.warning(f"API overloaded (attempt {attempt + 1}/{retries + 1}), retrying in {delay:.0f}s: {err_str[:120]}")
            await asyncio.sleep(delay)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MAX_SOFTWARE_BYTES = 50_000  # 50KB per software entity
MAX_STATS_COLUMNS = 25       # max columns of stats to include per dataset in prompt
MAX_SPLITS = 10              # max splits to include per dataset in prompt
MAX_PROMPT_DATASETS = 3      # max input/output datasets to include per computation prompt
CODE_EXTENSIONS = {".py", ".r", ".R", ".sh", ".pl", ".java", ".scala", ".jl", ".m", ".cpp", ".go", ".rs", ".ipynb", ".md"}
GITHUB_REPO_PATTERN = re.compile(r"https?://github\.com/([^/]+)/([^/]+?)(?:\.git)?(?:/.*)?$")
GITHUB_FILE_PATTERN = re.compile(
    r"https?://github\.com/([^/]+)/([^/]+)/blob/([^/]+)/(.*)"
)

# ---------------------------------------------------------------------------
# System Prompt -- Data Science Persona
# ---------------------------------------------------------------------------

DATASCI_SYSTEM_PROMPT = """You are an analyst making explicit the assumptions that support the claims produced by a computation step in a scientific provenance graph (RO-Crate).

You will receive the computation's metadata, input/output datasets with data profiles (when available), and software source code.

Your task: produce a structured annotation that surfaces the assumptions this step relies on — what the analysis is built on, and what would change the interpretation if it turned out to be wrong.

## What to Look For

### Data Assumptions
What properties of the input data does this step rely on? Consider: completeness, distribution shape, independence of observations, representativeness of the sample, encoding and formatting, absence of systematic missingness.

### Methodological Assumptions
What analytical choices were made, and what do they assume about the problem? Consider: model family appropriateness, distance/similarity metrics, evaluation strategy, splitting procedure, threshold values, handling of confounders.

### Software/Parameter Assumptions
What do the code's defaults, hardcoded values, and library choices assume? Consider: parameter sensitivity, random seed presence, version-dependent behavior, implicit ordering.

## Impact Classification

Every assumption MUST be assigned exactly one impact level:

**CRITICAL** — The entire result rests on this. If this assumption is wrong, the main conclusions do not hold.
Ask: "If this assumption fails, do the conclusions still stand?" If no → CRITICAL.
Examples: training/test independence, outcome variable validity, core statistical model appropriateness.

**MAJOR** — Critical for a subset of results. If wrong, specific results break or change, but other portions of the analysis may still hold.
Ask: "If this assumption fails, do specific results or secondary claims break?" If yes → MAJOR.
Examples: default hyperparameters being adequate, a particular threshold choice, evaluation metric appropriateness for the data.

**MINOR** — Present but unlikely to change the main conclusions. Worth recording for reuse or extension.
Ask: "Would correcting this change the results?" If no → MINOR.
Examples: version pinning, input validation, documentation completeness.

## Key Principles
- State what the assumption IS, not just that something could go wrong. "Assumes input features are independently distributed" is better than "features might be correlated."
- Be specific: name the parameter, the data property, the function.
- Many well-written steps will have only MINOR assumptions. That is valid.
- Do not manufacture assumptions to fill quotas.
- Weave critical assumptions into the stepSummary itself — they are part of the story of what this step does.
- If code is demonstrably wrong (produces misleading results), flag that as a CRITICAL assumption violation.

## Output
- stepSummary: What this step does, why, and what critical assumptions it rests on.
- codeAnalysis: Per software entity — summary, key functions, and assumptions (each with the structured format below).
- inputSummaries: Per input dataset — role, description, dataQuality observations from data profile.
- outputSummaries: Per output dataset — what it contains, dataQuality observations.
- assumptions: Step-level assumptions. May be empty if the step makes no notable assumptions.

Each assumption MUST be a structured object:
{
  impact: "CRITICAL" | "MAJOR" | "MINOR",
  name: "Short label (3-8 words) — e.g. 'Train/test split independence'",
  description: "Briefly describe what is being assumed",
  downstreamImpacts: "What changes if this assumption is wrong — potential downstream effects",
  evidence: { artifact: {"@id": "<@id of the data file or software entity>"}, location: "<line number, function name, or column name>" }
}

Be precise and evidence-based. Reference specific function names and parameter values."""


DATASCI_SYNTHESIS_PROMPT = """You are a senior data scientist synthesizing step annotations from a scientific analysis pipeline (RO-Crate) into a coherent picture of what supports the pipeline's claims.

You will receive the RO-Crate overview and step-by-step annotations including assumptions.

Produce:
1. executiveSummary: 3-5 sentences covering what the pipeline does, its approach, and the most important critical assumptions it rests on. Weave the load-bearing assumptions into this summary.
2. narrativeSummary: A forward-chronological story of the pipeline, explicitly noting where key assumptions enter and what claims they support. The reader should finish this knowing what the results depend on.
3. keyFindings: Bulleted list of important observations about what the pipeline discovered.
4. assumptions: Cross-cutting assumptions that span the pipeline, each as a structured object:
   {impact, name, description, downstreamImpacts}
   - impact: "CRITICAL" (if wrong, pipeline conclusions don't hold), "MAJOR" (if wrong, specific results break but others may hold), or "MINOR" (worth noting but won't change conclusions)
   - name: Short label (3-8 words)
   - description: What is being assumed
   - downstreamImpacts: What changes if this assumption is wrong

Do NOT re-list every step-level assumption. Surface those that matter at the pipeline level — because they span steps, compound across steps, or are the most consequential for trusting the results. A pipeline with no CRITICAL assumptions at the graph level is valid if step-level ones don't compound."""


BIOSTAT_SYNTHESIS_PROMPT = """You are a biostatistician reviewing a scientific analysis pipeline (RO-Crate). Synthesize the step annotations into a perspective focused on statistical rigor and the assumptions that underpin the quantitative claims.

You will receive the RO-Crate overview and step-by-step annotations including assumptions.

Produce:
1. executiveSummary: 3-5 sentences on the pipeline's statistical approach and the critical statistical assumptions it rests on.
2. narrativeSummary: Forward-chronological story emphasizing where statistical assumptions enter — distributional assumptions, independence assumptions, sample size considerations, multiple testing implications, model specification choices. The reader should understand the statistical scaffolding supporting the claims.
3. keyFindings: Bulleted observations focused on statistical methodology — what was done well and what gaps exist.
4. assumptions: Cross-cutting statistical assumptions, each as a structured object:
   {impact, name, description, downstreamImpacts}
   - impact: "CRITICAL" (core statistical assumptions, e.g. distributional assumptions, independence of observations), "MAJOR" (statistical choices that shape specific results, e.g. correction methods, model selection), or "MINOR" (minor statistical notes for reproducibility)
   - name: Short label (3-8 words)
   - description: What is being assumed
   - downstreamImpacts: What changes if this assumption is wrong

Focus on what a statistician reviewing this work would want to verify. Do NOT re-list every step-level assumption."""


CLINICIAN_SYNTHESIS_PROMPT = """You are a clinician reviewing a scientific analysis pipeline (RO-Crate). Synthesize the step annotations into a perspective focused on clinical applicability and what assumptions must hold for these results to inform patient care.

You will receive the RO-Crate overview and step-by-step annotations including assumptions.

Produce:
1. executiveSummary: 3-5 sentences on what clinical question this pipeline addresses and what critical assumptions must hold for the results to be clinically actionable.
2. narrativeSummary: Forward-chronological story emphasizing clinical relevance — what patient population is assumed, what outcome measures are used and whether they map to clinical endpoints, what generalizability assumptions are made. The reader should understand what would need to be true for these results to apply in practice.
3. keyFindings: Bulleted observations focused on clinical applicability — effect sizes, clinical vs statistical significance, population representativeness.
4. assumptions: Cross-cutting clinical assumptions, each as a structured object:
   {impact, name, description, downstreamImpacts}
   - impact: "CRITICAL" (assumptions about patient population, outcome validity, clinical relevance that conclusions rest on), "MAJOR" (assumptions affecting how broadly or confidently specific results can be applied), or "MINOR" (notes for clinical context unlikely to change interpretation)
   - name: Short label (3-8 words)
   - description: What is being assumed
   - downstreamImpacts: What changes if this assumption is wrong

Focus on what a clinician deciding whether to act on these results would want to know. Do NOT re-list every step-level assumption."""


# Audience configuration for synthesis
AUDIENCE_CONFIGS = [
    {
        "key": "biostat",
        "label": "Biostatistician",
        "prompt": BIOSTAT_SYNTHESIS_PROMPT,
    },
    {
        "key": "clinician",
        "label": "Clinician",
        "prompt": CLINICIAN_SYNTHESIS_PROMPT,
    },
]


# ---------------------------------------------------------------------------
# Graph-level synthesis result model (just the fields we need from LLM)
# ---------------------------------------------------------------------------

from pydantic import BaseModel, Field as PydanticField


class GraphSynthesisResult(BaseModel):
    """Result model for the graph-level synthesis LLM call."""
    executiveSummary: str
    narrativeSummary: str
    keyFindings: List[str] = []
    assumptions: List[LLMAssumption] = []


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
# Dataset statistics formatting for prompts
# ---------------------------------------------------------------------------

_HIST_BARS = " ▁▂▃▄▅▆▇█"


def _mini_histogram(counts: list) -> str:
    """Render a list of histogram counts as a sparkline string."""
    if not counts:
        return ""
    mx = max(counts) if max(counts) > 0 else 1
    return "".join(_HIST_BARS[min(int(c / mx * 8) + (1 if c > 0 else 0), 8)] for c in counts)


def _format_column_stats(col_name: str, col_data: dict) -> str:
    """Format one column's stats as a markdown table row."""
    stats = col_data.get("statistics", {})

    # Determine type by presence of 'mean' (numerical) vs 'unique' (categorical)
    if "mean" in stats and stats["mean"] is not None:
        # Numerical
        missing_pct = stats.get("missing_percentage", "")
        if missing_pct != "" and missing_pct is not None:
            missing_pct = f"{missing_pct}%"
        hist = _mini_histogram(stats.get("histogram_counts", []))
        return (
            f"| {col_name} | num | {stats.get('count', '')} | {missing_pct} "
            f"| {stats.get('mean', '')} | {stats.get('std', '')} "
            f"| {stats.get('min', '')} | {stats.get('first_quartile', '')} "
            f"| {stats.get('second_quartile', '')} | {stats.get('third_quartile', '')} "
            f"| {stats.get('max', '')} | {hist} |"
        )
    else:
        # Categorical
        missing_pct = stats.get("missing_percentage", "")
        if missing_pct != "" and missing_pct is not None:
            missing_pct = f"{missing_pct}%"
        return (
            f"| {col_name} | cat | {stats.get('count', '')} | {missing_pct} "
            f"| top: {stats.get('top', '')} | uniq: {stats.get('unique', '')} "
            f"| | | | | | freq: {stats.get('freq', '')} |"
        )


def _format_dataset_stats(ds_name: str, ds_stats: dict) -> str:
    """Format descriptiveStatistics and splitStatistics for one dataset into
    prompt-ready markdown.  Returns empty string if no stats available."""
    desc_stats = ds_stats.get("descriptiveStatistics", {})
    split_stats = ds_stats.get("splitStatistics", {})

    if not desc_stats and not split_stats:
        return ""

    parts = []

    # --- Overall descriptive statistics ---
    if desc_stats:
        columns = list(desc_stats.items())
        total_cols = len(columns)

        # Estimate row count from first column's count
        first_stats = columns[0][1].get("statistics", {}) if columns else {}
        row_count = first_stats.get("count", "?")

        # Total missing across all columns
        total_missing = 0
        for _, col_data in columns:
            mc = col_data.get("statistics", {}).get("missing_count")
            if mc is not None:
                total_missing += mc

        # Skip detailed stats for wide datasets (>10 columns) to keep prompts small
        if total_cols > 10:
            parts.append(f"#### Data Profile: {ds_name} ({total_cols} columns, ~{row_count} rows, {total_missing} missing values)")
            parts.append("*(Column-level statistics omitted for wide dataset)*")
            parts.append("")
        else:
            display_cols = columns

            parts.append(f"#### Data Profile: {ds_name} ({total_cols} columns, ~{row_count} rows, {total_missing} missing values)")
            parts.append("| Column | Type | Count | Missing% | Mean/Top | Std/Unique | Min | Q1 | Median | Q3 | Max | Hist |")
            parts.append("|--------|------|-------|----------|----------|------------|-----|----|----|----|----|------|")
            for col_name, col_data in display_cols:
                parts.append(_format_column_stats(col_name, col_data))
            parts.append("")

    # --- Split statistics ---
    if split_stats:
        split_items = list(split_stats.items())
        total_splits = len(split_items)
        truncated_splits = total_splits > MAX_SPLITS
        display_splits = split_items[:MAX_SPLITS]

        for split_name, split_data in display_splits:
            split_desc = split_data.get("description", "")
            split_col_stats = split_data.get("statistics", {})
            if not split_col_stats:
                continue

            # Row count from first column
            first_split_col = list(split_col_stats.values())[0] if split_col_stats else {}
            split_row_count = first_split_col.get("statistics", {}).get("count", "?")

            label = f'#### Split: "{split_name}"'
            if split_desc:
                label += f" ({split_desc})"
            label += f" -- {split_row_count} rows"
            parts.append(label)

            split_columns = list(split_col_stats.items())

            # Skip detailed split stats for wide datasets
            if len(split_columns) > 10:
                parts.append(f"*({len(split_columns)} columns, stats omitted for wide dataset)*")
                parts.append("")
            else:
                parts.append("| Column | Type | Count | Missing% | Mean/Top | Std/Unique | Min | Q1 | Median | Q3 | Max | Hist |")
                parts.append("|--------|------|-------|----------|----------|------------|-----|----|----|----|----|------|")
                for col_name, col_data in split_columns:
                    parts.append(_format_column_stats(col_name, col_data))
                parts.append("")

        if truncated_splits:
            parts.append(f"*... {total_splits - MAX_SPLITS} more splits omitted*\n")

    return "\n".join(parts)


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
    # Step 3b: Pre-fetch dataset statistics
    # ------------------------------------------------------------------

    def prefetch_dataset_statistics(
        self, task_guid: str, computations: list, index: dict
    ) -> Dict[str, dict]:
        """Fetch descriptiveStatistics and splitStatistics from MongoDB for all
        datasets referenced by computations. Returns {dataset_id: {...}}."""
        # Collect all dataset IDs
        dataset_ids = set()
        for comp in computations:
            for ds_id in _resolve_refs(comp.get("usedDataset")):
                dataset_ids.add(ds_id)
            for ds_id in _resolve_refs(comp.get("generated")):
                dataset_ids.add(ds_id)

        if not dataset_ids:
            return {}

        # Batch query MongoDB with projection
        cursor = self.config.identifierCollection.find(
            {"@id": {"$in": list(dataset_ids)}},
            {"@id": 1, "descriptiveStatistics": 1, "splitStatistics": 1},
        )

        stats_cache: Dict[str, dict] = {}
        for doc in cursor:
            ds_id = doc.get("@id")
            desc_stats = doc.get("descriptiveStatistics")
            split_stats = doc.get("splitStatistics")
            if desc_stats or split_stats:
                stats_cache[ds_id] = {
                    "descriptiveStatistics": desc_stats or {},
                    "splitStatistics": split_stats or {},
                }

        logger.info(
            f"Pre-fetched dataset statistics: {len(stats_cache)} of "
            f"{len(dataset_ids)} datasets have stats"
        )
        return stats_cache

    # ------------------------------------------------------------------
    # Step 4: Annotate single computation
    # ------------------------------------------------------------------

    def _build_computation_prompt(self, computation: dict, software_cache: dict, index: dict, stats_cache: Optional[Dict[str, dict]] = None) -> str:
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

        if stats_cache is None:
            stats_cache = {}

        # Input datasets
        input_refs = _resolve_refs(computation.get("usedDataset"))
        if input_refs:
            truncated_inputs = len(input_refs) > MAX_PROMPT_DATASETS
            display_inputs = input_refs[:MAX_PROMPT_DATASETS]
            parts.append("## Input Datasets")
            for ds_id in display_inputs:
                ds_node = index.get(ds_id, {})
                ds_name = ds_node.get('name', ds_id)
                parts.append(f"- **{ds_name}** ({ds_node.get('format', 'unknown format')})")
                parts.append(f"  ID: {ds_id}")
                parts.append(f"  Description: {ds_node.get('description', 'No description')}")
                if ds_node.get("keywords"):
                    parts.append(f"  Keywords: {', '.join(ds_node['keywords']) if isinstance(ds_node['keywords'], list) else ds_node['keywords']}")
                # Include dataset statistics if available
                if ds_id in stats_cache:
                    formatted = _format_dataset_stats(ds_name, stats_cache[ds_id])
                    if formatted:
                        parts.append("")
                        parts.append(formatted)
            if truncated_inputs:
                parts.append(f"*({len(input_refs) - MAX_PROMPT_DATASETS} more input datasets omitted)*")
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
            truncated_outputs = len(output_refs) > MAX_PROMPT_DATASETS
            display_outputs = output_refs[:MAX_PROMPT_DATASETS]
            parts.append("## Output Datasets")
            for ds_id in display_outputs:
                ds_node = index.get(ds_id, {})
                ds_name = ds_node.get('name', ds_id)
                parts.append(f"- **{ds_name}** ({ds_node.get('format', 'unknown format')})")
                parts.append(f"  ID: {ds_id}")
                parts.append(f"  Description: {ds_node.get('description', 'No description')}")
                # Include dataset statistics if available
                if ds_id in stats_cache:
                    formatted = _format_dataset_stats(ds_name, stats_cache[ds_id])
                    if formatted:
                        parts.append("")
                        parts.append(formatted)
            if truncated_outputs:
                parts.append(f"*({len(output_refs) - MAX_PROMPT_DATASETS} more output datasets omitted)*")
            parts.append("")

        prompt = "\n".join(parts)
        comp_id = computation.get("@id", "unknown")
        est_tokens = len(prompt) // 4
        logger.info(f"Prompt for {comp_id}: ~{est_tokens} tokens estimated ({len(prompt)} chars)")
        return prompt

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
                assumptions=[normalize_assumption(c) for c in (ca.assumptions or [])],
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
                dataQuality=ds.dataQuality,
            )
            for ds in (llm_result.inputSummaries or [])
        ]
        output_summaries = [
            DatasetSummary(
                dataset={"@id": ds.dataset_id},
                name=ds.name,
                role=ds.role,
                description=ds.description,
                dataQuality=ds.dataQuality,
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
            "evi:assumptions": [normalize_assumption(a).model_dump() for a in (llm_result.assumptions or [])],
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
        stats_cache: Optional[Dict[str, dict]] = None,
    ) -> AnnotatedComputation:
        """Annotate a single computation using PydanticAI."""
        prompt = self._build_computation_prompt(computation, software_cache, index, stats_cache=stats_cache)
        comp_id = computation.get("@id", f"ark:59853/computation-{uuid.uuid4()}")

        agent = Agent(
            llm_model,
            output_type=LLMComputationAnnotation,
            system_prompt=DATASCI_SYSTEM_PROMPT,
            retries=3,
        )

        result = await _run_agent_with_retry(agent, prompt)
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
        max_workers: int = 2,
        stats_cache: Optional[Dict[str, dict]] = None,
        rate_limiter: Optional["AsyncRateLimiter"] = None,
    ) -> List[AnnotatedComputation]:
        """Annotate all computations concurrently via asyncio.gather."""
        self._update_task(task_guid, {
            "current_step": "PROMPTING",
            "status": "PROMPTING",
        })

        async def _annotate_one(comp):
            comp_id = comp.get("@id", "unknown")
            if rate_limiter:
                await rate_limiter.acquire()
            try:
                annotated = await self._annotate_single_computation(
                    task_guid, comp, software_cache, index, llm_model, temperature,
                    stats_cache=stats_cache,
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
        max_workers: int = 2,
        stats_cache: Optional[Dict[str, dict]] = None,
        rate_limiter: Optional["AsyncRateLimiter"] = None,
    ) -> List[AnnotatedComputation]:
        """Annotate computations concurrently using async I/O."""
        return run_async(self._annotate_computations_async(
            task_guid, computations, software_cache, index, llm_model, temperature, max_workers,
            stats_cache=stats_cache, rate_limiter=rate_limiter,
        ))

    # ------------------------------------------------------------------
    # Step 5: Graph-level synthesis
    # ------------------------------------------------------------------

    def _build_synthesis_prompt(
        self,
        root_node: dict,
        step_annotations: List[AnnotatedComputation],
    ) -> str:
        """Build the shared synthesis prompt from step annotations."""
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
            if ann.assumptions:
                parts.append(f"**Assumptions:** {'; '.join(f'[{a.impact.value}] {a.description}' for a in ann.assumptions)}")
            if ann.codeAnalysis:
                for ca in ann.codeAnalysis:
                    parts.append(f"**Code ({ca.name or ca.software}):** {ca.summary}")
            parts.append("")

        return "\n".join(parts)

    def synthesize_graph(
        self,
        task_guid: str,
        root_node: dict,
        step_annotations: List[AnnotatedComputation],
        llm_model: str,
        temperature: float,
        rate_limiter: Optional["AsyncRateLimiter"] = None,
    ) -> Tuple[GraphSynthesisResult, List[dict]]:
        """Synthesize graph-level summary from all step annotations.

        Returns (datasci_synthesis, audience_perspectives) where
        audience_perspectives is a list of AudiencePerspective dicts.
        """
        self._update_task(task_guid, {
            "current_step": "SYNTHESIZING",
            "status": "SYNTHESIZING",
        })

        prompt = self._build_synthesis_prompt(root_node, step_annotations)

        async def _run_all_syntheses():
            # Run data scientist + audience syntheses in parallel
            async def _run_one(system_prompt: str, label: str) -> GraphSynthesisResult:
                if rate_limiter:
                    await rate_limiter.acquire()
                agent = Agent(
                    llm_model,
                    output_type=GraphSynthesisResult,
                    system_prompt=system_prompt,
                    retries=2,
                )
                result = await _run_agent_with_retry(agent, prompt)
                self._save_llm_result(task_guid, label, result.output.model_dump(mode="json"))
                return result.output

            tasks = [_run_one(DATASCI_SYNTHESIS_PROMPT, "synthesis:datasci")]
            # for aud in AUDIENCE_CONFIGS:
            #     tasks.append(_run_one(aud["prompt"], f"synthesis:{aud['key']}"))

            return await asyncio.gather(*tasks)

        results = run_async(_run_all_syntheses())

        datasci_synthesis = results[0]
        audience_perspectives = []
        for i, aud in enumerate(AUDIENCE_CONFIGS):
            if i + 1 >= len(results):
                break
            aud_result = results[i + 1]
            audience_perspectives.append({
                "targetAudience": aud["key"],
                "audienceLabel": aud["label"],
                "executiveSummary": aud_result.executiveSummary,
                "narrativeSummary": aud_result.narrativeSummary,
                "keyFindings": aud_result.keyFindings,
                "assumptions_raw": aud_result.assumptions,  # LLMAssumption list, normalized later
            })

        return datasci_synthesis, audience_perspectives

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
        audience_perspectives: List[dict],
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

        # Compile graph-level assumptions from step annotations with source links
        compiled_assumptions = []
        for ann in step_annotations:
            for assumption in (ann.assumptions or []):
                compiled_assumptions.append(GraphAssumption(
                    impact=assumption.impact,
                    name=assumption.name,
                    description=assumption.description,
                    downstreamImpacts=assumption.downstreamImpacts,
                    evidence=assumption.evidence,
                    sourceAnnotation={"@id": ann.guid},
                ))
        # Add synthesis-level assumptions (not tied to a single step)
        for llm_assumption in (synthesis.assumptions or []):
            normalized = normalize_assumption(llm_assumption)
            compiled_assumptions.append(GraphAssumption(
                impact=normalized.impact,
                name=normalized.name,
                description=normalized.description,
                downstreamImpacts=normalized.downstreamImpacts,
                evidence=normalized.evidence,
                sourceAnnotation={"@id": rocrate_id},
            ))

        # Build audience perspectives with normalized assumptions
        audiences = []
        for aud_data in audience_perspectives:
            aud_assumptions = []
            for llm_a in (aud_data.get("assumptions_raw") or []):
                normalized = normalize_assumption(llm_a)
                aud_assumptions.append(GraphAssumption(
                    impact=normalized.impact,
                    name=normalized.name,
                    description=normalized.description,
                    downstreamImpacts=normalized.downstreamImpacts,
                    evidence=normalized.evidence,
                    sourceAnnotation={"@id": rocrate_id},
                ))
            audiences.append(AudiencePerspective(
                targetAudience=aud_data["targetAudience"],
                audienceLabel=aud_data["audienceLabel"],
                executiveSummary=aud_data["executiveSummary"],
                narrativeSummary=aud_data["narrativeSummary"],
                keyFindings=aud_data.get("keyFindings", []),
                assumptions=aud_assumptions,
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

        # Build the overview from RO-Crate metadata
        root_entity = graph_dict.get(rocrate_id, {})
        root_name = root_entity.get("name", "")
        root_desc = root_entity.get("description", "")
        if root_name and root_desc:
            overview_description = f"{root_name}. {root_desc}"
        else:
            overview_description = root_name or root_desc or "No description available"

        # Collect unique data formats from all entities with a format field
        data_formats = sorted({
            entity.get("format")
            for entity in graph_dict.values()
            if isinstance(entity, dict) and entity.get("format")
        })

        # Collect keywords from the root entity
        root_keywords = root_entity.get("keywords", [])
        if isinstance(root_keywords, str):
            root_keywords = [root_keywords]

        # Pick top 1-2 assumptions (CRITICAL first, then MAJOR)
        top_assumptions = [a for a in compiled_assumptions if a.impact == "CRITICAL"][:2]
        if len(top_assumptions) < 2:
            major = [a for a in compiled_assumptions if a.impact == "MAJOR"]
            top_assumptions.extend(major[:2 - len(top_assumptions)])

        overview = DataOverview(
            dataDescription=overview_description,
            dataFormats=data_formats,
            keywords=root_keywords,
            license=root_entity.get("license"),
            conditionsOfAccess=root_entity.get("conditionsOfAccess"),
            topAssumptions=top_assumptions,
        )

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
            "evi:assumptions": [a.model_dump(by_alias=True, mode="json") for a in compiled_assumptions],
            "evi:overview": overview.model_dump(by_alias=True, mode="json"),
            "evi:audiences": [a.model_dump(by_alias=True, mode="json") for a in audiences],
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
        rate_limiter = AsyncRateLimiter(max_requests=2, window_seconds=10.0)

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

            # Step 3b: Pre-fetch dataset statistics
            stats_cache = self.prefetch_dataset_statistics(task_guid, computations, index)

            # Step 4: Annotate computations (paced by rate limiter)
            step_annotations = self.annotate_computations_parallel(
                task_guid, computations, software_cache, index, llm_model, temperature,
                stats_cache=stats_cache, rate_limiter=rate_limiter,
            )

            # Step 5: Graph-level synthesis (paced by rate limiter)
            synthesis, audience_perspectives = self.synthesize_graph(
                task_guid, root_node, step_annotations, llm_model, temperature,
                rate_limiter=rate_limiter,
            )

            # Step 6: Build and store
            aeg_id = self.build_and_store(
                task_guid, rocrate_id, condensed_id, graph,
                step_annotations, synthesis, audience_perspectives,
                llm_model, temperature,
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

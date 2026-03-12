"""
interpretation.py -- AI-Mediated Interpretation Pipeline

Takes an RO-Crate, ensures it is condensed, finds all Computation nodes,
pre-fetches software source code from GitHub, prompts an LLM per computation
(in parallel via ThreadPoolExecutor), synthesizes a graph-level summary,
and stores the resulting AnnotatedEvidenceGraph.
"""

import datetime
import re
import uuid
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

import httpx
from pydantic_ai import Agent

from fairscape_models.annotated_computation import AnnotatedComputation, CodeAnalysis, DatasetSummary
from fairscape_models.annotated_evidence_graph import AnnotatedEvidenceGraph

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
# Constants
# ---------------------------------------------------------------------------

MAX_SOFTWARE_BYTES = 50_000  # 50KB per software entity
CODE_EXTENSIONS = {".py", ".r", ".R", ".sh", ".pl", ".java", ".scala", ".jl", ".m", ".cpp", ".go", ".rs", ".ipynb"}
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

### Output Requirements
Return a structured annotation with:
- stepSummary: A clear description of what this computation step does and why
- codeAnalysis: For each software entity, provide a summary, key functions, and concerns
- inputSummaries: For each input dataset, describe its role
- outputSummaries: For each output dataset, describe what it contains
- concerns: List any methodological, statistical, or reproducibility concerns

Be precise and evidence-based. Reference specific function names and parameter values.
Distinguish critical issues from minor improvements.
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
4. concerns: Bulleted list of methodological, statistical, or reproducibility concerns across the whole pipeline

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
    concerns: List[str] = []


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

    def prefetch_all_software(self, task_guid: str, computations: list, index: dict) -> Dict[str, str]:
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

    def _annotate_single_computation(
        self,
        computation: dict,
        software_cache: dict,
        index: dict,
        llm_model: str,
        temperature: float,
    ) -> AnnotatedComputation:
        """Annotate a single computation using PydanticAI. Runs synchronously."""
        prompt = self._build_computation_prompt(computation, software_cache, index)

        agent = Agent(
            llm_model,
            result_type=AnnotatedComputation,
            system_prompt=DATASCI_SYSTEM_PROMPT,
            retries=2,
        )

        # Build the required fields that PydanticAI can't infer
        comp_id = computation.get("@id", f"ark:59853/computation-{uuid.uuid4()}")
        annotation_id = f"ark:59853/annotated-computation-{uuid.uuid4()}"

        # Run the agent synchronously
        result = agent.run_sync(prompt)
        annotated: AnnotatedComputation = result.data

        # Ensure required fields are set correctly
        if not annotated.guid:
            annotated.guid = annotation_id
        if not annotated.annotates:
            annotated.annotates = {"@id": comp_id}
        annotated.llmModel = llm_model
        annotated.llmTemperature = temperature
        annotated.dateCreated = datetime.datetime.utcnow().isoformat()

        return annotated

    # ------------------------------------------------------------------
    # Step 4b: Parallel computation processing
    # ------------------------------------------------------------------

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
        """Annotate all computations in parallel using ThreadPoolExecutor."""
        self._update_task(task_guid, {
            "current_step": "PROMPTING",
            "status": "PROMPTING",
        })

        results: List[AnnotatedComputation] = []
        errors: List[dict] = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    self._annotate_single_computation,
                    comp, software_cache, index, llm_model, temperature,
                ): comp
                for comp in computations
            }

            for future in as_completed(futures):
                comp = futures[future]
                comp_id = comp.get("@id", "unknown")

                try:
                    annotated = future.result()
                    results.append(annotated)

                    # Update per-computation status
                    self.config.asyncCollection.update_one(
                        {"guid": task_guid, "computation_details.computation_id": comp_id},
                        {"$set": {"computation_details.$.status": "done"}}
                    )
                except Exception as e:
                    logger.error(f"Failed to annotate computation {comp_id}: {e}")
                    errors.append({"computation_id": comp_id, "error": str(e)})

                    self.config.asyncCollection.update_one(
                        {"guid": task_guid, "computation_details.computation_id": comp_id},
                        {"$set": {
                            "computation_details.$.status": "error",
                            "computation_details.$.error": str(e),
                        }}
                    )

                # Increment completed count
                self.config.asyncCollection.update_one(
                    {"guid": task_guid},
                    {"$inc": {"completed_computations": 1}}
                )

        if errors and not results:
            raise RuntimeError(f"All {len(errors)} computation annotations failed. First error: {errors[0]['error']}")

        if errors:
            logger.warning(f"{len(errors)} of {len(computations)} computation annotations failed")

        return results

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
                parts.append(f"**Concerns:** {'; '.join(ann.concerns)}")
            if ann.codeAnalysis:
                for ca in ann.codeAnalysis:
                    parts.append(f"**Code ({ca.name or ca.software}):** {ca.summary}")
            parts.append("")

        prompt = "\n".join(parts)

        agent = Agent(
            llm_model,
            result_type=GraphSynthesisResult,
            system_prompt=SYNTHESIS_SYSTEM_PROMPT,
            retries=2,
        )

        result = agent.run_sync(prompt)
        return result.data

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

        # Add annotated computations to graph
        for ann in step_annotations:
            ann_dict = ann.model_dump(by_alias=True, exclude_none=True, mode="json")
            graph_dict[ann.guid] = ann_dict

        # Build step annotation refs
        step_ann_refs = [{"@id": ann.guid} for ann in step_annotations]

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
            "evi:concerns": synthesis.concerns,
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
            "@type": "AnnotatedEvidenceGraph",
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

        self._update_task(task_guid, {"annotated_evidence_graph_id": aeg_id})
        logger.info(f"Stored AnnotatedEvidenceGraph {aeg_id}")
        return aeg_id

    # ------------------------------------------------------------------
    # Main orchestrator
    # ------------------------------------------------------------------

    def interpret_rocrate(self, task_guid: str) -> str:
        """Main pipeline orchestrator. Returns the AnnotatedEvidenceGraph @id."""
        # Load task config
        task_doc = self.config.asyncCollection.find_one({"guid": task_guid})
        if not task_doc:
            raise ValueError(f"Task {task_guid} not found")

        rocrate_id = task_doc["rocrate_id"]
        llm_model = task_doc.get("llm_model", "google-gla:gemini-2.5-flash")
        temperature = task_doc.get("llm_temperature", 0.2)

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
            software_cache = self.prefetch_all_software(task_guid, computations, index)

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

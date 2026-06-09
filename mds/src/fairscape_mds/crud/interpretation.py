"""interpretation.py -- Server-side thin wrapper for the AI-mediated
interpretation pipeline.

The pipeline itself lives in `fairscape_graph_tools`. This module only
loads the per-task configuration from `asyncCollection`, assembles the
four Mongo-backed adapters from `crud/interpret_adapters.py`, and hands
them to the shared `Interpreter`. Keeping
`interpret_rocrate(task_guid, user_token)` at the same signature lets
`worker.py`'s Celery task continue to call it unchanged.

Module-level re-exports below preserve the import surface that other
callers (notably the existing test suite under
`tests/crud/test_interpretation.py`) expect. The helpers themselves now
live in `fairscape_graph_tools`.
"""

import logging

import httpx  # noqa: F401 -- kept so `patch("...interpretation.httpx.get")` still resolves

from fairscape_graph_tools.condenser import Condenser
from fairscape_graph_tools.interpreter import Interpreter, InterpretConfig
from fairscape_graph_tools.pipeline.github import prefetch_software_code
from fairscape_graph_tools.pipeline.graph_utils import (
    _build_index,
    _is_computation,
    _is_rocrate_root,
    _resolve_refs,
)
from fairscape_graph_tools.pipeline.synthesize import GraphSynthesisResult

from fairscape_mds.crud.fairscape_request import FairscapeRequest
from fairscape_mds.crud.interpret_adapters import (
    MongoGraphSource,
    MongoResultSink,
    MongoTaskTracker,
    ServerSoftwareFetcher,
)

__all__ = [
    "FairscapeInterpretationRequest",
    "GraphSynthesisResult",
    "_build_index",
    "_is_computation",
    "_is_rocrate_root",
    "_resolve_refs",
    "prefetch_software_code",
]

logger = logging.getLogger(__name__)


class FairscapeInterpretationRequest(FairscapeRequest):
    """Thin wrapper: assemble Mongo-backed adapters and dispatch to the
    shared `fairscape_graph_tools.Interpreter`."""

    def interpret_rocrate(self, task_guid: str, user_token: str = "") -> str:
        """Run the interpretation pipeline for the RO-Crate recorded in
        the async task document. Returns the persisted
        AnnotatedEvidenceGraph `@id`.

        Signature is load-bearing: `worker.py`'s Celery task calls this
        positionally with a bearer token for the `/software/download/`
        endpoint."""
        task_doc = self.config.asyncCollection.find_one({"guid": task_guid})
        if not task_doc:
            raise ValueError(f"Task {task_guid} not found")

        rocrate_id = task_doc["rocrate_id"]
        llm_model = task_doc.get("llm_model", "google-gla:gemini-2.5-flash-lite")
        temperature = task_doc.get("llm_temperature", 0.2)
        effective_token = user_token or task_doc.get("user_token", "")

        source = MongoGraphSource(self.config)
        sink = MongoResultSink(self.config)
        tracker = MongoTaskTracker(self.config, task_guid)
        software = ServerSoftwareFetcher(self.config, user_token=effective_token)
        condenser = Condenser(source, sink)

        interpreter = Interpreter(
            graph=source,
            sink=sink,
            tracker=tracker,
            software=software,
            condenser=condenser,
            config=InterpretConfig(llm_model=llm_model, temperature=temperature),
        )
        return interpreter.run_sync(rocrate_id)

"""Compatibility shim -- re-exports from the shared fairscape_interpret package.

The model definitions now live in fairscape_interpret.models.annotated_evidence_graph.
This module is kept so existing `from fairscape_mds.models.annotated_evidence_graph
import X` paths keep working; new code should import from fairscape_interpret directly.
"""

from fairscape_interpret.models.annotated_evidence_graph import (
    ANNOTATED_EVIDENCE_GRAPH_TYPE,
    AnnotatedEvidenceGraph,
    AudiencePerspective,
    DataOverview,
    GraphAssumption,
)

__all__ = [
    "ANNOTATED_EVIDENCE_GRAPH_TYPE",
    "AnnotatedEvidenceGraph",
    "AudiencePerspective",
    "DataOverview",
    "GraphAssumption",
]

"""Compatibility shim -- re-exports from the shared fairscape_graph_tools package.

The model definitions now live in fairscape_graph_tools.models.annotated_computation.
This module is kept so existing `from fairscape_mds.models.annotated_computation
import X` paths keep working; new code should import from fairscape_graph_tools directly.
"""

from fairscape_graph_tools.models.annotated_computation import (
    ANNOTATED_COMPUTATION_TYPE,
    AnnotatedComputation,
    Assumption,
    AssumptionImpact,
    CodeAnalysis,
    ComputationError,
    ComputationReviewStatus,
    DatasetSummary,
    EvidencePointer,
    LLMAssumption,
    LLMCodeAnalysis,
    LLMComputationAnnotation,
    LLMDatasetSummary,
    LLMError,
    normalize_assumption,
    normalize_error,
)

__all__ = [
    "ANNOTATED_COMPUTATION_TYPE",
    "AnnotatedComputation",
    "Assumption",
    "AssumptionImpact",
    "CodeAnalysis",
    "ComputationError",
    "ComputationReviewStatus",
    "DatasetSummary",
    "EvidencePointer",
    "LLMAssumption",
    "LLMCodeAnalysis",
    "LLMComputationAnnotation",
    "LLMDatasetSummary",
    "LLMError",
    "normalize_assumption",
    "normalize_error",
]

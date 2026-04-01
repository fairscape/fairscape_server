"""AnnotatedEvidenceGraph model -- local copy for Docker builds that use
an older fairscape_models release without this module.

Copied from fairscape_models/fairscape_models/annotated_evidence_graph.py
with imports adjusted to use fairscape_models base classes that ARE
present in the published package.
"""

from pydantic import BaseModel, Field, model_validator
from typing import Optional, List, Union, Dict, Any

from fairscape_models.fairscape_base import IdentifierValue
from fairscape_models.digital_object import DigitalObject
from fairscape_mds.models.annotated_computation import AssumptionImpact, EvidencePointer

ANNOTATED_EVIDENCE_GRAPH_TYPE = "AnnotatedEvidenceGraph"


class GraphAssumption(BaseModel):
    """A graph-level assumption linked to its source annotation."""
    impact: AssumptionImpact
    name: str = Field(default="", description="Short label for the assumption")
    description: str
    downstreamImpacts: Optional[str] = Field(default=None, description="What changes if this assumption is wrong")
    evidence: Optional[EvidencePointer] = Field(default=None, description="Pointer to supporting artifact")
    reviewRecommended: bool = Field(default=False, description="True if a scientist should validate this assumption")
    recommendedValidation: Optional[str] = Field(default=None, description="Concrete step a scientist could take to test this assumption")
    sourceAnnotation: IdentifierValue


class DataOverview(BaseModel):
    """Brief top-level overview for quick orientation."""
    dataDescription: str = Field(description="1-2 sentences: what this data is")
    dataFormats: List[str] = Field(default=[], description="File formats found in datasets")
    keywords: List[str] = Field(default=[], description="Keywords from RO-Crate")
    license: Optional[str] = Field(default=None, description="License URL or name")
    conditionsOfAccess: Optional[str] = Field(default=None)
    topAssumptions: List[GraphAssumption] = Field(default=[], description="1-2 most critical assumptions")
    pipelineDescription: Optional[str] = Field(default=None, description="LLM-generated 1-2 sentence description of what the pipeline produces and its key findings")
    pipelineSteps: Optional[List[str]] = Field(default=None, description="Ordered list of pipeline steps following DAG order")


class AudiencePerspective(BaseModel):
    """Audience-specific synthesis of the annotated evidence graph."""
    targetAudience: str
    audienceLabel: str
    executiveSummary: str
    narrativeSummary: str
    keyFindings: Optional[List[str]] = Field(default=[])
    assumptions: Optional[List[GraphAssumption]] = Field(default=[])


class AnnotatedEvidenceGraph(DigitalObject):
    """Full annotated condensed evidence graph -- the graph-level LLM output.

    Contains all original crate entities plus AnnotatedComputation nodes
    in a flat dict keyed by @id. Computation nodes are replaced by their
    annotated supersets. DAG is reconstructable from cross-references
    (generatedBy, usedDataset, evi:annotates, etc.).
    """
    metadataType: Optional[Union[List[str], str]] = Field(
        default=[
            'prov:Entity',
            "https://w3id.org/EVI#EvidenceGraph",
            "https://w3id.org/EVI#AnnotatedEvidenceGraph",
        ],
        alias="@type",
    )
    additionalType: Optional[str] = Field(default=ANNOTATED_EVIDENCE_GRAPH_TYPE)

    # Reference to the original evidence graph or RO-Crate root
    annotates: IdentifierValue = Field(..., alias="evi:annotates")

    # Flat entity lookup -- all entities keyed by ARK @id
    graph: Dict[str, Any] = Field(..., alias="@graph")

    # Graph-level LLM outputs (data scientist perspective — the default)
    executiveSummary: str = Field(..., alias="evi:executiveSummary")
    narrativeSummary: str = Field(..., alias="evi:narrativeSummary")
    keyFindings: Optional[List[str]] = Field(default=[], alias="evi:keyFindings")
    assumptions: Optional[List[GraphAssumption]] = Field(default=[], alias="evi:assumptions")

    # Brief top-level overview (data type, license, top assumptions)
    overview: Optional[DataOverview] = Field(default=None, alias="evi:overview")

    # Audience-specific perspectives (biostatistician, clinician, etc.)
    audiences: Optional[List[AudiencePerspective]] = Field(default=[], alias="evi:audiences")

    # Quick index of all AnnotatedComputation @ids in the graph
    stepAnnotations: Optional[List[IdentifierValue]] = Field(default=[], alias="evi:stepAnnotations")

    # Provenance of the graph-level analysis
    llmModel: str = Field(alias="evi:llmModel")
    llmTemperature: Optional[float] = Field(default=None, alias="evi:llmTemperature")
    dateCreated: str
    interpreterVersion: Optional[str] = Field(default=None, alias="evi:interpreterVersion")

    @model_validator(mode='after')
    def populate_prov_fields(self):
        """Auto-populate PROV-O fields."""
        self.wasDerivedFrom = [self.annotates]
        self.wasAttributedTo = [self.llmModel]
        return self

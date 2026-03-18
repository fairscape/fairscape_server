"""AnnotatedComputation model -- local copy for Docker builds that use
an older fairscape_models release without this module.

Copied from fairscape_models/fairscape_models/annotated_computation.py
with imports adjusted to use fairscape_models base classes that ARE
present in the published package.
"""

from enum import Enum

from pydantic import BaseModel, Field, ConfigDict, model_validator
from typing import Optional, List, Union, Dict, Any

# These base classes exist in the published fairscape_models package
from fairscape_models.fairscape_base import IdentifierValue
from fairscape_models.digital_object import DigitalObject

ANNOTATED_COMPUTATION_TYPE = "AnnotatedComputation"


# ---------------------------------------------------------------------------
# Concern severity levels
# ---------------------------------------------------------------------------

class ConcernLevel(str, Enum):
    """Exactly three severity levels for annotation concerns."""
    CRITICAL = "CRITICAL"
    MODERATE = "MODERATE"
    MINOR = "MINOR"


class Concern(BaseModel):
    """A structured concern with a severity level."""
    level: ConcernLevel
    description: str


class LLMConcern(BaseModel):
    """What the LLM returns for a concern (level as str for flexibility)."""
    level: str = Field(description="One of: CRITICAL, MODERATE, MINOR")
    description: str


def normalize_concern(llm_concern: LLMConcern) -> Concern:
    """Convert an LLMConcern to a validated Concern, normalizing the level."""
    raw = llm_concern.level.strip().upper()
    try:
        level = ConcernLevel(raw)
    except ValueError:
        # Fallback: map common alternatives
        if "CRIT" in raw:
            level = ConcernLevel.CRITICAL
        elif "MOD" in raw or "WARN" in raw:
            level = ConcernLevel.MODERATE
        else:
            level = ConcernLevel.MINOR
    return Concern(level=level, description=llm_concern.description)


class CodeAnalysis(BaseModel):
    """Analysis of a software entity used in the computation."""
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    software: IdentifierValue
    name: Optional[str] = Field(default=None)
    summary: str
    keyFunctions: Optional[List[str]] = Field(default=None)
    concerns: Optional[List[Concern]] = Field(default=None)


class DatasetSummary(BaseModel):
    """Summary of a dataset's role in the computation."""
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    dataset: IdentifierValue
    name: Optional[str] = Field(default=None)
    role: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    dataQuality: Optional[str] = Field(default=None)


# ---------------------------------------------------------------------------
# Lightweight LLM output models (no infrastructure fields the LLM can't know)
# ---------------------------------------------------------------------------

class LLMCodeAnalysis(BaseModel):
    """What the LLM returns for a software entity analysis."""
    software_id: str = Field(description="The @id of the software entity being analyzed")
    name: Optional[str] = Field(default=None)
    summary: str
    keyFunctions: Optional[List[str]] = Field(default=None)
    concerns: Optional[List[LLMConcern]] = Field(default=None)


class LLMDatasetSummary(BaseModel):
    """What the LLM returns for a dataset summary."""
    dataset_id: str = Field(description="The @id of the dataset")
    name: Optional[str] = Field(default=None)
    role: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    dataQuality: Optional[str] = Field(default=None, description="Data quality observations based on summary statistics")


class LLMComputationAnnotation(BaseModel):
    """Lightweight model for LLM output — only the fields the LLM should fill."""
    stepSummary: str
    codeAnalysis: Optional[List[LLMCodeAnalysis]] = Field(default=[])
    inputSummaries: Optional[List[LLMDatasetSummary]] = Field(default=[])
    outputSummaries: Optional[List[LLMDatasetSummary]] = Field(default=[])
    concerns: Optional[List[LLMConcern]] = Field(default=[])


class AnnotatedComputation(DigitalObject):
    """LLM-generated annotation of a single evi:Computation step.

    A DigitalObject (Document) that annotates an evi:Computation.
    The original Computation stays in the graph in its original form;
    this annotation points to it via evi:annotates.
    """
    metadataType: Optional[Union[List[str], str]] = Field(
        default=[
            'prov:Entity',
            "https://w3id.org/EVI#Annotation",
            "https://w3id.org/EVI#AnnotatedComputation",
        ],
        alias="@type",
    )
    additionalType: Optional[str] = Field(default=ANNOTATED_COMPUTATION_TYPE)

    # Points to the original Computation this annotates
    annotates: IdentifierValue = Field(..., alias="evi:annotates")

    # LLM-generated content
    stepSummary: str = Field(..., alias="evi:stepSummary")
    codeAnalysis: Optional[List[CodeAnalysis]] = Field(default=[], alias="evi:codeAnalysis")
    inputSummaries: Optional[List[DatasetSummary]] = Field(default=[], alias="evi:inputSummaries")
    outputSummaries: Optional[List[DatasetSummary]] = Field(default=[], alias="evi:outputSummaries")
    concerns: Optional[List[Concern]] = Field(default=[], alias="evi:concerns")

    # Provenance of the annotation itself
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

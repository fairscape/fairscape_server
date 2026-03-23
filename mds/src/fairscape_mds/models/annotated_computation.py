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
# Assumption impact levels
# ---------------------------------------------------------------------------

class AssumptionImpact(str, Enum):
    """How much this assumption matters for trusting/reusing results."""
    CRITICAL = "CRITICAL"
    MAJOR = "MAJOR"
    MINOR = "MINOR"


class EvidencePointer(BaseModel):
    """Points to the specific artifact and location supporting an assumption."""
    model_config = ConfigDict(extra="allow")

    artifact: IdentifierValue = Field(description='{"@id": "ark:..."} pointing to data file or software')
    location: Optional[str] = Field(default=None, description="Specific location, e.g. line number, function name, column name")

    @model_validator(mode='before')
    @classmethod
    def normalize_artifact(cls, values):
        """Accept various LLM artifact formats and normalize to IdentifierValue."""
        if not isinstance(values, dict):
            return values
        artifact = values.get("artifact")
        if artifact is None:
            return values
        if isinstance(artifact, str):
            values["artifact"] = {"@id": artifact}
        elif isinstance(artifact, dict) and "@id" not in artifact:
            # LLM may return {guid: "ark:..."} or {id: "ark:..."} etc
            ark_id = (
                artifact.get("guid")
                or artifact.get("id")
                or artifact.get("@id")
                or next(iter(artifact.values()), None)
            )
            if ark_id:
                values["artifact"] = {"@id": str(ark_id)}
        return values


class Assumption(BaseModel):
    """A structured assumption with an impact level."""
    impact: AssumptionImpact
    name: str = Field(default="", description="Short label for the assumption (3-8 words)")
    description: str
    downstreamImpacts: Optional[str] = Field(default=None, description="What changes if this assumption is wrong")
    evidence: Optional[EvidencePointer] = Field(default=None, description="Pointer to supporting artifact")


class LLMAssumption(BaseModel):
    """What the LLM returns for an assumption (impact as str for flexibility)."""
    impact: str = Field(description="One of: CRITICAL, MAJOR, MINOR")
    name: str = Field(default="", description="Short label for this assumption (3-8 words)")
    description: str
    downstreamImpacts: Optional[str] = Field(default=None, description="What changes downstream if this assumption is wrong")
    evidence: Optional[dict] = Field(default=None, description='Pointer: {artifact: {"@id": "ark:..."}, location: "..."}')


def normalize_assumption(llm_assumption: LLMAssumption) -> Assumption:
    """Convert an LLMAssumption to a validated Assumption, normalizing the impact."""
    raw = llm_assumption.impact.strip().upper()
    try:
        impact = AssumptionImpact(raw)
    except ValueError:
        # Fallback: map common alternatives, old concern levels, and legacy names
        if "CRIT" in raw or "FOUND" in raw:
            impact = AssumptionImpact.CRITICAL
        elif "MAJ" in raw or "CONSEQ" in raw or "SIG" in raw or "MOD" in raw or "WARN" in raw:
            impact = AssumptionImpact.MAJOR
        else:
            impact = AssumptionImpact.MINOR

    evidence = None
    if llm_assumption.evidence and isinstance(llm_assumption.evidence, dict):
        try:
            evidence = EvidencePointer.model_validate(llm_assumption.evidence)
        except Exception:
            # If evidence can't be parsed, skip it rather than failing
            pass

    return Assumption(
        impact=impact,
        name=getattr(llm_assumption, "name", "") or "",
        description=llm_assumption.description,
        downstreamImpacts=getattr(llm_assumption, "downstreamImpacts", None),
        evidence=evidence,
    )


class CodeAnalysis(BaseModel):
    """Analysis of a software entity used in the computation."""
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    software: IdentifierValue
    name: Optional[str] = Field(default=None)
    summary: str
    keyFunctions: Optional[List[str]] = Field(default=None)
    assumptions: Optional[List[Assumption]] = Field(default=None)


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
    assumptions: Optional[List[LLMAssumption]] = Field(default=None)


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
    assumptions: Optional[List[LLMAssumption]] = Field(default=[])


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
    assumptions: Optional[List[Assumption]] = Field(default=[], alias="evi:assumptions")

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

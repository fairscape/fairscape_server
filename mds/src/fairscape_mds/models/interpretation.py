from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
import datetime


class ComputationProgress(BaseModel):
    """Track progress of a single computation annotation."""
    computation_id: str
    name: str = ""
    status: str = "pending"  # pending, processing, done, error
    error: Optional[str] = None


class InterpretationTask(BaseModel):
    guid: str = Field(alias="guid")
    task_type: str = Field(default="InterpretROCrate")
    rocrate_id: str
    owner_email: str
    status: str = Field(default="PENDING")

    # Pipeline progress
    current_step: str = Field(default="PENDING")
    # Steps: CONDENSING, TRAVERSING, PREFETCHING, PROMPTING, SYNTHESIZING, STORING

    # Computation-level progress (for parallel step)
    total_computations: int = 0
    completed_computations: int = 0
    computation_details: List[Dict[str, Any]] = Field(default_factory=list)

    # Results
    condensed_rocrate_id: Optional[str] = None
    annotated_evidence_graph_id: Optional[str] = None

    # Timing
    time_created: datetime.datetime = Field(default_factory=datetime.datetime.utcnow)
    time_started: Optional[datetime.datetime] = None
    time_finished: Optional[datetime.datetime] = None

    # Config
    llm_model: str = "gemini-2.5-flash"
    llm_temperature: float = 0.2
    persona: str = "datasci"

    error: Optional[Dict[str, Any]] = None

    class Config:
        populate_by_name = True

from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
import datetime


class LLMAssistTask(BaseModel):
    guid: str = Field(alias="@id")
    task_type: str = Field(default="LLMAssist")
    owner_email: str
    filenames: List[str] = Field(default_factory=list)
    document_texts: List[str] = Field(default_factory=list)
    status: str = Field(default="PENDING")
    time_created: datetime.datetime = Field(default_factory=datetime.datetime.utcnow)
    time_started: Optional[datetime.datetime] = None
    time_finished: Optional[datetime.datetime] = None
    result: Optional[str] = None
    error: Optional[Dict[str, Any]] = None
    input_dataset_ark: Optional[str] = None
    output_dataset_ark: Optional[str] = None
    computation_ark: Optional[str] = None

    class Config:
        populate_by_name = True


class D4DFromIssueRequest(BaseModel):
    issue_number: int
    issue_title: str
    issue_body: str
    issue_comments: List[Dict[str, Any]] = Field(default_factory=list)
    yaml_url: str
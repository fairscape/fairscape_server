from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
import datetime


class LLMAssistTask(BaseModel):
    guid: str = Field(alias="@id")
    task_type: str = Field(default="LLMAssist")
    owner_email: str
    file_paths: List[str] = Field(default_factory=list)
    document_texts: List[str] = Field(default=[""])
    status: str = Field(default="PENDING")
    time_created: datetime.datetime = Field(default_factory=datetime.datetime.utcnow)
    time_started: Optional[datetime.datetime] = None
    time_finished: Optional[datetime.datetime] = None
    result: Optional[str] = None
    error: Optional[Dict[str, Any]] = None

    class Config:
        populate_by_name = True
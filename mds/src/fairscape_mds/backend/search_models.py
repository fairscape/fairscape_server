# File: mds/src/fairscape_mds/backend/search_models.py
from pydantic import BaseModel, Field
from typing import List, Optional, Any

class SearchResultItem(BaseModel):
    id: str = Field(alias="@id")
    type: Optional[Any] = Field(alias="@type", default=None)
    name: Optional[str] = None
    description: Optional[str] = None
    keywords: List[str] = Field(default_factory=list)
    score: float

class SearchResults(BaseModel):
    query: str
    total_results: int
    results: List[SearchResultItem]
    time_taken_ms: float
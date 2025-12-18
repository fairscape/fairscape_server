from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime


class GitHubComment(BaseModel):
    """Model for GitHub issue comments"""
    id: int
    user: str
    body: str
    created_at: datetime
    updated_at: datetime


class GitHubIssue(BaseModel):
    """Model for GitHub issues"""
    number: int
    title: str
    body: Optional[str] = None
    state: str
    created_at: datetime
    updated_at: datetime
    user: str
    labels: List[str] = Field(default_factory=list)
    comments_count: Optional[int] = None


class GitHubIssueWithComments(GitHubIssue):
    """Model for GitHub issue with full comment details"""
    comments: List[GitHubComment] = Field(default_factory=list)


class CreateIssueRequest(BaseModel):
    """Request model for creating a GitHub issue"""
    title: str
    body: Optional[str] = ""
    labels: List[str] = Field(default_factory=list)


class AddCommentRequest(BaseModel):
    """Request model for adding a comment to an issue"""
    body: str


class UpdateIssueRequest(BaseModel):
    """Request model for updating a GitHub issue"""
    state: Optional[str] = None
    title: Optional[str] = None
    body: Optional[str] = None
    labels: Optional[List[str]] = None


class GitHubIssueTracker(BaseModel):
    """Model for tracking GitHub issues in MongoDB"""
    guid: str = Field(alias="@id")
    issue_number: int
    github_url: str
    creator_email: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
    title: str

    class Config:
        populate_by_name = True


class UploadedFile(BaseModel):
    """Model for tracking uploaded files"""
    filename: str
    url: str


class FileUpdateResponse(BaseModel):
    """Response model for file updates"""
    success: bool
    file_path: str
    commit_sha: str
    commit_url: str
    raw_url: str

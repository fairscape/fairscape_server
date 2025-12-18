from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form, Query, Body, status
from fastapi.responses import JSONResponse
from typing import Annotated, List, Optional

from fairscape_mds.models.user import UserWriteModel
from fairscape_mds.models.github import AddCommentRequest, UpdateIssueRequest
from fairscape_mds.crud.github import FairscapeGitHubRequest
from fairscape_mds.deps import getCurrentUser
from fairscape_mds.core.config import appConfig, settings


router = APIRouter(
    prefix="/github",
    tags=["GitHub"]
)


# Initialize GitHub request handler with token from settings
try:
    github_request_handler = FairscapeGitHubRequest(
        appConfig,
        settings.GITHUB_TOKEN,
        settings.GITHUB_REPO_NAME
    )
except ValueError as e:
    # If GitHub token is not configured, the endpoints will fail gracefully
    github_request_handler = None


@router.get(
    "/issues",
    summary="List GitHub issues",
    description="Get a list of GitHub issues with optional filtering by state and labels"
)
def list_issues(
    state: str = Query(default='open', description="Issue state: open, closed, or all"),
    labels: List[str] = Query(default=[], description="Filter by label names"),
    current_user: Annotated[UserWriteModel, Depends(getCurrentUser)] = None
):
    """
    List GitHub issues from the configured repository.

    Requires authentication.
    """
    if github_request_handler is None:
        raise HTTPException(
            status_code=503,
            detail="GitHub integration is not configured. Please set GITHUB_TOKEN environment variable."
        )

    response = github_request_handler.list_issues(state=state, labels=labels)

    if not response.success:
        raise HTTPException(status_code=response.statusCode, detail=response.error)

    return response.model


@router.get(
    "/issues/{issue_number}",
    summary="Get a specific GitHub issue",
    description="Get detailed information about a GitHub issue including all comments"
)
def get_issue(
    issue_number: int,
    current_user: Annotated[UserWriteModel, Depends(getCurrentUser)] = None
):
    """
    Get a specific GitHub issue with all its comments.

    Requires authentication.
    """
    if github_request_handler is None:
        raise HTTPException(
            status_code=503,
            detail="GitHub integration is not configured. Please set GITHUB_TOKEN environment variable."
        )

    response = github_request_handler.get_issue(issue_number)

    if not response.success:
        raise HTTPException(status_code=response.statusCode, detail=response.error)

    return response.model


@router.post(
    "/issues",
    status_code=status.HTTP_201_CREATED,
    summary="Create a new GitHub issue",
    description="Create a new issue in the GitHub repository with optional file attachments"
)
def create_issue(
    title: str = Form(..., description="Issue title"),
    body: str = Form(default="", description="Issue description"),
    labels: List[str] = Form(default=[], description="Issue labels"),
    files: List[UploadFile] = File(default=[], description="Files to attach to the issue"),
    current_user: Annotated[UserWriteModel, Depends(getCurrentUser)] = None
):
    """
    Create a new GitHub issue.

    Files will be uploaded to the repository and linked in the issue body.
    The issue creation will be tracked in MongoDB with the creator's email.

    Requires authentication.
    """
    if github_request_handler is None:
        raise HTTPException(
            status_code=503,
            detail="GitHub integration is not configured. Please set GITHUB_TOKEN environment variable."
        )

    response = github_request_handler.create_issue(
        title=title,
        body=body,
        labels=labels,
        files=files,
        current_user=current_user
    )

    if not response.success:
        raise HTTPException(status_code=response.statusCode, detail=response.error)

    return response.model


@router.post(
    "/issues/{issue_number}/comments",
    status_code=status.HTTP_201_CREATED,
    summary="Add a comment to an issue",
    description="Add a new comment to an existing GitHub issue"
)
def add_comment(
    issue_number: int,
    comment_data: AddCommentRequest,
    current_user: Annotated[UserWriteModel, Depends(getCurrentUser)] = None
):
    """
    Add a comment to an existing GitHub issue.

    Requires authentication.
    """
    if github_request_handler is None:
        raise HTTPException(
            status_code=503,
            detail="GitHub integration is not configured. Please set GITHUB_TOKEN environment variable."
        )

    response = github_request_handler.add_comment(
        issue_number=issue_number,
        body=comment_data.body
    )

    if not response.success:
        raise HTTPException(status_code=response.statusCode, detail=response.error)

    return response.model


@router.patch(
    "/issues/{issue_number}",
    summary="Update a GitHub issue",
    description="Update an existing GitHub issue's state, title, body, or labels"
)
def update_issue(
    issue_number: int,
    update_data: UpdateIssueRequest,
    current_user: Annotated[UserWriteModel, Depends(getCurrentUser)] = None
):
    """
    Update an existing GitHub issue.

    Only the fields provided in the request will be updated.

    Requires authentication.
    """
    if github_request_handler is None:
        raise HTTPException(
            status_code=503,
            detail="GitHub integration is not configured. Please set GITHUB_TOKEN environment variable."
        )

    response = github_request_handler.update_issue(
        issue_number=issue_number,
        state=update_data.state,
        title=update_data.title,
        body=update_data.body,
        labels=update_data.labels
    )

    if not response.success:
        raise HTTPException(status_code=response.statusCode, detail=response.error)

    return response.model


@router.put(
    "/files/update",
    summary="Update a file in GitHub",
    description="Update a file in any GitHub repository using its URL"
)
def update_file(
    file_url: str = Form(..., description="GitHub URL to the file (web or raw URL)"),
    file: UploadFile = File(..., description="New file content to upload"),
    commit_message: Optional[str] = Form(default=None, description="Optional custom commit message"),
    current_user: Annotated[UserWriteModel, Depends(getCurrentUser)] = None
):
    """
    Update a file in a GitHub repository using its URL.

    Accepts both web view URLs (github.com/.../blob/...) and raw URLs (raw.githubusercontent.com/...).

    The file URL should follow one of these formats:
    - https://github.com/{owner}/{repo}/blob/{branch}/{file_path}
    - https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{file_path}

    Returns the commit details and new raw URL.

    Requires authentication.
    """
    if github_request_handler is None:
        raise HTTPException(
            status_code=503,
            detail="GitHub integration is not configured. Please set GITHUB_TOKEN environment variable."
        )

    response = github_request_handler.update_file(
        file_url=file_url,
        new_file=file,
        commit_message=commit_message,
        current_user=current_user
    )

    if not response.success:
        raise HTTPException(status_code=response.statusCode, detail=response.error)

    return response.model

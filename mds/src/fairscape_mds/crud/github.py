from typing import List, Optional
from datetime import datetime
from werkzeug.utils import secure_filename
from github import Github, Auth, GithubException
from fastapi import UploadFile
import re

from fairscape_mds.crud.fairscape_request import FairscapeRequest
from fairscape_mds.crud.fairscape_response import FairscapeResponse
from fairscape_mds.models.user import UserWriteModel
from fairscape_mds.models.github import (
    GitHubIssue,
    GitHubComment,
    GitHubIssueWithComments,
    GitHubIssueTracker,
    UploadedFile,
    FileUpdateResponse
)


class FairscapeGitHubRequest(FairscapeRequest):
    """CRUD operations for GitHub issues integration"""

    def __init__(self, config, github_token: str, repo_name: str):
        super().__init__(config)
        if not github_token:
            raise ValueError("GitHub token is required but not provided")
        auth = Auth.Token(github_token)
        self.github = Github(auth=auth)
        self.repo_name = repo_name

    def get_repo(self):
        """Get the GitHub repository instance"""
        return self.github.get_repo(self.repo_name)

    def parse_github_url(self, url: str) -> dict:
        """
        Parse a GitHub file URL to extract repo, branch, and file path

        Args:
            url: GitHub URL (either web view or raw URL)

        Returns:
            dict with 'owner', 'repo', 'branch', and 'file_path'

        Raises:
            ValueError if URL format is invalid
        """
        # Pattern for web view: https://github.com/{owner}/{repo}/blob/{branch}/{file_path}
        web_pattern = r'https://github\.com/([^/]+)/([^/]+)/blob/([^/]+)/(.+)'

        # Pattern for raw view: https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{file_path}
        raw_pattern = r'https://raw\.githubusercontent\.com/([^/]+)/([^/]+)/([^/]+)/(.+)'

        web_match = re.match(web_pattern, url)
        if web_match:
            owner, repo, branch, file_path = web_match.groups()
            return {
                'owner': owner,
                'repo': repo,
                'branch': branch,
                'file_path': file_path
            }

        raw_match = re.match(raw_pattern, url)
        if raw_match:
            owner, repo, branch, file_path = raw_match.groups()
            return {
                'owner': owner,
                'repo': repo,
                'branch': branch,
                'file_path': file_path
            }

        raise ValueError(
            "Invalid GitHub URL format. Expected format: "
            "https://github.com/{owner}/{repo}/blob/{branch}/{file_path} or "
            "https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{file_path}"
        )

    def list_issues(
        self,
        state: str = 'open',
        labels: Optional[List[str]] = None
    ) -> FairscapeResponse:
        """
        List GitHub issues with optional filtering

        Args:
            state: Issue state ('open', 'closed', 'all')
            labels: Optional list of label names to filter by

        Returns:
            FairscapeResponse with list of issues
        """
        try:
            repo = self.get_repo()

            if labels:
                issues = repo.get_issues(state=state, labels=labels)
            else:
                issues = repo.get_issues(state=state)

            issues_list = []
            for issue in issues:
                issue_data = GitHubIssue(
                    number=issue.number,
                    title=issue.title,
                    body=issue.body,
                    state=issue.state,
                    created_at=issue.created_at,
                    updated_at=issue.updated_at,
                    user=issue.user.login,
                    labels=[label.name for label in issue.labels],
                    comments_count=issue.comments
                )
                issues_list.append(issue_data.model_dump())

            return FairscapeResponse(
                success=True,
                statusCode=200,
                model={'issues': issues_list}
            )

        except GithubException as e:
            return FairscapeResponse(
                success=False,
                statusCode=e.status,
                error={'message': str(e)}
            )
        except Exception as e:
            return FairscapeResponse(
                success=False,
                statusCode=500,
                error={'message': f'Error listing issues: {str(e)}'}
            )

    def get_issue(self, issue_number: int) -> FairscapeResponse:
        """
        Get a single GitHub issue with all comments

        Args:
            issue_number: The issue number

        Returns:
            FairscapeResponse with issue details and comments
        """
        try:
            repo = self.get_repo()
            issue = repo.get_issue(issue_number)

            comments_list = []
            for comment in issue.get_comments():
                comment_data = GitHubComment(
                    id=comment.id,
                    user=comment.user.login,
                    body=comment.body,
                    created_at=comment.created_at,
                    updated_at=comment.updated_at
                )
                comments_list.append(comment_data.model_dump())

            issue_data = GitHubIssueWithComments(
                number=issue.number,
                title=issue.title,
                body=issue.body,
                state=issue.state,
                created_at=issue.created_at,
                updated_at=issue.updated_at,
                user=issue.user.login,
                labels=[label.name for label in issue.labels],
                comments_count=issue.comments,
                comments=comments_list
            )

            return FairscapeResponse(
                success=True,
                statusCode=200,
                model=issue_data.model_dump()
            )

        except GithubException as e:
            return FairscapeResponse(
                success=False,
                statusCode=e.status,
                error={'message': str(e)}
            )
        except Exception as e:
            return FairscapeResponse(
                success=False,
                statusCode=500,
                error={'message': f'Error getting issue: {str(e)}'}
            )

    def create_issue(
        self,
        title: str,
        body: str,
        labels: List[str],
        files: List[UploadFile],
        current_user: UserWriteModel
    ) -> FairscapeResponse:
        """
        Create a new GitHub issue with optional file attachments

        Args:
            title: Issue title
            body: Issue body/description
            labels: List of label names
            files: List of files to upload
            current_user: The user creating the issue

        Returns:
            FairscapeResponse with created issue details
        """
        try:
            repo = self.get_repo()

            # Create the issue
            issue = repo.create_issue(title=title, body=body, labels=labels)

            # Handle file uploads if any
            uploaded_files = []
            if files:
                for file in files:
                    if file and file.filename:
                        filename = secure_filename(file.filename)
                        file_content = file.file.read()

                        file_path = f"d4d_uploads/issue_{issue.number}/{filename}"

                        try:
                            # Try to get existing file
                            existing_file = repo.get_contents(file_path)
                            repo.update_file(
                                file_path,
                                f"Update attachment for issue #{issue.number}",
                                file_content,
                                existing_file.sha
                            )
                        except:
                            # Create new file
                            repo.create_file(
                                file_path,
                                f"Add attachment for issue #{issue.number}",
                                file_content
                            )

                        raw_url = f"https://raw.githubusercontent.com/{self.repo_name}/main/{file_path}"
                        uploaded_files.append(
                            UploadedFile(filename=filename, url=raw_url).model_dump()
                        )

                # Update issue body with file attachments
                if uploaded_files:
                    updated_body = body + "\n\n---\n**Attachments:**\n"
                    for file_info in uploaded_files:
                        updated_body += f"\n- [{file_info['filename']}]({file_info['url']})"

                    issue.edit(body=updated_body)

            # Track the issue in MongoDB
            issue_tracker = GitHubIssueTracker(
                guid=f"github-issue-{issue.number}",
                issue_number=issue.number,
                github_url=issue.html_url,
                creator_email=current_user.email,
                title=title
            )

            self.config.asyncCollection.insert_one(
                issue_tracker.model_dump(by_alias=True, mode="json")
            )

            return FairscapeResponse(
                success=True,
                statusCode=201,
                model={
                    'number': issue.number,
                    'title': issue.title,
                    'state': issue.state,
                    'url': issue.html_url,
                    'uploaded_files': uploaded_files
                }
            )

        except GithubException as e:
            return FairscapeResponse(
                success=False,
                statusCode=e.status,
                error={'message': str(e)}
            )
        except Exception as e:
            return FairscapeResponse(
                success=False,
                statusCode=500,
                error={'message': f'Error creating issue: {str(e)}'}
            )

    def add_comment(self, issue_number: int, body: str) -> FairscapeResponse:
        """
        Add a comment to a GitHub issue

        Args:
            issue_number: The issue number
            body: Comment text

        Returns:
            FairscapeResponse with comment details
        """
        try:
            repo = self.get_repo()
            issue = repo.get_issue(issue_number)

            comment = issue.create_comment(body)

            comment_data = GitHubComment(
                id=comment.id,
                user=comment.user.login,
                body=comment.body,
                created_at=comment.created_at,
                updated_at=comment.updated_at
            )

            return FairscapeResponse(
                success=True,
                statusCode=201,
                model=comment_data.model_dump()
            )

        except GithubException as e:
            return FairscapeResponse(
                success=False,
                statusCode=e.status,
                error={'message': str(e)}
            )
        except Exception as e:
            return FairscapeResponse(
                success=False,
                statusCode=500,
                error={'message': f'Error adding comment: {str(e)}'}
            )

    def update_issue(
        self,
        issue_number: int,
        state: Optional[str] = None,
        title: Optional[str] = None,
        body: Optional[str] = None,
        labels: Optional[List[str]] = None
    ) -> FairscapeResponse:
        """
        Update a GitHub issue

        Args:
            issue_number: The issue number
            state: New state ('open' or 'closed')
            title: New title
            body: New body
            labels: New labels list

        Returns:
            FairscapeResponse with updated issue details
        """
        try:
            repo = self.get_repo()
            issue = repo.get_issue(issue_number)

            # Update only the fields that were provided
            if state is not None:
                issue.edit(state=state)

            if title is not None:
                issue.edit(title=title)

            if body is not None:
                issue.edit(body=body)

            if labels is not None:
                issue.edit(labels=labels)

            return FairscapeResponse(
                success=True,
                statusCode=200,
                model={
                    'number': issue.number,
                    'title': issue.title,
                    'state': issue.state,
                    'updated_at': issue.updated_at.isoformat()
                }
            )

        except GithubException as e:
            return FairscapeResponse(
                success=False,
                statusCode=e.status,
                error={'message': str(e)}
            )
        except Exception as e:
            return FairscapeResponse(
                success=False,
                statusCode=500,
                error={'message': f'Error updating issue: {str(e)}'}
            )

    def update_file(
        self,
        file_url: str,
        new_file: UploadFile,
        commit_message: Optional[str] = None,
        current_user: Optional[UserWriteModel] = None
    ) -> FairscapeResponse:
        """
        Update a file in a GitHub repository using its URL

        Args:
            file_url: GitHub URL to the file (web or raw URL)
            new_file: The new file content to upload
            commit_message: Optional custom commit message
            current_user: The user performing the update

        Returns:
            FairscapeResponse with file update details
        """
        try:
            # Parse the GitHub URL
            url_parts = self.parse_github_url(file_url)
            owner = url_parts['owner']
            repo_name = url_parts['repo']
            branch = url_parts['branch']
            file_path = url_parts['file_path']

            # Get the repository
            repo = self.github.get_repo(f"{owner}/{repo_name}")

            # Read the new file content
            new_content = new_file.file.read()

            # Get the current file to obtain its SHA
            try:
                current_file = repo.get_contents(file_path, ref=branch)
                file_sha = current_file.sha
            except GithubException as e:
                if e.status == 404:
                    return FairscapeResponse(
                        success=False,
                        statusCode=404,
                        error={'message': f'File not found at path: {file_path}'}
                    )
                raise

            # Create commit message
            if commit_message is None:
                user_info = f" by {current_user.email}" if current_user else ""
                commit_message = f"Update {file_path.split('/')[-1]}{user_info}"

            # Update the file
            update_result = repo.update_file(
                path=file_path,
                message=commit_message,
                content=new_content,
                sha=file_sha,
                branch=branch
            )

            # Construct the raw URL
            raw_url = f"https://raw.githubusercontent.com/{owner}/{repo_name}/{branch}/{file_path}"

            response_model = FileUpdateResponse(
                success=True,
                file_path=file_path,
                commit_sha=update_result['commit'].sha,
                commit_url=update_result['commit'].html_url,
                raw_url=raw_url
            )

            return FairscapeResponse(
                success=True,
                statusCode=200,
                model=response_model.model_dump()
            )

        except ValueError as e:
            # URL parsing error
            return FairscapeResponse(
                success=False,
                statusCode=400,
                error={'message': str(e)}
            )
        except GithubException as e:
            return FairscapeResponse(
                success=False,
                statusCode=e.status,
                error={'message': str(e)}
            )
        except Exception as e:
            return FairscapeResponse(
                success=False,
                statusCode=500,
                error={'message': f'Error updating file: {str(e)}'}
            )

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, status
from fastapi.responses import JSONResponse
from typing import Annotated, List
import uuid

from fairscape_mds.models.user import UserWriteModel
from fairscape_mds.models.llm_assist import LLMAssistTask, D4DFromIssueRequest
from fairscape_mds.crud.llm_assist import FairscapeLLMAssistRequest
from fairscape_mds.deps import getCurrentUser
from fairscape_mds.core.config import appConfig
from fairscape_mds.worker import process_llm_assist_task


router = APIRouter(
    prefix="/llmassist",
    tags=["LLMAssist"]
)

llm_assist_request_handler = FairscapeLLMAssistRequest(appConfig)


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=dict,
    summary="Submit PDFs for LLM processing to generate RO-Crate metadata"
)
def create_llm_assist_task_route(
    files: List[UploadFile] = File(...),
    current_user: Annotated[UserWriteModel, Depends(getCurrentUser)] = None,
):
    
    task_guid = str(uuid.uuid4())
    
    response = llm_assist_request_handler.create_llm_assist_task(
        requesting_user=current_user,
        files=files,
        task_guid=task_guid
    )
    
    if not response.success:
        raise HTTPException(status_code=response.statusCode, detail=response.error)
    
    process_llm_assist_task.delay(task_guid=task_guid)
    
    return {
        "message": "LLM processing task created",
        "task_id": task_guid,
        "status_endpoint": f"/llmassist/status/{task_guid}"
    }


@router.get(
    "/status/{task_id}",
    summary="Get the status of an LLM processing task"
)
def get_llm_assist_task_status_route(
    task_id: str,
):
    response = llm_assist_request_handler.get_task_status(task_guid=task_id)

    if not response.success:
        raise HTTPException(status_code=response.statusCode, detail=response.error)

    task = response.model

    response_data = {
        "task_id": task.guid,
        "status": task.status,
        "time_created": task.time_created.isoformat() if task.time_created else None,
        "time_started": task.time_started.isoformat() if task.time_started else None,
        "time_finished": task.time_finished.isoformat() if task.time_finished else None,
    }

    if task.status == "SUCCESS":
        response_data["rocrate"] = task.result
        response_data["provenance"] = {
            "inputArk": task.input_dataset_ark,
            "outputArk": task.output_dataset_ark,
            "computationArk": task.computation_ark,
            "requiresGithubPush": False,
            "sourceFlow": "direct"
        }
    elif task.status in ["ERROR", "JSON_PARSE_FAILED"]:
        response_data["error"] = task.error
        if task.raw_llm_response:
            response_data["raw_llm_response"] = task.raw_llm_response

    return response_data


@router.post(
    "/from-issue",
    status_code=status.HTTP_200_OK,
    response_model=dict,
    summary="Process a D4D GitHub issue with full provenance tracking"
)
def create_d4d_from_issue_route(
    request: D4DFromIssueRequest,
    current_user: Annotated[UserWriteModel, Depends(getCurrentUser)],
):
    """
    Process a D4D generation request from a GitHub issue.
    Creates full provenance chain: user request -> computation -> YAML dataset.
    Returns RO-Crate JSON and provenance information.
    """
    try:
        result = llm_assist_request_handler.process_d4d_issue_with_provenance(
            request=request,
            requesting_user=current_user
        )

        return result

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process D4D issue: {str(e)}"
        )
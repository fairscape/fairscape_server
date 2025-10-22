from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, status
from fastapi.responses import JSONResponse
from typing import Annotated, List
import uuid

from fairscape_mds.models.user import UserWriteModel
from fairscape_mds.models.llm_assist import LLMAssistTask
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
    response_model=LLMAssistTask,
    summary="Get the status of an LLM processing task"
)
def get_llm_assist_task_status_route(
    task_id: str,
):
    response = llm_assist_request_handler.get_task_status(task_guid=task_id)
    
    if not response.success:
        raise HTTPException(status_code=response.statusCode, detail=response.error)
    
    return response.model
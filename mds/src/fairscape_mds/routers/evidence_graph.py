from fastapi import APIRouter, Depends, HTTPException, Path, status
from fastapi.responses import JSONResponse
from typing import Annotated, List, Dict
import uuid

from fairscape_mds.models.user import UserWriteModel
from fairscape_mds.models.evidence_graph import EvidenceGraph, EvidenceGraphCreate, EvidenceGraphBuildRequest
from fairscape_mds.crud.evidence_graph import FairscapeEvidenceGraphRequest
from fairscape_mds.main import getCurrentUser
from fairscape_mds.core.config import appConfig
from fairscape_mds.worker import build_evidence_graph_task


router = APIRouter(
    prefix="/evidencegraph",
    tags=["EvidenceGraph"]
)

evidence_graph_request_handler = FairscapeEvidenceGraphRequest(appConfig)

@router.post("", status_code=201, response_model=EvidenceGraph, summary="Create a new EvidenceGraph record")
def create_evidence_graph_route(
    evidence_graph_data: EvidenceGraphCreate,
    current_user: Annotated[UserWriteModel, Depends(getCurrentUser)],
):
    response = evidence_graph_request_handler.create_evidence_graph(
        requesting_user=current_user,
        evi_graph_create_model=evidence_graph_data
    )
    if response.success:
        return response.model
    else:
        raise HTTPException(status_code=response.statusCode, detail=response.error)

@router.get("/ark:{NAAN}/{postfix}", response_model=EvidenceGraph, summary="Get an EvidenceGraph by its ARK ID")
def get_evidence_graph_route(
    NAAN: Annotated[str, Path(description="Name Assigning Authority Number of the ARK ID")],
    postfix: Annotated[str, Path(description="Postfix of the ARK ID")],
):
    evidence_id = f"ark:{NAAN}/{postfix}"
    response = evidence_graph_request_handler.get_evidence_graph(evidence_id)
    if response.success:
        return response.model
    else:
        raise HTTPException(status_code=response.statusCode, detail=response.error)


@router.get("/query/ark:{NAAN}/{postfix}", response_model=EvidenceGraph, summary="Get an EvidenceGraph by its ARK ID")
def get_evidence_graph_query_route(
    NAAN: Annotated[str, Path(description="Name Assigning Authority Number of the ARK ID")],
    postfix: Annotated[str, Path(description="Postfix of the ARK ID")],
    current_user: Annotated[UserWriteModel, Depends(getCurrentUser)],
):
    evidence_id = f"ark:{NAAN}/{postfix}"
    response = evidence_graph_request_handler.get_evidence_graph(evidence_id)
    if response.success:
        return response.model
    else:
        raise HTTPException(status_code=response.statusCode, detail=response.error)


@router.delete("/ark:{NAAN}/{postfix}", summary="Delete an EvidenceGraph by its ARK ID")
def delete_evidence_graph_route(
    NAAN: Annotated[str, Path(description="Name Assigning Authority Number of the ARK ID")],
    postfix: Annotated[str, Path(description="Postfix of the ARK ID")],
    current_user: Annotated[UserWriteModel, Depends(getCurrentUser)],
):
    evidence_id = f"ark:{NAAN}/{postfix}"
    response = evidence_graph_request_handler.delete_evidence_graph(
        requesting_user=current_user,
        evidence_id=evidence_id
    )
    if response.success:
        return JSONResponse(content=response.model, status_code=response.statusCode)
    else:
        raise HTTPException(status_code=response.statusCode, detail=response.error)

@router.get("", response_model=List[EvidenceGraph], summary="List all EvidenceGraphs")
def list_evidence_graphs_route():
    response = evidence_graph_request_handler.list_evidence_graphs()
    if response.success:
        return response.model
    else:
        raise HTTPException(status_code=response.statusCode, detail=response.error)

@router.post(
    "/build/ark:{NAAN}/{postfix}",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=Dict,
    summary="Initiate building or rebuilding the EvidenceGraph for a given node ARK ID"
)
def initiate_build_evidence_graph_for_node_route(
    NAAN: Annotated[str, Path(description="NAAN of the node to build graph for")],
    postfix: Annotated[str, Path(description="Postfix of the node to build graph for")],
    current_user: Annotated[UserWriteModel, Depends(getCurrentUser)],
):
    task_guid = str(uuid.uuid4())

    task_request_data = {
        "guid": task_guid,
        "owner_email": current_user.email,
        "naan": NAAN,
        "postfix": postfix,
        "status": "PENDING",
    }
    
    try:
        task_request_model = EvidenceGraphBuildRequest.model_validate(task_request_data)
        appConfig.asyncCollection.insert_one(task_request_model.model_dump(by_alias=True))
    except Exception as e:
        print(f"Failed to create task record for evidence graph build: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to initiate evidence graph build task."
        )

    build_evidence_graph_task.delay(
        task_guid=task_guid,
        user_email=current_user.email,
        naan=NAAN,
        postfix=postfix
    )

    return {"message": "EvidenceGraph build process initiated.", "task_id": task_guid, "status_endpoint": f"/evidencegraph/build/status/{task_guid}"}

@router.get(
    "/build/status/{task_id}",
    response_model=EvidenceGraphBuildRequest,
    summary="Get the status of an EvidenceGraph build task"
)
def get_build_evidence_graph_status_route(
    task_id: Annotated[str, Path(description="The ID of the build task")],
):
    task_status_doc = appConfig.asyncCollection.find_one({"guid": task_id}, {"_id": 0})

    if not task_status_doc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found.")

    try:
        task_model = EvidenceGraphBuildRequest.model_validate(task_status_doc)
        return task_model
    except Exception as e:
        print(f"Data validation error for task {task_id}: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error retrieving task status.")
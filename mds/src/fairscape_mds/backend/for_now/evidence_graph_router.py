from fastapi import APIRouter, Depends, HTTPException, Path
from fastapi.responses import JSONResponse
from typing import Annotated, List

from fairscape_mds.backend.models import UserWriteModel
from fairscape_mds.backend.for_now.evidence_graph import EvidenceGraph, EvidenceGraphCreate
from fairscape_mds.backend.for_now.evidence_graph_crud import FairscapeEvidenceGraphRequest
from fairscape_mds.main import getCurrentUser
from fairscape_mds.backend.backend import (
    identifierCollection,
    userCollection
)

router = APIRouter(
    prefix="/evidencegraph",
    tags=["EvidenceGraph"]
)

evidence_graph_request_handler = FairscapeEvidenceGraphRequest(
    identiferCollection=identifierCollection,
    userCollection=userCollection
)

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
    status_code=201,
    response_model=EvidenceGraph,
    summary="Build or rebuild the EvidenceGraph for a given node ARK ID"
)
def build_evidence_graph_for_node_route(
    NAAN: Annotated[str, Path(description="NAAN of the node to build graph for")],
    postfix: Annotated[str, Path(description="Postfix of the node to build graph for")],
    current_user: Annotated[UserWriteModel, Depends(getCurrentUser)],
):
    response = evidence_graph_request_handler.build_evidence_graph_for_node(
        requesting_user=current_user,
        naan=NAAN,
        postfix=postfix
    )
    if response.success:
        return response.model
    else:
        raise HTTPException(status_code=response.statusCode, detail=response.error)
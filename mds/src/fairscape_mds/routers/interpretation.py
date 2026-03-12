"""Router for AI-mediated interpretation of RO-Crates."""

from typing import Annotated
from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse

import uuid
import datetime

from fairscape_mds.models.user import UserWriteModel
from fairscape_mds.core.config import appConfig
from fairscape_mds.deps import getCurrentUser
from fairscape_mds.crud.fairscape_request import flexible_ark_query

router = APIRouter(
    prefix="/interpretation",
    tags=["Interpretation"],
)


def _flexible_find(ark_id: str):
    """Look up an entity by ARK, tolerating dash/slash variants."""
    result = appConfig.identifierCollection.find_one({"@id": ark_id}, {"_id": False})
    if result:
        return result
    query = flexible_ark_query(ark_id)
    if query:
        result = appConfig.identifierCollection.find_one(query, {"_id": False})
    return result


# ---------------------------------------------------------------------------
# POST /interpretation/ark:{NAAN}/{postfix} -- trigger interpretation
# ---------------------------------------------------------------------------

@router.post(
    "/ark:/{NAAN}/{postfix}",
    summary="Trigger AI-mediated interpretation of an RO-Crate",
    status_code=202,
)
@router.post(
    "/ark:{NAAN}/{postfix}",
    summary="Trigger AI-mediated interpretation of an RO-Crate",
    status_code=202,
)
def trigger_interpretation(
    currentUser: Annotated[UserWriteModel, Depends(getCurrentUser)],
    NAAN: str,
    postfix: str,
    llm_model: str = Query(default="google-gla:gemini-2.5-flash", description="PydanticAI model string"),
    temperature: float = Query(default=0.2, ge=0.0, le=2.0, description="LLM temperature"),
    persona: str = Query(default="datasci", description="Interpretation persona"),
    force: bool = Query(default=False, description="Force re-interpretation if one already exists"),
):
    """Trigger async AI-mediated interpretation of an RO-Crate.
    Ensures condensation, annotates each computation, synthesizes a graph-level
    summary, and stores the resulting AnnotatedEvidenceGraph."""

    from fairscape_mds.worker import interpret_rocrate_task

    ark_id = f"ark:{NAAN}/{postfix}"

    entity = _flexible_find(ark_id)
    if not entity:
        return JSONResponse(status_code=404, content={"error": f"Entity {ark_id} not found"})

    entity_type = entity.get("@type", [])
    if isinstance(entity_type, str):
        entity_type = [entity_type]
    is_rocrate = any("ROCrate" in str(t) for t in entity_type)
    if not is_rocrate:
        return JSONResponse(status_code=400, content={"error": "Entity is not an RO-Crate"})

    # Check for existing AnnotatedEvidenceGraph
    if not force:
        existing_aeg = entity.get("metadata", {}).get("hasAnnotatedEvidenceGraph")
        if existing_aeg:
            return JSONResponse(
                status_code=200,
                content={
                    "message": "Annotated Evidence Graph already exists. Use force=true to re-interpret.",
                    "annotated_evidence_graph_id": existing_aeg.get("@id"),
                }
            )

    # Check for in-progress task
    task_doc = appConfig.asyncCollection.find_one({
        "task_type": "InterpretROCrate",
        "rocrate_id": ark_id,
        "status": {"$nin": ["SUCCESS", "FAILURE"]},
    }, {"_id": 0})

    if task_doc:
        return JSONResponse(
            status_code=202,
            content={
                "message": "Interpretation already in progress",
                "task_id": task_doc["guid"],
                "status": task_doc.get("status"),
                "current_step": task_doc.get("current_step"),
                "status_endpoint": f"/interpretation/status/{task_doc['guid']}",
            }
        )

    # Create async task
    task_guid = str(uuid.uuid4())
    task_data = {
        "guid": task_guid,
        "task_type": "InterpretROCrate",
        "rocrate_id": ark_id,
        "owner_email": currentUser.email,
        "status": "PENDING",
        "current_step": "PENDING",
        "total_computations": 0,
        "completed_computations": 0,
        "computation_details": [],
        "condensed_rocrate_id": None,
        "annotated_evidence_graph_id": None,
        "llm_model": llm_model,
        "llm_temperature": temperature,
        "persona": persona,
        "time_created": datetime.datetime.utcnow(),
        "error": None,
    }

    appConfig.asyncCollection.insert_one(task_data)

    interpret_rocrate_task.delay(
        task_guid=task_guid,
        rocrate_id=ark_id,
        llm_model=llm_model,
        temperature=temperature,
    )

    return JSONResponse(
        status_code=202,
        content={
            "message": "Interpretation initiated",
            "task_id": task_guid,
            "status_endpoint": f"/interpretation/status/{task_guid}",
        }
    )


# ---------------------------------------------------------------------------
# GET /interpretation/status/{task_id} -- check progress
# ---------------------------------------------------------------------------

@router.get(
    "/status/{task_id}",
    summary="Get status of an interpretation task",
)
def get_interpretation_status(task_id: str):
    """Check the status of an async interpretation task.
    Includes granular step tracking and per-computation progress."""
    task_doc = appConfig.asyncCollection.find_one(
        {"guid": task_id}, {"_id": 0}
    )

    if not task_doc:
        return JSONResponse(status_code=404, content={"error": "Task not found"})

    # Serialize datetimes for JSON
    for key in ("time_created", "time_started", "time_finished"):
        if task_doc.get(key) and hasattr(task_doc[key], "isoformat"):
            task_doc[key] = task_doc[key].isoformat()

    return JSONResponse(status_code=200, content=task_doc)


# ---------------------------------------------------------------------------
# GET /interpretation/result/ark:{NAAN}/{postfix} -- get result
# ---------------------------------------------------------------------------

@router.get(
    "/result/ark:/{NAAN}/{postfix}",
    summary="Get the AnnotatedEvidenceGraph for an RO-Crate",
)
@router.get(
    "/result/ark:{NAAN}/{postfix}",
    summary="Get the AnnotatedEvidenceGraph for an RO-Crate",
)
def get_interpretation_result(NAAN: str, postfix: str):
    """Return the AnnotatedEvidenceGraph if it exists."""
    ark_id = f"ark:{NAAN}/{postfix}"

    entity = _flexible_find(ark_id)
    if not entity:
        return JSONResponse(status_code=404, content={"error": f"Entity {ark_id} not found"})

    aeg_ref = entity.get("metadata", {}).get("hasAnnotatedEvidenceGraph")
    if not aeg_ref:
        return JSONResponse(
            status_code=404,
            content={"error": "No AnnotatedEvidenceGraph exists for this RO-Crate. Trigger interpretation first."}
        )

    aeg_id = aeg_ref.get("@id") if isinstance(aeg_ref, dict) else aeg_ref
    aeg_doc = _flexible_find(aeg_id)
    if not aeg_doc:
        return JSONResponse(status_code=404, content={"error": f"AnnotatedEvidenceGraph {aeg_id} not found"})

    metadata = aeg_doc.get("metadata", {})
    return JSONResponse(status_code=200, content=metadata)

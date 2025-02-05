# Standard library
from typing import Annotated
from uuid import uuid4

# Third party
from fastapi import APIRouter, Depends, Response
from fastapi.responses import JSONResponse

# Fairscape
from fairscape_mds.auth.oauth import getCurrentUser
from fairscape_mds.config import get_fairscape_config
from fairscape_mds.models.evidencegraph import EvidenceGraph
from fairscape_mds.models.user import UserLDAP
from fairscape_mds.worker import (
   AsyncBuildEvidenceGraph,
   getUploadJob
)

router = APIRouter()
fairscapeConfig = get_fairscape_config()
mongoClient = fairscapeConfig.CreateMongoClient()
mongoDB = mongoClient[fairscapeConfig.mongo.db]
asyncCollection = mongoDB[fairscapeConfig.mongo.async_collection]

@router.post("/evidencegraph/build/ark:{NAAN}/{postfix}",
             summary="Build evidence graph for node",
             status_code=202)
def build_evidence_graph(
    NAAN: str,
    postfix: str,
    currentUser: Annotated[UserLDAP, Depends(getCurrentUser)]
):
    task_id = str(uuid4())
    
    AsyncBuildEvidenceGraph.apply_async(args=(
        currentUser.cn,
        NAAN,
        postfix,
        task_id  
    ))
    
    return JSONResponse(
        status_code=202,
        content={
            "task_id": task_id,
            "status": "processing"
        }
    )


@router.get("/evidencegraph/status/{task_id}")
def get_build_status(
    task_id: str,
):
    # Direct MongoDB query to async_collection
    job_status = asyncCollection.find_one({"task_id": task_id})
    
    if not job_status:
        return JSONResponse(status_code=404, content={"error": "Task not found"})
    
    # Remove MongoDB internal _id field before returning
    if '_id' in job_status:
        del job_status['_id']
    
    return job_status

@router.post("/evidencegraph")
def create_evidence_graph(
   evidence_graph: EvidenceGraph,
   currentUser: Annotated[UserLDAP, Depends(getCurrentUser)]
):
   create_status = evidence_graph.create(fairscapeConfig.mongo.identifier_collection)
   if create_status.success:
       return JSONResponse(
           status_code=201,
           content={"created": {"@id": evidence_graph.guid}}
       )
   return JSONResponse(
       status_code=create_status.status_code,
       content={"error": create_status.message}
   )

@router.get("/evidencegraph/ark:{NAAN}/{postfix}")
def get_evidence_graph(NAAN: str, postfix: str):
   evidence_id = f"ark:{NAAN}/{postfix}"
   graph = EvidenceGraph.construct(id=evidence_id)
   read_status = graph.read(fairscapeConfig.mongo.identifier_collection)
   
   if read_status.success:
       return graph
   return JSONResponse(
       status_code=read_status.status_code,
       content={"error": read_status.message}
   )

@router.get("/evidencegraph")
def list_evidence_graphs():
   return list_evidence_graphs(fairscapeConfig.mongo.identifier_collection)

@router.delete("/evidencegraph/ark:{NAAN}/{postfix}")
def delete_evidence_graph(NAAN: str, postfix: str):
   evidence_id = f"ark:{NAAN}/{postfix}"
   graph = EvidenceGraph.construct(id=evidence_id)
   delete_status = graph.delete(fairscapeConfig.mongo.identifier_collection)
   
   if delete_status.success:
       return JSONResponse(
           status_code=200,
           content={"deleted": evidence_id}
       )
   return JSONResponse(
       status_code=delete_status.status_code,
       content={"error": delete_status.message}
   )
from typing import (
	Annotated,
	Optional
)
from fastapi import (
	APIRouter,
	Depends,
	HTTPException,
	Request,
	UploadFile,
	Query
)
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.encoders import jsonable_encoder

import uuid
import datetime

from fairscape_mds.crud.rocrate import FairscapeROCrateRequest
from fairscape_mds.crud.fairscape_request import flexible_ark_query

from fairscape_mds.models.user import UserWriteModel
from fairscape_mds.models.identifier import StoredIdentifier
from fairscape_mds.core.config import appConfig
from fairscape_models.rocrate import ROCrateV1_2, ROCrateMetadataElem
from fairscape_mds.deps import getCurrentUser
from fairscape_mds.worker import celeryUploadROCrate, score_ai_ready_task, condense_rocrate_task

from fairscape_models.conversion.converter import ROCToTargetConverter
from fairscape_models.conversion.mapping.croissant import MAPPING_CONFIGURATION as CROISSANT_MAPPING

import pathlib

rocrateRequest = FairscapeROCrateRequest(appConfig)

rocrateRouter = APIRouter(prefix="", tags=['evi', 'rocrate'])


def _flexible_find(guid: str, projection=None):
	"""Exact match then dash/slash-tolerant fallback lookup."""
	if projection is None:
		projection = {"_id": 0}
	result = appConfig.identifierCollection.find_one({"@id": guid}, projection)
	if not result:
		query = flexible_ark_query(guid)
		if query:
			result = appConfig.identifierCollection.find_one(query, projection)
	return result


@rocrateRouter.post("/rocrate/upload-async")
def uploadROCrate(
	currentUser: Annotated[UserWriteModel, Depends(getCurrentUser)],
	crate: UploadFile
):

	uploadOperation = rocrateRequest.uploadROCrate(
		userInstance=currentUser,
		rocrate=crate
	)

	if uploadOperation.success:

		uploadJob = uploadOperation.model

		# start backend job
		# processROCrate.apply_async(args=(uploadJob.guid,), )
		celeryUploadROCrate(uploadJob.guid)
		return uploadJob

	else:
		return JSONResponse(
			status_code=400,
			content={"error": uploadOperation.error}
		)
  
@rocrateRouter.post(
	"/rocrate/metadata",
	summary="Mint metadata-only ROCrate records without file content",
	status_code=201
)
def publishMetadataOnly(
	currentUser: Annotated[UserWriteModel, Depends(getCurrentUser)],
	crateMetadata: ROCrateV1_2,
	baseDatasetArk: Optional[str] = Query(default=None, description="Optional base dataset ARK identifier")
):
	try:
		result = rocrateRequest.mintMetadataOnlyROCrate(
			requestingUser=currentUser,
			crateModel=crateMetadata,
			baseDatasetArk=baseDatasetArk
		)
		
		if result.success:
			return JSONResponse(
				content=result.model,
				status_code=result.statusCode
			)
		else:
			return JSONResponse(
				content=result.error,
				status_code=result.statusCode
			)
	except Exception as e:
		return JSONResponse(
			content={
				"message": "Error minting metadata-only ROCrate identifiers",
				"error": str(e)
			},
			status_code=500
		)
		
@rocrateRouter.get(
	"/rocrate",
	summary="List all ROCrates accessible by the current user",
	response_description="A list of RO-Crates with their basic metadata"
)
def list_rocrates_endpoint(
	currentUser: Annotated[UserWriteModel, Depends(getCurrentUser)],
):
	fairscape_response = rocrateRequest.list_crates(requestingUser=currentUser)

	if fairscape_response.success:
		return JSONResponse(
			status_code=fairscape_response.statusCode,
			content=fairscape_response.model
		)
	else:
		raise HTTPException(
			status_code=fairscape_response.statusCode,
			detail=fairscape_response.error
		)


@rocrateRouter.get("/rocrate/upload/status/{submissionUUID}")
def getUploadStatus(
	currentUser: Annotated[UserWriteModel, Depends(getCurrentUser)],
	submissionUUID: str
):

	response = rocrateRequest.getUploadMetadata(
		currentUser,
		submissionUUID
	)

	if response.success:
		return response.model

	else:
		return JSONResponse(
			status_code = response.statusCode,
			content = response.error
		)


@rocrateRouter.get("/rocrate/download/ark:/{NAAN}/{postfix}")
@rocrateRouter.get("/rocrate/download/ark:{NAAN}/{postfix}")
def getROCrateArchive(
	currentUser: Annotated[UserWriteModel, Depends(getCurrentUser)],
	NAAN: str,
	postfix: str
):

	rocrateGUID = f"ark:{NAAN}/{postfix}"

	response = rocrateRequest.downloadROCrateArchive(
		currentUser,
		rocrateGUID
	)
	
	if response.success:

		object_key = response.model.distribution.location.path
		filename = pathlib.Path(object_key).name

		zip_headers = {
			"Content-Type": "application/zip",
			"Content-Disposition": f'attachment; filename="{filename}"'
		}
		
		return StreamingResponse(
			response.fileResponse,
			headers=zip_headers
		)

	else:
		return JSONResponse(
			status_code = response.statusCode,
			content = response.error
		)


@rocrateRouter.get(
	"/rocrate/summary/ark:/{NAAN}/{postfix}",
	summary="Get a summary of RO-Crate contents",
	response_description="Paginated list of datasets, software, computations, etc. with counts"
)
@rocrateRouter.get(
	"/rocrate/summary/ark:{NAAN}/{postfix}",
	summary="Get a summary of RO-Crate contents",
	response_description="Paginated list of datasets, software, computations, etc. with counts"
)
def getROCrateContentSummary(
	NAAN: str,
	postfix: str,
	limit: int = Query(default=10, ge=1, le=100, description="Max items per category"),
	offset: int = Query(default=0, ge=0, description="Starting index for pagination")
):
	"""
	Retrieve a lightweight summary of RO-Crate contents.

	Returns the first N items (by default 10) of each category:
	- datasets
	- software
	- computations
	- schemas
	- samples
	- mlModels
	- rocrates (nested RO-Crates)
	- other

	Also includes total counts for each category.

	Use `offset` and `limit` for pagination through large collections.
	"""
	guid = f"ark:{NAAN}/{postfix}"

	response = rocrateRequest.getROCrateContentSummary(
		rocrateGUID=guid,
		limit=limit,
		offset=offset
	)

	if response.success:
		return JSONResponse(
			status_code=200,
			content=response.model
		)
	else:
		return JSONResponse(
			status_code=response.statusCode,
			content=response.error
		)


@rocrateRouter.get("/rocrate/ark:/{NAAN}/{postfix}")
@rocrateRouter.get("/rocrate/ark:{NAAN}/{postfix}")
def getROCrateMetadata(
	request: Request,
	NAAN: str,
	postfix: str,
):
	"""
	Retrieve RO-Crate metadata.  
	Supports content negotiation:  
	- `application/json` (default, raw RO-Crate JSON)  
	- `application/vnd.mlcommons-croissant+json` (Croissant JSON-LD)  
	"""
	guid = f"ark:{NAAN}/{postfix}"
	response = rocrateRequest.getROCrateMetadata(guid)

	if not response.success:
		return JSONResponse(
			status_code=response.statusCode,
			content=response.error
		)

	accept_header = request.headers.get("accept", "application/json")

	if "application/vnd.mlcommons-croissant+json" in accept_header.lower():
		try:
			source_crate = ROCrateV1_2(**response.model)
			croissant_converter = ROCToTargetConverter(source_crate, CROISSANT_MAPPING)
			croissant_result = croissant_converter.convert()

			return JSONResponse(
				status_code=200,
				content=croissant_result.model_dump(by_alias=True, exclude_none=True)
			)
		except Exception as e:
			raise HTTPException(
				status_code=500,
				detail=f"Error converting RO-Crate to Croissant: {str(e)}"
			)

	return JSONResponse(
		status_code=200,
		content=response.model
	)

@rocrateRouter.get(
	"/rocrate/ai-ready-score/ark:/{NAAN}/{postfix}",
	summary="Get or initiate AI-Ready Score for an RO-Crate (Public)",
	response_description="AI-Ready Score or task status"
)
@rocrateRouter.get(
	"/rocrate/ai-ready-score/ark:{NAAN}/{postfix}",
	summary="Get or initiate AI-Ready Score for an RO-Crate (Public)",
	response_description="AI-Ready Score or task status"
)
def get_or_create_ai_ready_score(
	NAAN: str,
	postfix: str
):
	ark_id = f"ark:{NAAN}/{postfix}"
	
	entity = _flexible_find(ark_id)
	if not entity:
		return JSONResponse(
			status_code=404,
			content={"error": f"Entity {ark_id} not found"}
		)

	entity_type = entity.get("@type")
	if isinstance(entity_type, str):
		entity_type_list = [entity_type]
	else:
		entity_type_list = entity_type if entity_type else []

	if "evi:AIReadyScore" in entity_type_list or entity_type == "evi:AIReadyScore":
		try:
			stored_identifier = StoredIdentifier.model_validate(entity)
			return JSONResponse(
				status_code=200,
				content=stored_identifier.model_dump(by_alias=True, mode="json")
			)
		except Exception as e:
			return JSONResponse(
				status_code=500,
				content={"error": f"Error validating AIReadyScore: {str(e)}"}
			)

	is_rocrate = any("ROCrate" in str(t) for t in entity_type_list)
	if not is_rocrate:
		return JSONResponse(
			status_code=400,
			content={"error": "Entity is not an RO-Crate or AI-Ready Score"}
		)

	if entity.get("metadata", {}).get("hasAIReadyScore"):
		score_id = entity["metadata"]["hasAIReadyScore"].get("@id")
		score_entity = _flexible_find(score_id)
		if score_entity:
			try:
				stored_identifier = StoredIdentifier.model_validate(score_entity)
				return JSONResponse(
					status_code=200,
					content=stored_identifier.model_dump(by_alias=True, mode="json")
				)
			except Exception as e:
				return JSONResponse(
					status_code=500,
					content={"error": f"Error validating existing AIReadyScore: {str(e)}"}
				)
	
	task_doc = appConfig.asyncCollection.find_one({
		"task_type": "AIReadyScoring",
		"rocrate_id": ark_id,
		"status": {"$in": ["PENDING", "PROCESSING"]}
	}, {"_id": 0})
	
	if task_doc:
		return JSONResponse(
			status_code=202,
			content={
				"message": "AI-Ready scoring in progress",
				"task_id": task_doc["guid"],
				"status": task_doc["status"],
				"status_endpoint": f"/rocrate/ai-ready-score/status/{task_doc['guid']}"
			}
		)
	
	task_guid = str(uuid.uuid4())
	task_data = {
		"guid": task_guid,
		"task_type": "AIReadyScoring",
		"rocrate_id": ark_id,
		"owner_email": "system@fairscape.org",
		"status": "PENDING",
		"time_created": datetime.datetime.utcnow()
	}
	
	appConfig.asyncCollection.insert_one(task_data)
	
	score_ai_ready_task.delay(
		task_guid=task_guid,
		rocrate_id=ark_id
	)
	
	return JSONResponse(
		status_code=202,
		content={
			"message": "AI-Ready scoring initiated",
			"task_id": task_guid,
			"status_endpoint": f"/rocrate/ai-ready-score/status/{task_guid}"
		}
	)

@rocrateRouter.get(
	"/rocrate/ai-ready-score/status/{task_id}",
	summary="Get status of AI-Ready Score task (Public)"
)
def get_ai_ready_score_status(
	task_id: str
):
	task_doc = appConfig.asyncCollection.find_one({"guid": task_id}, {"_id": 0})
	
	if not task_doc:
		return JSONResponse(
			status_code=404,
			content={"error": "Task not found"}
		)
	
	return JSONResponse(
		status_code=200,
		content=task_doc
	)
	
@rocrateRouter.post(
	"/rocrate/ai-ready-score/ark:/{NAAN}/{postfix}/rescore",
	summary="Rescore an existing AI-Ready Score (Public)",
	status_code=202
)
@rocrateRouter.post(
	"/rocrate/ai-ready-score/ark:{NAAN}/{postfix}/rescore",
	summary="Rescore an existing AI-Ready Score (Public)",
	status_code=202
)
def rescore_ai_ready_score(
	NAAN: str,
	postfix: str
):
	ark_id = f"ark:{NAAN}/{postfix}"
	
	entity = _flexible_find(ark_id)
	if not entity:
		return JSONResponse(
			status_code=404,
			content={"error": f"Entity {ark_id} not found"}
		)

	entity_type = entity.get("@type", [])
	if isinstance(entity_type, str):
		entity_type = [entity_type]

	is_rocrate = any("ROCrate" in t for t in entity_type)
	if not is_rocrate:
		return JSONResponse(
			status_code=400,
			content={"error": "Entity is not an RO-Crate"}
		)

	score_id = f"{ark_id}-ai-ready-score"
	existing_score = _flexible_find(score_id)
	if not existing_score:
		return JSONResponse(
			status_code=404,
			content={"error": f"No existing AI-Ready Score found for {ark_id}"}
		)
	
	from fairscape_mds.crud.AIReady import FairscapeAIReadyScoreRequest
	ai_ready_request = FairscapeAIReadyScoreRequest(appConfig)
	
	delete_response = ai_ready_request.delete_ai_ready_score(ark_id)
	if not delete_response.success:
		return JSONResponse(
			status_code=delete_response.statusCode,
			content=delete_response.error
		)
	
	task_doc = appConfig.asyncCollection.find_one({
		"task_type": "AIReadyScoring",
		"rocrate_id": ark_id,
		"status": {"$in": ["PENDING", "PROCESSING"]}
	}, {"_id": 0})
	
	if task_doc:
		return JSONResponse(
			status_code=202,
			content={
				"message": "AI-Ready rescoring already in progress",
				"task_id": task_doc["guid"],
				"status": task_doc["status"],
				"status_endpoint": f"/rocrate/ai-ready-score/status/{task_doc['guid']}"
			}
		)
	
	task_guid = str(uuid.uuid4())
	task_data = {
		"guid": task_guid,
		"task_type": "AIReadyScoring",
		"rocrate_id": ark_id,
		"owner_email": "system@fairscape.org",
		"status": "PENDING",
		"time_created": datetime.datetime.utcnow()
	}
	
	appConfig.asyncCollection.insert_one(task_data)
	
	score_ai_ready_task.delay(
		task_guid=task_guid,
		rocrate_id=ark_id
	)
	
	return JSONResponse(
		status_code=202,
		content={
			"message": "AI-Ready rescoring initiated",
			"task_id": task_guid,
			"status_endpoint": f"/rocrate/ai-ready-score/status/{task_guid}"
		}
	)


# ---------------------------------------------------------------------------
# Condensation endpoints
# ---------------------------------------------------------------------------

@rocrateRouter.post(
	"/rocrate/condense/ark:/{NAAN}/{postfix}",
	summary="Trigger condensation of an RO-Crate",
	status_code=202,
)
@rocrateRouter.post(
	"/rocrate/condense/ark:{NAAN}/{postfix}",
	summary="Trigger condensation of an RO-Crate",
	status_code=202,
)
def trigger_condense_rocrate(
	currentUser: Annotated[UserWriteModel, Depends(getCurrentUser)],
	NAAN: str,
	postfix: str,
	threshold: int = Query(default=5, ge=2, description="Min group size to trigger condensation"),
	max_member_ids: int = Query(default=0, ge=0, description="Max member IDs per group (0=unlimited)"),
	force: bool = Query(default=False, description="Force re-condensation if one already exists"),
):
	"""Trigger async condensation of an RO-Crate. Resolves cross-crate
	references from MongoDB and collapses repetitive provenance into
	DatasetGroup summary nodes."""
	ark_id = f"ark:{NAAN}/{postfix}"

	entity = _flexible_find(ark_id)
	if not entity:
		return JSONResponse(
			status_code=404,
			content={"error": f"Entity {ark_id} not found"}
		)

	entity_type = entity.get("@type", [])
	if isinstance(entity_type, str):
		entity_type = [entity_type]
	is_rocrate = any("ROCrate" in str(t) for t in entity_type)
	if not is_rocrate:
		return JSONResponse(
			status_code=400,
			content={"error": "Entity is not an RO-Crate"}
		)

	# Handle existing condensed ROCrate
	if entity.get("metadata", {}).get("hasCondensedROCrate") and not force:
		condensed_ref = entity["metadata"]["hasCondensedROCrate"]
		return JSONResponse(
			status_code=200,
			content={
				"message": "Condensed ROCrate already exists. Use force=true to re-condense.",
				"condensed_id": condensed_ref.get("@id"),
			}
		)

	# If force, delete existing condensed ROCrate first
	if force and entity.get("metadata", {}).get("hasCondensedROCrate"):
		from fairscape_mds.crud.condensation import FairscapeCondensationRequest
		condensation_req = FairscapeCondensationRequest(appConfig)
		condensation_req.delete_condensed_rocrate(ark_id)

	# Check for in-progress task
	task_doc = appConfig.asyncCollection.find_one({
		"task_type": "CondensedROCrateBuild",
		"rocrate_id": ark_id,
		"status": {"$in": ["PENDING", "PROCESSING"]}
	}, {"_id": 0})

	if task_doc:
		return JSONResponse(
			status_code=202,
			content={
				"message": "Condensation already in progress",
				"task_id": task_doc["guid"],
				"status": task_doc["status"],
				"status_endpoint": f"/rocrate/condense/status/{task_doc['guid']}"
			}
		)

	# Create async task
	task_guid = str(uuid.uuid4())
	task_data = {
		"guid": task_guid,
		"task_type": "CondensedROCrateBuild",
		"rocrate_id": ark_id,
		"owner_email": currentUser.email,
		"threshold": threshold,
		"max_member_ids": max_member_ids,
		"status": "PENDING",
		"time_created": datetime.datetime.utcnow(),
	}

	appConfig.asyncCollection.insert_one(task_data)

	condense_rocrate_task.delay(
		task_guid=task_guid,
		rocrate_id=ark_id,
		threshold=threshold,
		max_member_ids=max_member_ids,
		user_email=currentUser.email,
	)

	return JSONResponse(
		status_code=202,
		content={
			"message": "Condensation initiated",
			"task_id": task_guid,
			"status_endpoint": f"/rocrate/condense/status/{task_guid}"
		}
	)


@rocrateRouter.get(
	"/rocrate/condense/status/{task_id}",
	summary="Get status of condensation task",
)
def get_condense_status(task_id: str):
	"""Check the status of an async condensation task."""
	task_doc = appConfig.asyncCollection.find_one(
		{"guid": task_id}, {"_id": 0}
	)

	if not task_doc:
		return JSONResponse(
			status_code=404,
			content={"error": "Task not found"}
		)

	return JSONResponse(status_code=200, content=task_doc)


@rocrateRouter.get(
	"/rocrate/condensed/ark:/{NAAN}/{postfix}",
	summary="Get condensed ROCrate (auto-triggers if none exists)",
)
@rocrateRouter.get(
	"/rocrate/condensed/ark:{NAAN}/{postfix}",
	summary="Get condensed ROCrate (auto-triggers if none exists)",
)
def get_condensed_rocrate(
	NAAN: str,
	postfix: str,
	threshold: int = Query(default=5, ge=2, description="Threshold if auto-triggering"),
):
	"""Return the condensed ROCrate if it exists. If not, auto-trigger
	condensation and return 202 with the task ID."""
	ark_id = f"ark:{NAAN}/{postfix}"

	entity = _flexible_find(ark_id)
	if not entity:
		return JSONResponse(
			status_code=404,
			content={"error": f"Entity {ark_id} not found"}
		)

	entity_type = entity.get("@type", [])
	if isinstance(entity_type, str):
		entity_type = [entity_type]
	is_rocrate = any("ROCrate" in str(t) for t in entity_type)
	if not is_rocrate:
		return JSONResponse(
			status_code=400,
			content={"error": "Entity is not an RO-Crate"}
		)

	# If condensed version exists, return it
	condensed_ref = entity.get("metadata", {}).get("hasCondensedROCrate")
	if condensed_ref:
		condensed_id = condensed_ref.get("@id")
		condensed_doc = _flexible_find(condensed_id)
		if condensed_doc:
			# Return the condensed @graph from metadata
			metadata = condensed_doc.get("metadata", {})
			condensed_graph = metadata.get("@graph", [])
			return JSONResponse(
				status_code=200,
				content={
					"@context": {"@vocab": "https://schema.org/"},
					"@graph": condensed_graph,
					"evi:condensationStats": metadata.get("evi:condensationStats"),
					"evi:sourceROCrate": metadata.get("evi:sourceROCrate"),
				}
			)

	# Check for in-progress task
	task_doc = appConfig.asyncCollection.find_one({
		"task_type": "CondensedROCrateBuild",
		"rocrate_id": ark_id,
		"status": {"$in": ["PENDING", "PROCESSING"]}
	}, {"_id": 0})

	if task_doc:
		return JSONResponse(
			status_code=202,
			content={
				"message": "Condensation in progress",
				"task_id": task_doc["guid"],
				"status": task_doc["status"],
				"status_endpoint": f"/rocrate/condense/status/{task_doc['guid']}"
			}
		)

	# Auto-trigger condensation
	task_guid = str(uuid.uuid4())
	task_data = {
		"guid": task_guid,
		"task_type": "CondensedROCrateBuild",
		"rocrate_id": ark_id,
		"owner_email": "system@fairscape.org",
		"threshold": threshold,
		"max_member_ids": 0,
		"status": "PENDING",
		"time_created": datetime.datetime.utcnow(),
	}

	appConfig.asyncCollection.insert_one(task_data)

	condense_rocrate_task.delay(
		task_guid=task_guid,
		rocrate_id=ark_id,
		threshold=threshold,
		max_member_ids=0,
		user_email="system@fairscape.org",
	)

	return JSONResponse(
		status_code=202,
		content={
			"message": "Condensation auto-triggered",
			"task_id": task_guid,
			"status_endpoint": f"/rocrate/condense/status/{task_guid}"
		}
	)
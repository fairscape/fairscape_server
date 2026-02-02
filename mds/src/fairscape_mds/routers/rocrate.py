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

from fairscape_mds.models.user import UserWriteModel
from fairscape_mds.models.identifier import StoredIdentifier
from fairscape_mds.core.config import appConfig
from fairscape_models.rocrate import ROCrateV1_2, ROCrateMetadataElem
from fairscape_mds.deps import getCurrentUser
from fairscape_mds.worker import celeryUploadROCrate, score_ai_ready_task

from fairscape_models.conversion.converter import ROCToTargetConverter
from fairscape_models.conversion.mapping.croissant import MAPPING_CONFIGURATION as CROISSANT_MAPPING

import pathlib

rocrateRequest = FairscapeROCrateRequest(appConfig)

rocrateRouter = APIRouter(prefix="", tags=['evi', 'rocrate'])


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
	"/rocrate/ai-ready-score/ark:{NAAN}/{postfix}",
	summary="Get or initiate AI-Ready Score for an RO-Crate (Public)",
	response_description="AI-Ready Score or task status"
)
def get_or_create_ai_ready_score(
	NAAN: str,
	postfix: str
):
	ark_id = f"ark:{NAAN}/{postfix}"
	
	entity = appConfig.identifierCollection.find_one({"@id": ark_id}, {"_id": 0})
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
		score_entity = appConfig.identifierCollection.find_one({"@id": score_id}, {"_id": 0})
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
	"/rocrate/ai-ready-score/ark:{NAAN}/{postfix}/rescore",
	summary="Rescore an existing AI-Ready Score (Public)",
	status_code=202
)
def rescore_ai_ready_score(
	NAAN: str,
	postfix: str
):
	ark_id = f"ark:{NAAN}/{postfix}"
	
	entity = appConfig.identifierCollection.find_one({"@id": ark_id}, {"_id": 0})
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
	existing_score = appConfig.identifierCollection.find_one({"@id": score_id})
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
from celery import chain
import datetime
import mimetypes

from fairscape_mds.core.config import appConfig, celeryApp
from fairscape_mds.crud.rocrate import FairscapeROCrateRequest
from fairscape_mds.models.user import UserWriteModel
from fairscape_mds.models.identifier import (
	MetadataTypeEnum,
	StoredIdentifier
)
from fairscape_mds.crud.identifier import IdentifierRequest

from fairscape_mds.crud.evidence_graph import FairscapeEvidenceGraphRequest
from fairscape_mds.crud.AIReady import FairscapeAIReadyScoreRequest
from fairscape_mds.crud.llm_assist import FairscapeLLMAssistRequest

from fairscape_models.conversion.models.AIReady import AIReadyScore
from fairscape_models.conversion.mapping.AIReady import (
    _score_fairness, _score_provenance, _score_characterization,
    _score_pre_model, _score_ethics, _score_sustainability,
    _score_computability
)

rocrateRequests = FairscapeROCrateRequest(appConfig)
evidenceGraphRequests = FairscapeEvidenceGraphRequest(appConfig)
llmAssistRequests = FairscapeLLMAssistRequest(appConfig)
identifierRequestFactory = IdentifierRequest(appConfig)

def celeryUploadROCrate(transactionGUID: str):
    ''' Chain Together Tasks for Uploading an ROCrate
    '''
    processChain = chain(processROCrate.s(transactionGUID), processStatisticsROCrate.s())
    processChain()

@celeryApp.task(name='fairscape_mds.worker.processStatisticsROCrate')
def processStatisticsROCrate(guid):
    print(f"Processing Statistics: {guid}")

    # query mongo
    cursor = identifierRequestFactory.config.identifierCollection.find(
        {
            "metadata.isPartOf.@id": guid,
            "@type": str(MetadataTypeEnum.DATASET.value),
            "$or": [
                {"distribution.location.path": {"$regex": ".csv$"}},
                {"distribution.location.path": {"$regex": ".tsv$"}},
                {"distribution.location.path": {"$regex": ".hdf5$"}},
                {"distribution.location.path": {"$regex": ".parquet$"}},
            ]
        },
        projection={
           "_id": False
        }
        )

    # TODO split into multiple tasks
    for elem in cursor:
        datasetElem = StoredIdentifier.model_validate(elem)
        datasetPath = datasetElem.distribution.location.path

        stats = identifierRequestFactory.generateStatistics(
            guid=datasetElem.guid, 
            fileName=datasetPath
            )


@celeryApp.task(name='fairscape_mds.worker.processROCrate')
def processROCrate(transactionGUID: str):
    print(f"Starting Job: {transactionGUID}")
    rocrateRequests.processROCrate(transactionGUID)

    # get the rocrate guid
    uploadAttempt = rocrateRequests.getUpload(transactionGUID)
    return uploadAttempt.rocrateGUID


#Are the guids supposed to be @id?
@celeryApp.task(name='fairscape_mds.worker.build_evidence_graph_task', bind=True)
def build_evidence_graph_task(self, task_guid: str, user_email: str, naan: str, postfix: str):
    print(f"Starting Evidence Graph Build Job: Task GUID {task_guid} for ark:{naan}/{postfix}")

    try:
        appConfig.asyncCollection.update_one(
            {"guid": task_guid},
            {"$set": {"status": "PROCESSING", "time_started": datetime.datetime.utcnow()}}
        )

        user_data = appConfig.userCollection.find_one({"email": user_email})
        if not user_data:
            error_msg = f"User {user_email} not found."
            print(error_msg)
            appConfig.asyncCollection.update_one(
                {"guid": task_guid},
                {"$set": {
                    "status": "FAILURE",
                    "error": {"message": error_msg},
                    "time_finished": datetime.datetime.utcnow()
                }}
            )
            return {"status": "FAILURE", "error": error_msg}

        requesting_user = UserWriteModel.model_validate(user_data)

        response = evidenceGraphRequests.build_evidence_graph_for_node(
            requesting_user=requesting_user,
            naan=naan,
            postfix=postfix
        )

        if response.success:
            evidence_graph_id = response.model.guid if response.model else None
            print(f"Successfully built evidence graph {evidence_graph_id} for Task GUID {task_guid}")
            appConfig.asyncCollection.update_one(
                {"guid": task_guid},
                {"$set": {
                    "status": "SUCCESS",
                    "result": {"evidence_graph_id": evidence_graph_id},
                    "time_finished": datetime.datetime.utcnow()
                }}
            )
            return {"status": "SUCCESS", "evidence_graph_id": evidence_graph_id}
        else:
            error_detail = response.error if isinstance(response.error, dict) else {"message": str(response.error)}
            print(f"Failed to build evidence graph for Task GUID {task_guid}: {error_detail}")
            appConfig.asyncCollection.update_one(
                {"guid": task_guid},
                {"$set": {
                    "status": "FAILURE",
                    "error": error_detail,
                    "time_finished": datetime.datetime.utcnow()
                }}
            )
            return {"status": "FAILURE", "error": error_detail}

    except Exception as e:
        import traceback
        error_msg = f"Unexpected error in build_evidence_graph_task (Task GUID {task_guid}): {str(e)}"
        print(error_msg)
        traceback.print_exc()
        appConfig.asyncCollection.update_one(
            {"guid": task_guid},
            {"$set": {
                "status": "FAILURE",
                "error": {"message": "An unexpected error occurred.", "details": str(e)},
                "time_finished": datetime.datetime.utcnow()
            }}
        )
        return {"status": "FAILURE", "error": {"message": "An unexpected server error occurred."}}

@celeryApp.task(name='fairscape_mds.worker.score_ai_ready_task', bind=True)
def score_ai_ready_task(self, task_guid: str, rocrate_id: str):
    print(f"Starting AI-Ready Scoring Task: {task_guid} for {rocrate_id}")
    
    try:
        appConfig.asyncCollection.update_one(
            {"guid": task_guid},
            {"$set": {
                "status": "PROCESSING",
                "time_started": datetime.datetime.utcnow()
            }}
        )
        
        ai_ready_request = FairscapeAIReadyScoreRequest(appConfig)
        
        metadata_graph = ai_ready_request.build_metadata_graph_for_rocrate(rocrate_id)
        
        if not metadata_graph:
            error_msg = f"No metadata found for RO-Crate {rocrate_id}"
            appConfig.asyncCollection.update_one(
                {"guid": task_guid},
                {"$set": {
                    "status": "FAILURE",
                    "error": {"message": error_msg},
                    "time_finished": datetime.datetime.utcnow()
                }}
            )
            return {"status": "FAILURE", "error": error_msg}
        
        root_data = None
        for entity in metadata_graph:
            if entity.get("@id") == rocrate_id:
                root_data = entity
                break
        
        if not root_data:
            error_msg = f"Root entity not found for {rocrate_id}"
            appConfig.asyncCollection.update_one(
                {"guid": task_guid},
                {"$set": {
                    "status": "FAILURE",
                    "error": {"message": error_msg},
                    "time_finished": datetime.datetime.utcnow()
                }}
            )
            return {"status": "FAILURE", "error": error_msg}
        
        score = AIReadyScore(name=f"AI-Ready Score for {root_data.get('name', rocrate_id)}")
        _score_fairness(score.fairness, root_data)
        _score_provenance(score.provenance, root_data, metadata_graph)
        _score_characterization(score.characterization, root_data, metadata_graph)
        _score_pre_model(score.pre_model_explainability, root_data, metadata_graph)
        _score_ethics(score.ethics, root_data)
        _score_sustainability(score.sustainability, root_data)
        _score_computability(score.computability, root_data, metadata_graph)
        
        response = ai_ready_request.create_ai_ready_score(
            rocrate_id=rocrate_id,
            score=score,
            owner_email="system@fairscape.org"
        )
        
        if response.success:
            score_id = response.model.guid
            print(f"Successfully created AI-Ready Score {score_id} for {rocrate_id}")
            appConfig.asyncCollection.update_one(
                {"guid": task_guid},
                {"$set": {
                    "status": "SUCCESS",
                    "result": {"ai_ready_score_id": score_id},
                    "time_finished": datetime.datetime.utcnow()
                }}
            )
            return {"status": "SUCCESS", "ai_ready_score_id": score_id}
        else:
            error_detail = response.error if isinstance(response.error, dict) else {"message": str(response.error)}
            print(f"Failed to create AI-Ready Score for {rocrate_id}: {error_detail}")
            appConfig.asyncCollection.update_one(
                {"guid": task_guid},
                {"$set": {
                    "status": "FAILURE",
                    "error": error_detail,
                    "time_finished": datetime.datetime.utcnow()
                }}
            )
            return {"status": "FAILURE", "error": error_detail}
        
    except Exception as e:
        import traceback
        error_msg = f"Unexpected error in score_ai_ready_task: {str(e)}"
        print(error_msg)
        traceback.print_exc()
        appConfig.asyncCollection.update_one(
            {"guid": task_guid},
            {"$set": {
                "status": "FAILURE",
                "error": {"message": "An unexpected error occurred", "details": str(e)},
                "time_finished": datetime.datetime.utcnow()
            }}
        )
        return {"status": "FAILURE", "error": {"message": "An unexpected server error occurred"}}

@celeryApp.task(name='fairscape_mds.worker.process_llm_assist_task', bind=True)
def process_llm_assist_task(self, task_guid: str):
    print(f"Starting LLM Assist Processing Task: {task_guid}")
    
    try:
        result_json = llmAssistRequests.process_pdfs_with_llm(task_guid)
        
        print(f"Successfully processed LLM task {task_guid}")
        return {"status": "SUCCESS", "result": result_json}
        
    except Exception as e:
        import traceback
        error_msg = f"Unexpected error in process_llm_assist_task: {str(e)}"
        print(error_msg)
        traceback.print_exc()
        
        appConfig.asyncCollection.update_one(
            {"@id": task_guid},
            {"$set": {
                "status": "FAILURE",
                "error": {"message": "An unexpected worker error occurred", "details": str(e)},
                "time_finished": datetime.datetime.utcnow()
            }}
        )
        return {"status": "FAILURE", "error": error_msg}
    
if __name__ == '__main__':
    args = ['worker', '--loglevel=INFO']
    celeryApp.worker_main(argv=args)
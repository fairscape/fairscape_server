from celery import Celery
import datetime

from fairscape_mds.core.config import appConfig, celeryApp
from fairscape_mds.crud.rocrate import FairscapeROCrateRequest
from fairscape_mds.models.user import UserWriteModel

from fairscape_mds.backend.evidence_graph_crud import FairscapeEvidenceGraphRequest

rocrateRequests = FairscapeROCrateRequest(appConfig)
evidenceGraphRequests = FairscapeEvidenceGraphRequest(appConfig)

@celeryApp.task(name='fairscape_mds.worker.processROCrate')
def processROCrate(transactionGUID: str):
    print(f"Starting Job: {transactionGUID}")
    return rocrateRequests.processROCrate(transactionGUID)

@celeryApp.task(name='fairscape_mds.worker.build_evidence_graph_task', bind=True)
def build_evidence_graph_task(self, task_guid: str, user_email: str, naan: str, postfix: str):
    print(f"Starting Evidence Graph Build Job: Task GUID {task_guid} for ark:{naan}/{postfix}")

    try:
        config.asyncCollection.update_one(
            {"guid": task_guid},
            {"$set": {"status": "PROCESSING", "time_started": datetime.datetime.utcnow()}}
        )

        user_data = config.userCollection.find_one({"email": user_email})
        if not user_data:
            error_msg = f"User {user_email} not found."
            print(error_msg)
            config.asyncCollection.update_one(
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
            config.asyncCollection.update_one(
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
            config.asyncCollection.update_one(
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
        config.asyncCollection.update_one(
            {"guid": task_guid},
            {"$set": {
                "status": "FAILURE",
                "error": {"message": "An unexpected error occurred.", "details": str(e)},
                "time_finished": datetime.datetime.utcnow()
            }}
        )
        return {"status": "FAILURE", "error": {"message": "An unexpected server error occurred."}}


if __name__ == '__main__':
    args = ['worker', '--loglevel=INFO']
    celeryApp.worker_main(argv=args)
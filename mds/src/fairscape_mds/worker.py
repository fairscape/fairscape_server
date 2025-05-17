from celery import Celery
from fairscape_mds.backend.models import FairscapeROCrateRequest
from fairscape_mds.backend.backend import s3, minioDefaultBucket, identifierCollection, userCollection, rocrateCollection, asyncCollection

# Initialize Celery app
app = Celery('fairscape_mds.worker')
app.conf.broker_url = "redis://localhost:6379//"
app.conf.update(
    task_concurrency=4,
    worker_prefetch_multiplier=4,
    broker_connection_retry_on_startup=True  # Fix the deprecation warning
)

# Initialize the ROCrate request handler
rocrateRequests = FairscapeROCrateRequest(
    minioClient=s3,
    minioBucket=minioDefaultBucket,
    identifierCollection=identifierCollection,
    userCollection=userCollection,
    rocrateCollection=rocrateCollection,
    asyncCollection=asyncCollection
)

@app.task(name='fairscape_mds.worker.processROCrate')
def processROCrate(transactionGUID: str):
    return rocrateRequests.processROCrate(transactionGUID)

if __name__ == '__main__':
    args = ['worker', '--loglevel=INFO']
    app.worker_main(argv=args)
from celery import Celery
from fairscape_mds.backend.models import FairscapeConfig, FairscapeROCrateRequest
from fairscape_mds.backend.backend import (
    config,
    brokerURL
    )

# Initialize Celery app
app = Celery('fairscape_mds.worker')
app.conf.broker_url = f"redis://{brokerURL}"
app.conf.update(
    task_concurrency=4,
    worker_prefetch_multiplier=4,
    broker_connection_retry_on_startup=True
)


rocrateRequests = FairscapeROCrateRequest(config)

@app.task(name='fairscape_mds.worker.processROCrate')
def processROCrate(transactionGUID: str):
    return rocrateRequests.processROCrate(transactionGUID)

if __name__ == '__main__':
    args = ['worker', '--loglevel=INFO']
    app.worker_main(argv=args)
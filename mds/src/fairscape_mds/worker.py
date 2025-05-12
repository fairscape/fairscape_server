from fairscape_mds.backend.backend import *
from fairscape_mds.backend.models import *
from celery import Celery


rocrateRequests = FairscapeROCrateRequest(
	minioClient=s3,
	minioBucket=minioDefaultBucket,
#	minioDefaultPath="fairscape",
	identifierCollection=identifierCollection,
	userCollection=userCollection,
	rocrateCollection=rocrateCollection,
	asyncCollection=asyncCollection	
)


@celeryApp.task()
def processROCrate(transactionGUID: str):
	rocrateRequests.processROCrate(transactionGUID)


if __name__ == '__main__':
	args = ['worker', '--loglevel=INFO']

	celeryApp.worker_main(argv=args)

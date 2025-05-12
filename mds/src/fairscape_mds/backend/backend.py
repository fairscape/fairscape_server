import pymongo
import boto3
from urllib.parse import quote_plus
from botocore.client import Config
from pymongo.collection import Collection
from celery import Celery

from fairscape_mds.backend.models import FairscapeUserRequest, FairscapeDatasetRequest

# local test parameters
mongoUser = "mongotestaccess"
mongoPassword = "mongotestsecret"
mongoHost = "localhost"
mongoPort = "27017"
mongoDB = "fairscape"
mongoUserCollection = "users"
mongoIdentifierCollection = "mds"
mongoROCrateCollection = "rocrate"
mongoAsyncCollection = "async"

minioAccessKey = "miniotestadmin"
minioSecretKey = "miniotestsecret"
minioEndpoint = "http://localhost:9000"
minioDefaultBucket = "fairscape"
minioDefaultPath = "fairscape"

# redis settings
brokerURL = "localhost:6379"

jwtSecret = "test-jwt-secret"

# create a mongo client
connection_string = f"mongodb://{quote_plus(mongoUser)}:{quote_plus(mongoPassword)}@{mongoHost}:{mongoPort}"
mongoClient = pymongo.MongoClient(connection_string)

mongoDB = mongoClient[mongoDB]

userCollection = mongoDB[mongoUserCollection]
identifierCollection = mongoDB[mongoIdentifierCollection]
rocrateCollection = mongoDB[mongoROCrateCollection]
asyncCollection = mongoDB[mongoAsyncCollection]

# create a boto s3 client
s3 = boto3.client('s3',
        endpoint_url=minioEndpoint,
        aws_access_key_id=minioAccessKey,
        aws_secret_access_key=minioSecretKey,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

try:
    s3.create_bucket(Bucket=minioDefaultBucket)
except:
    pass

# set up support for compression headers
def _add_header(request, **kwargs):
    request.headers.add_header('x-minio-extract', 'true')

event_system = s3.meta.events
event_system.register_first('before-sign.s3.*', _add_header)

celeryApp = Celery()
celeryApp.conf.broker_url = "redis://" + brokerURL

celeryApp.conf.update(
    task_concurrency=4,  # Use 4 threads for concurrency
    worker_prefetch_multiplier=4  # Prefetch one task at a time
)
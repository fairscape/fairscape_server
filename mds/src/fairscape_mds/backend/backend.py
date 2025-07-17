import pymongo
import boto3
from urllib.parse import quote_plus
from botocore.client import Config
from pymongo.collection import Collection
from celery import Celery

from fairscape_mds.backend.models import FairscapeConfig
import os

# local test parameters
mongoUser = os.environ.get("FAIRSCAPE_MONGO_ACCESS_KEY", "mongotestaccess")
mongoPassword = os.environ.get("FAIRSCAPE_MONGO_SECRET_KEY", "mongotestsecret")
mongoHost = os.environ.get("FAIRSCAPE_MONGO_HOST", "localhost")
mongoPort = os.environ.get("FAIRSCAPE_MONGO_PORT", "27017")
mongoDatabaseName = os.environ.get("FAIRSCAPE_MONGO_DATABASE", "fairscape")
mongoUserCollection = os.environ.get("FAIRSCAPE_MONGO_USER_COLLECTION", "users")
mongoIdentifierCollection = os.environ.get("FAIRSCAPE_MONGO_IDENTIFIER_COLLECTION", "mds")
mongoROCrateCollection = os.environ.get("FAIRSCAPE_MONGO_ROCRATE_COLLECTION", "rocrate")
mongoAsyncCollection =  os.environ.get("FAIRSCAPE_MONGO_ASYNC_COLLECTION", "async")
mongoTokensCollection =  os.environ.get("FAIRSCAPE_MONGO_TOKENS_COLLECTION", "tokens")


minioAccessKey = os.environ.get("FAIRSCAPE_MINIO_ACCESS_KEY", "miniotestadmin")
minioSecretKey = os.environ.get("FAIRSCAPE_MINIO_SECRET_KEY", "miniotestsecret")
minioEndpoint = os.environ.get("FAIRSCAPE_MINIO_URI", "http://localhost:9000")
minioDefaultBucket = os.environ.get("FAIRSCAPE_MINIO_DEFAULT_BUCKET", "fairscape")
minioDefaultPath = os.environ.get("FAIRSCAPE_MINIO_DEFAULT_BUCKET_PATH", "fairscape")

# redis settings
redisHost = os.environ.get("FAIRSCAPE_REDIS_HOST", "localhost")
redisPort = os.environ.get("FAIRSCAPE_REDIS_PORT", "6379")
brokerURL = f"{redisHost}:{redisPort}"

# JWT Secret
jwtSecret = os.environ.get("FAIRSCAPE_JWT_SECRET", "test-jwt-secret")
adminGroup = os.environ.get("FAIRSCAPE_ADMIN_GROUP", "admin")

# Fairscape base URL
baseUrl = os.environ.get("FAIRSCAPE_BASE_URL", "http://localhost:8080/api")

# create a mongo client
if "localhost" in baseUrl:
    connection_string = f"mongodb://{quote_plus(mongoUser)}:{quote_plus(mongoPassword)}@{mongoHost}:{mongoPort}/{mongoDatabaseName}?authSource=admin&retryWrites=true"
else:
    connection_string = f"mongodb://{quote_plus(mongoUser)}:{quote_plus(mongoPassword)}@{mongoHost}:{mongoPort}/{mongoDatabaseName}?retryWrites=true"
mongoClient = pymongo.MongoClient(connection_string)

mongoDB = mongoClient[mongoDatabaseName]

userCollection = mongoDB[mongoUserCollection]
identifierCollection = mongoDB[mongoIdentifierCollection]
rocrateCollection = mongoDB[mongoROCrateCollection]
asyncCollection = mongoDB[mongoAsyncCollection]
tokensCollection = mongoDB[mongoTokensCollection]

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

config = FairscapeConfig(
    minioClient=s3,
    minioBucket=minioDefaultBucket,
	minioDefaultPath=minioDefaultPath,
	userCollection=userCollection,
	identifierCollection=identifierCollection,
	asyncCollection=asyncCollection,
	rocrateCollection=rocrateCollection,
	tokensCollection=tokensCollection,
    jwtSecret=jwtSecret,
	adminGroup=adminGroup,
    baseUrl=baseUrl
)
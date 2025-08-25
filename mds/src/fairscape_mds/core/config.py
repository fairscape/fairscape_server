from pydantic import (
    computed_field,
    Field
)

from pydantic_settings import BaseSettings, SettingsConfigDict
import boto3
from botocore.client import Config
from urllib.parse import quote_plus
from typing import Optional
from celery import Celery
import pymongo



class Settings(BaseSettings):
    model_config = SettingsConfigDict(
            env_file="../.env",
            env_ignore_empty=True,
            extra="ignore"
        )
    FAIRSCAPE_MONGO_ACCESS_KEY: str
    FAIRSCAPE_MONGO_SECRET_KEY: str
    FAIRSCAPE_MONGO_HOST: str
    FAIRSCAPE_MONGO_PORT: str
    FAIRSCAPE_MONGO_DATABASE: str
    FAIRSCAPE_MONGO_AUTH_DATABASE: Optional[str] = Field(default=None)
    FAIRSCAPE_MONGO_USER_COLLECTION: str
    FAIRSCAPE_MONGO_IDENTIFIER_COLLECTION: str
    FAIRSCAPE_MONGO_ROCRATE_COLLECTION: str
    FAIRSCAPE_MONGO_ASYNC_COLLECTION: str
    FAIRSCAPE_MONGO_TOKENS_COLLECTION: str

    FAIRSCAPE_MINIO_ACCESS_KEY: str
    FAIRSCAPE_MINIO_SECRET_KEY: str
    FAIRSCAPE_MINIO_URI: str
    FAIRSCAPE_MINIO_DEFAULT_BUCKET: str
    FAIRSCAPE_MINIO_DEFAULT_BUCKET_PATH: str

    FAIRSCAPE_REDIS_HOST: str
    FAIRSCAPE_REDIS_PORT: str
    FAIRSCAPE_REDIS_JOB_DATABASE: str
    FAIRSCAPE_REDIS_RESULT_DATABASE: str

    @computed_field
    @property
    def FAIRSCAPE_REDIS_BROKER_URL(self) -> str:
        return f"{self.redisHost}:{self.redisPort}"

    FAIRSCAPE_JWT_SECRET: str 
    FAIRSCAPE_ADMIN_GROUP: str

    FAIRSCAPE_BASE_URL: str



settings = Settings()


class FairscapeConfig():
	def __init__(
			self,
			minioClient, 
			minioBucket: str, 
			minioDefaultPath: str,
			identifierCollection,
			userCollection, 
			asyncCollection,
			rocrateCollection,
			tokensCollection,
			jwtSecret: str,
			adminGroup: str,
			baseUrl: str
	):
		self.minioClient=minioClient
		self.minioBucket=minioBucket
		self.minioDefaultPath=minioDefaultPath 
		self.identifierCollection=identifierCollection
		self.userCollection=userCollection
		self.rocrateCollection=rocrateCollection
		self.asyncCollection=asyncCollection
		self.tokensCollection=tokensCollection
		self.jwtSecret = jwtSecret
		self.adminGroup = adminGroup
		self.baseUrl = baseUrl
  

		

	def __str__(self):
		minioStr = f"Minio:\n\tMinioClient: {self.minioClient}\n\tBucket: {self.minioBucket}\n\tDefaultPath: {self.minioDefaultPath}"
		return f"Backend Configuration Object:\n{minioStr}"



# TODO clean up client string generation
mongoUser = settings.FAIRSCAPE_MONGO_ACCESS_KEY
mongoPassword = settings.FAIRSCAPE_MONGO_SECRET_KEY
mongoHost = settings.FAIRSCAPE_MONGO_HOST
mongoPort = settings.FAIRSCAPE_MONGO_PORT
mongoDatabaseName = settings.FAIRSCAPE_MONGO_DATABASE
mongoAuthDatabaseName = settings.FAIRSCAPE_MONGO_AUTH_DATABASE


# create a mongo client
if "localhost" in settings.FAIRSCAPE_BASE_URL:
    connection_string = f"mongodb://{quote_plus(mongoUser)}:{quote_plus(mongoPassword)}@{mongoHost}:{mongoPort}/{mongoDatabaseName}?authSource=admin&retryWrites=true"

if mongoAuthDatabaseName:
    connection_string = f"mongodb://{quote_plus(mongoUser)}:{quote_plus(mongoPassword)}@{mongoHost}:{mongoPort}/{mongoDatabaseName}?authSource={mongoAuthDatabaseName}&retryWrites=true"
else:
    connection_string = f"mongodb://{quote_plus(mongoUser)}:{quote_plus(mongoPassword)}@{mongoHost}:{mongoPort}/{mongoDatabaseName}?retryWrites=true"


mongoClient = pymongo.MongoClient(connection_string)
mongoDB = mongoClient[settings.FAIRSCAPE_MONGO_DATABASE]
userCollection = mongoDB[settings.FAIRSCAPE_MONGO_USER_COLLECTION]
identifierCollection = mongoDB[settings.FAIRSCAPE_MONGO_IDENTIFIER_COLLECTION]
rocrateCollection = mongoDB[settings.FAIRSCAPE_MONGO_ROCRATE_COLLECTION]
asyncCollection = mongoDB[settings.FAIRSCAPE_MONGO_ASYNC_COLLECTION]
tokensCollection = mongoDB[settings.FAIRSCAPE_MONGO_TOKENS_COLLECTION]


# create a boto s3 client
s3 = boto3.client('s3',
        endpoint_url=settings.FAIRSCAPE_MINIO_URI,
        aws_access_key_id= settings.FAIRSCAPE_MINIO_ACCESS_KEY,
        aws_secret_access_key= settings.FAIRSCAPE_MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        aws_session_token=None,
        region_name='us-east-1'
    )

# set up support for compression headers
def _add_header(request, **kwargs):
    request.headers.add_header('x-minio-extract', 'true')

s3_event_system = s3.meta.events
s3_event_system.register_first('before-sign.s3.*', _add_header)


celeryApp = Celery()
celeryApp.conf.broker_url = "redis://" + settings.FAIRSCAPE_REDIS_HOST + ":" +  settings.FAIRSCAPE_REDIS_PORT  + "/" + settings.FAIRSCAPE_REDIS_JOB_DATABASE
celeryApp.conf.result_backend = "redis://" + settings.FAIRSCAPE_REDIS_HOST + ":" + settings.FAIRSCAPE_REDIS_PORT  + "/" + settings.FAIRSCAPE_REDIS_RESULT_DATABASE


celeryApp.conf.update(
    task_concurrency=4,  # Use 4 threads for concurrency
    worker_prefetch_multiplier=4  # Prefetch one task at a time
)


appConfig = FairscapeConfig(
    minioClient=s3,
    minioBucket=settings.FAIRSCAPE_MINIO_DEFAULT_BUCKET,
	minioDefaultPath=settings.FAIRSCAPE_MINIO_DEFAULT_BUCKET_PATH,
	userCollection=userCollection,
	identifierCollection=identifierCollection,
	asyncCollection=asyncCollection,
	rocrateCollection=rocrateCollection,
	tokensCollection=tokensCollection,
    jwtSecret=settings.FAIRSCAPE_JWT_SECRET,
	adminGroup=settings.FAIRSCAPE_ADMIN_GROUP,
    baseUrl=settings.FAIRSCAPE_BASE_URL
)

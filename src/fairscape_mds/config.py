from enum import Enum
from pydantic import (
    BaseModel,
    Field
)
from typing import (
    Optional
)
import os
import pathlib
from urllib.parse import quote_plus
from pymongo import MongoClient, ASCENDING
import ldap3
import minio
from functools import lru_cache
import uvicorn

from dotenv import dotenv_values
import urllib3

import logging
logging.getLogger('pymongo').setLevel(logging.INFO)

urllib3.disable_warnings()

@lru_cache()
def cached_dotenv(env_path: str = '.env'):
    config = {
        **dotenv_values(env_path),
        **os.environ,
    }
    return config

def create_fairscape_config(config_values):
    server_mongo_config = MongoConfig.model_validate(
        {
            'host': config_values.get('FAIRSCAPE_MONGO_HOST'), 
            'port': config_values.get("FAIRSCAPE_MONGO_PORT", "27017"),
            'user': config_values.get('FAIRSCAPE_MONGO_ACCESS_KEY'),
            'password': config_values.get('FAIRSCAPE_MONGO_SECRET_KEY'),
            'db': config_values.get('FAIRSCAPE_MONGO_DATABASE'),
            'identifier_collection': config_values.get('FAIRSCAPE_MONGO_IDENTIFIER_COLLECTION'),
            'user_collection': config_values.get('FAIRSCAPE_MONGO_USER_COLLECTION'),
            'rocrate_collection': config_values.get('FAIRSCAPE_MONGO_ROCRATE_COLLECTION'),
            'async_collection': config_values.get('FAIRSCAPE_MONGO_ASYNC_COLLECTION', 'async')
        }
        )

    if config_values.get("FAIRSCAPE_MINIO_CERT_CHECK"):
        checkCert = bool(config_values.get("FAIRSCAPE_MINIO_CERT_CHECK")=="True")
    else:
        checkCert = False

    if config_values.get("FAIRSCAPE_MINIO_SECURE"):
        secure = bool(config_values.get("FAIRSCAPE_MINIO_SECURE")=="True")
    else:
        secure = False

    server_minio_config = MinioConfig(
        host= config_values.get("FAIRSCAPE_MINIO_URI"),
        port=config_values.get("FAIRSCAPE_MINIO_PORT"),
        access_key = config_values.get("FAIRSCAPE_MINIO_ACCESS_KEY"),
        secret_key = config_values.get("FAIRSCAPE_MINIO_SECRET_KEY"),
        default_bucket= config_values.get("FAIRSCAPE_MINIO_DEFAULT_BUCKET"), 
        default_bucket_path = config_values.get("FAIRSCAPE_MINIO_DEFAULT_BUCKET_PATH"),
        rocrate_bucket=config_values.get("FAIRSCAPE_MINIO_ROCRATE_BUCKET"),
        rocrate_bucket_path = config_values.get("FAIRSCAPE_MINIO_ROCRATE_BUCKET_PATH"),
        secure= secure,
        check_cert= checkCert
    )

    # defaulting several redis DB values
    redisDB=int(config_values.get("FAIRSCAPE_REDIS_DATABASE", 0))
    redisResultDB=int(config_values.get("FAIRSCAPE_REDIS_RESULT_DATABASE", 1))
    redisPort= int(config_values.get("FAIRSCAPE_REDIS_PORT", 6379))

    server_redis_config = RedisConfig(
        port= redisPort,
        hostname = config_values.get("FAIRSCAPE_REDIS_HOST", 'localhost'),
        username= config_values.get("FAIRSCAPE_REDIS_USERNAME"),
        password= config_values.get("FAIRSCAPE_REDIS_PASSWORD"),
        database= redisDB,
        result_database = redisResultDB
    )

    # setup LDAP Config
    server_ldap_config = LDAPConfig.model_validate(
        {
            "hostname": config_values.get("FAIRSCAPE_LDAP_HOST"), 
            "port": config_values.get("FAIRSCAPE_LDAP_PORT"),
            "baseDN": config_values.get("FAIRSCAPE_LDAP_BASE_DN"),
            "usersDN": config_values.get("FAIRSCAPE_LDAP_USERS_DN"),
            "groupsDN": config_values.get("FAIRSCAPE_LDAP_GROUPS_DN"),
            "adminDN": config_values.get("FAIRSCAPE_LDAP_ADMIN_DN"),
            "adminPassword": config_values.get("FAIRSCAPE_LDAP_ADMIN_PASSWORD"),
            }
        )

    # TODO support multiple NAANs
    
    return FairscapeConfig(
            host = config_values.get('FAIRSCAPE_HOST'),
            port = config_values.get('FAIRSCAPE_PORT'),
            jwtSecret = config_values.get('FAIRSCAPE_JWT_SECRET', 'testjwtsecret'),
            passwordSalt = config_values.get('FAIRSCAPE_PASSWORD_SALT', 'testsalt'),
            NAAN = config_values.get('FAIRSCAPE_NAAN', '59852'),
            url = config_values.get("FAIRSCAPE_URL"),
            mongo = server_mongo_config,
            minio = server_minio_config,
            redis = server_redis_config,
            ldap = server_ldap_config
            )
    pass


@lru_cache()
def get_fairscape_config(env_path: str = '/fairscape/.env'):    
    config_values = cached_dotenv(env_path)

    server_mongo_config = MongoConfig.model_validate(
        {
            'host': config_values.get('FAIRSCAPE_MONGO_HOST'), 
            'port': config_values.get("FAIRSCAPE_MONGO_PORT", "27017"),
            'user': config_values.get('FAIRSCAPE_MONGO_ACCESS_KEY'),
            'password': config_values.get('FAIRSCAPE_MONGO_SECRET_KEY'),
            'db': config_values.get('FAIRSCAPE_MONGO_DATABASE'),
            'identifier_collection': config_values.get('FAIRSCAPE_MONGO_IDENTIFIER_COLLECTION'),
            'user_collection': config_values.get('FAIRSCAPE_MONGO_USER_COLLECTION'),
            'rocrate_collection': config_values.get('FAIRSCAPE_MONGO_ROCRATE_COLLECTION'),
            'async_collection': config_values.get('FAIRSCAPE_MONGO_ASYNC_COLLECTION', 'async')
        }
        )

    if config_values.get("FAIRSCAPE_MINIO_CERT_CHECK"):
        checkCert = bool(config_values.get("FAIRSCAPE_MINIO_CERT_CHECK")=="True")
    else:
        checkCert = False

    if config_values.get("FAIRSCAPE_MINIO_SECURE"):
        secure = bool(config_values.get("FAIRSCAPE_MINIO_SECURE")=="True")
    else:
        secure = False

    server_minio_config = MinioConfig(
        host= config_values.get("FAIRSCAPE_MINIO_URI"),
        port=config_values.get("FAIRSCAPE_MINIO_PORT"),
        access_key = config_values.get("FAIRSCAPE_MINIO_ACCESS_KEY"),
        secret_key = config_values.get("FAIRSCAPE_MINIO_SECRET_KEY"),
        default_bucket= config_values.get("FAIRSCAPE_MINIO_DEFAULT_BUCKET"), 
        default_bucket_path = config_values.get("FAIRSCAPE_MINIO_DEFAULT_BUCKET_PATH"),
        rocrate_bucket=config_values.get("FAIRSCAPE_MINIO_ROCRATE_BUCKET"),
        rocrate_bucket_path = config_values.get("FAIRSCAPE_MINIO_ROCRATE_BUCKET_PATH"),
        secure= secure,
        check_cert= checkCert
    )

    # defaulting several redis DB values
    redisDB=int(config_values.get("FAIRSCAPE_REDIS_DATABASE", 0))
    redisResultDB=int(config_values.get("FAIRSCAPE_REDIS_RESULT_DATABASE", 1))
    redisPort= int(config_values.get("FAIRSCAPE_REDIS_PORT", 6379))

    server_redis_config = RedisConfig(
        port= redisPort,
        hostname = config_values.get("FAIRSCAPE_REDIS_HOST", 'localhost'),
        username= config_values.get("FAIRSCAPE_REDIS_USERNAME"),
        password= config_values.get("FAIRSCAPE_REDIS_PASSWORD"),
        database= redisDB,
        result_database = redisResultDB
    )

    # setup LDAP Config
    server_ldap_config = LDAPConfig.model_validate(
        {
            "hostname": config_values.get("FAIRSCAPE_LDAP_HOST"), 
            "port": config_values.get("FAIRSCAPE_LDAP_PORT"),
            "baseDN": config_values.get("FAIRSCAPE_LDAP_BASE_DN"),
            "usersDN": config_values.get("FAIRSCAPE_LDAP_USERS_DN"),
            "groupsDN": config_values.get("FAIRSCAPE_LDAP_GROUPS_DN"),
            "adminDN": config_values.get("FAIRSCAPE_LDAP_ADMIN_DN"),
            "adminPassword": config_values.get("FAIRSCAPE_LDAP_ADMIN_PASSWORD"),
            }
        )

    # TODO support multiple NAANs
    
    return FairscapeConfig(
            host = config_values.get('FAIRSCAPE_HOST'),
            port = config_values.get('FAIRSCAPE_PORT'),
            jwtSecret = config_values.get('FAIRSCAPE_JWT_SECRET', 'testjwtsecret'),
            passwordSalt = config_values.get('FAIRSCAPE_PASSWORD_SALT', 'testsalt'),
            NAAN = config_values.get('FAIRSCAPE_NAAN', '59852'),
            url = config_values.get("FAIRSCAPE_URL"),
            mongo = server_mongo_config,
            minio = server_minio_config,
            redis = server_redis_config,
            ldap = server_ldap_config
            )


@lru_cache()
def get_mongo_client():
    fairscapeConfig = get_fairscape_config()
    return fairscapeConfig.mongo.CreateClient()

@lru_cache()
def get_minio_client():
    fairscapeConfig = get_fairscape_config()
    return fairscapeConfig.minio.CreateClient()


class MongoConfig(BaseModel):
    host: Optional[str] = Field(default="localhost")
    port: Optional[str] = Field(default="27017")
    user: Optional[str] = Field(default="mongotestaccess")
    password: Optional[str] = Field(default="mongotestsecret")
    db: Optional[str] = Field(default="fairscape")
    identifier_collection: Optional[str] = Field(default="mds")
    rocrate_collection: Optional[str] = Field(default="rocrate")
    user_collection: Optional[str] = Field(default="users")
    async_collection: Optional[str] = Field(default="async")
    session_collection: Optional[str] = Field(default="sessions")


    def CreateClient(self):

        connection_string = f"mongodb://{quote_plus(self.user)}:{quote_plus(self.password)}@{self.host}:{self.port}"
        #connection_string = f"mongodb://{quote_plus(self.user)}:{quote_plus(self.password)}@{self.host}:{self.port}/{self.db}"
        #connection_string = f"mongodb://{quote_plus(self.user)}:{quote_plus(self.password)}@{self.host}:{self.port}/?authSource=admin"
        return MongoClient(connection_string)


class MinioConfig(BaseModel):
    host: Optional[str] = Field(default="localhost")
    port: Optional[str] = Field(default=None)
    secret_key: Optional[str] = Field(default="")
    access_key: Optional[str] = Field(default="")
    default_bucket: Optional[str] = Field(default="default")
    default_bucket_path: str | None 
    rocrate_bucket: Optional[str] = Field(default="rocrate")
    rocrate_bucket_path: str | None
    secure: Optional[bool] = Field(default=False)
    cert_check: Optional[bool] = Field(default=False)


    def CreateClient(self):

        if self.port is None:
            uri = self.host
        else:
            uri = f"{self.host}:{self.port}"

        return minio.Minio(
                endpoint = uri, 
                access_key = self.access_key, 
                secret_key = self.secret_key,
                secure = self.secure,
                cert_check = self.cert_check
                )


class ComputeBackendEnum(str, Enum):
    docker = "docker"
    kubernetes = "kubernetes"


class RedisConfig(BaseModel):
    port: Optional[int] = Field(default=6379)
    hostname: Optional[str] = Field(default='localhost')
    username: Optional[str] = Field(default=None)
    password: Optional[str] = Field(default=None)
    database: Optional[int] = Field(default=0)
    result_database: Optional[int] = Field(default=1)

    def getBrokerURL(self):
        if self.username and self.password:
            return f'redis://{self.username}:{self.password}@{self.hostname}:{self.port}/{self.database}'
        else:
            return f'redis://{self.hostname}:{self.port}/{self.database}'


class K8sComputeConfig(BaseModel):
    redis: RedisConfig


class LDAPConfig(BaseModel):
    hostname: str
    port: str = Field(default="1389")
    baseDN: str
    usersDN: str
    groupsDN: str
    adminDN: str
    adminPassword: str

    def connect(self, userDN: str, userPassword: str) -> ldap3.Connection:
        """
        Connect to the LDAP server using the configuration in the LDAPConfig Class, as the passed identity

        :param LDAPConfig self: The LDAPConfig class instance
        :param str username: The optional username to bind to the LDAP server as the passed identity
        :param str password: The optional userpassword to bind to the LDAP server as a passed identity
        :return: An LDAP connection instance bound to the server
        :rtype: ldap3.Connection
        :raises ldap3.core.exceptions.LDAPBindError: Error raised when client fails to connect to the LDAP Server
        :raises ldap3.core.exceptions.LDAPException: Error raised by LDAP Client
        """
        
        ldapURI = f'ldap://{self.hostname}:{self.port}'

        try:
            server = ldap3.Server(ldapURI, get_info=ldap3.ALL)

            connection = ldap3.Connection(
                    server,
                    user=userDN,
                    password=userPassword,
                    authentication=ldap3.SIMPLE 
                    )

            bind_response = connection.bind()
            return connection

        except ldap3.core.exceptions.LDAPBindError as bindError:
            print(bindError)
            raise bindError

        except ldap3.core.exceptions.LDAPException as ldapError:
            print(ldapError)
            raise ldapError


    def connectAdmin(self):
        return self.connect(self.adminDN, self.adminPassword)


    def connectUser(self, username: str, password: str) -> ldap3.Connection:
        """
        """ 
        serverURI = f'ldap://{self.hostname}:{self.port}'

        # format the username into a full distinguished name
        if self.baseDN not in username:
            if "cn=" not in username:
                userDN = f"cn={username},{self.usersDN}"
            else:
                userDN = f"{username},{self.usersDN}"
        else:
            userDN = username

        return self.connect(userDN, password)



class FairscapeConfig(BaseModel):
    host: Optional[str] = Field(default='0.0.0.0')
    port: Optional[str] = Field(default='8080')
    jwtSecret: str 
    passwordSalt: str 
    NAAN: Optional[str] = Field(default = '59852')
    url: Optional[str] = Field(default = "http://localhost:8080/")
    mongo: MongoConfig
    minio: MinioConfig
    redis: Optional[RedisConfig]
    ldap: LDAPConfig

    def CreateMongoClient(self):
        return self.mongo.CreateClient() 
    
    def CreateMinioClient(self):
        return self.minio.CreateClient()

    def RunServer(self):
        uvicorn.run(
            'fairscape_mds.app:app', 
            host=self.host, 
            port=int(self.port), 
            reload=True
            )


    def StartWorker(self):
        celery_app = Celery('compute', include="mds.compute.tasks")
        app.conf.broker_url = self.compute.redis.broker_url
        app.conf.result_backend = self.compute.redis.result_backend

        return celery_app



    def SetupMongo(self):
        ''' Initalize mongo database for fairscape server application
        '''
        mongo_client = self.CreateMongoClient()

        # create database if not created 
        mongo_db = mongo_client[self.mongo.db]
        
        # create identifier collection
        identifier_collection = mongo_db[self.mongo.identifier_collection]

        # create user collection
        mongo_collection = mongo_db[self.mongo.user_collection]

        # create text index for 
        # db.identifier.createIndex( { name: "text", description: "text" } )
        identifier_collection.create_index([('description', 'text'), ('name', 'text')], name="description_text")

        # create index for identifiers
        identifier_collection.create_index([("@id", ASCENDING)])

        # TODO create index for provenance properties

        # TODO recency of metadata publication
        #identifier_collection.create_index([])


    def SetupMinio(self):
        ''' Initalize minio buckets for fairscape server application
        '''

        minio_client = self.CreateMinioClient()

        try:
            # create default bucket
            minio_client.make_bucket(self.minio.default_bucket)

        except minio.S3Error as e:
            pass


        try:
            # create rocrate bucket
            minio_client.make_bucket(self.minio.rocrate_bucket)
        except minio.S3Error as e:
            pass


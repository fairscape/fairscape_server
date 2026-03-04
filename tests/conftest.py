import pytest
import pymongo
import boto3
from botocore.client import Config
from urllib.parse import quote_plus
from fairscape_mds.models.user import UserWriteModel


MONGO_CONNECTION_STRING = (
    f"mongodb://{quote_plus('mongotestaccess')}:{quote_plus('mongotestsecret')}"
    "@localhost:27017/?authSource=admin&retryWrites=true"
)
S3_ENDPOINT = "http://localhost:9000"
S3_BUCKET = "default"


@pytest.fixture(scope="session")
def s3_client():
    client = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id="miniotestadmin",
        aws_secret_access_key="miniotestsecret",
        config=Config(signature_version="s3v4"),
        aws_session_token=None,
        region_name="us-east-1",
    )
    try:
        client.create_bucket(Bucket=S3_BUCKET)
    except Exception:
        pass
    return client


@pytest.fixture(scope="session")
def mongo_client():
    return pymongo.MongoClient(MONGO_CONNECTION_STRING)


@pytest.fixture(autouse=True)
def clean_db(mongo_client, s3_client):
    """Clear all test collections and remove the test object from S3 before each test."""
    db = mongo_client.fairscape
    db.mds.delete_many({})
    db.users.delete_many({})
    db.rocrate.delete_many({})
    db["async"].delete_many({})

    try:
        s3_client.delete_object(Bucket=S3_BUCKET, Key="default/test/rocrates/Example.zip")
    except Exception:
        pass

    test_user = UserWriteModel.model_validate({
        "email": "test@example.org",
        "firstName": "John",
        "lastName": "Doe",
        "password": "test",
    })
    db.users.insert_one(test_user.model_dump(by_alias=True, mode="json"))


@pytest.fixture
def auth_headers(root_url):
    import httpx
    response = httpx.post(
        root_url + "/login",
        data={"username": "test@example.org", "password": "test"},
    )
    assert response.status_code == 200
    token = response.json()["access_token"]
    return {"Authorization": f"Bearer {token}"}

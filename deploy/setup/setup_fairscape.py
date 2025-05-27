import time
import minio
from dotenv import dotenv_values
from os import environ
import csv
import pymongo
from pydantic import BaseModel, Field
from typing import Optional, List, Literal, Dict, Any

# --- Pydantic Models (subset relevant to this script) ---
class UserCreateModel(BaseModel):
    email: str
    firstName: str
    lastName: str
    password: str

class UserWriteModel(UserCreateModel):
    metadataType: Literal['Person'] = Field(alias="@type", default="Person")
    session: Optional[str] = Field(default=None)
    groups: Optional[List[str]] = Field(default_factory=list)
    # Add other fields if needed by user_doc dump, but keeping minimal for this example
    # datasets: Optional[List[str]] = Field(default_factory=list)
    # software: Optional[List[str]] = Field(default_factory=list)
    # computations: Optional[List[str]] = Field(default_factory=list)
    # rocrates: Optional[List[str]] = Field(default_factory=list)


# --- Configuration ---
try:
    configValues = {
        **dotenv_values(dotenv_path=environ.get("SETUP_ENV_PATH", "./setup.env")),
        **environ
    }
except Exception as e:
    print(f"WARNING: Error loading .env file: {e}. Using environment variables only.")
    configValues = { **environ }

def get_config(key: str, default: Any = None) -> Any:
    value = configValues.get(key)
    return value if value is not None else default

# --- MinIO Setup ---
def setupMinio():
    print("INFO: Starting MinIO setup...")
    minio_host_config = get_config('FAIRSCAPE_MINIO_URI', 'minio')
    minio_port_config = get_config('FAIRSCAPE_MINIO_PORT', '9000')
    minio_secure = get_config('FAIRSCAPE_MINIO_SECURE', 'False').lower() == 'true'
    access_key = get_config('FAIRSCAPE_MINIO_ACCESS_KEY')
    secret_key = get_config('FAIRSCAPE_MINIO_SECRET_KEY')

    if not access_key or not secret_key:
        print("ERROR: MinIO access key or secret key not configured. Skipping MinIO setup.")
        return

    try:
        # Prepare host for MinIO client: remove scheme and path
        minio_host_processed = minio_host_config
        if minio_host_processed.startswith("http://"):
            minio_host_processed = minio_host_processed[len("http://"):]
        elif minio_host_processed.startswith("https://"):
            minio_host_processed = minio_host_processed[len("https://"):]
        
        # Remove any path component by splitting at the first '/'
        minio_host_processed = minio_host_processed.split('/', 1)[0]

        # Construct the endpoint for Minio client (host:port)
        minio_endpoint_for_client = f"{minio_host_processed}:{minio_port_config}"

        minioClient = minio.Minio(
            endpoint=minio_endpoint_for_client,
            access_key=access_key,
            secret_key=secret_key,
            secure=minio_secure,
        )
        minioClient.list_buckets() # Test connection
        print(f"INFO: Successfully connected to MinIO at {minio_endpoint_for_client}")

        defaultBucketName = get_config('FAIRSCAPE_MINIO_DEFAULT_BUCKET', 'fairscape-default')
        rocrateBucketName = get_config('FAIRSCAPE_MINIO_ROCRATE_BUCKET', 'fairscape-rocrate')

        for bucket_name in [defaultBucketName, rocrateBucketName]:
            if not minioClient.bucket_exists(bucket_name):
                minioClient.make_bucket(bucket_name)
                print(f"INFO: Bucket '{bucket_name}' created.")
            else:
                print(f"INFO: Bucket '{bucket_name}' already exists.")
        print("INFO: MinIO setup completed.")
    except Exception as e:
        print(f"ERROR: MinIO setup failed. Error: {e}")

# --- MongoDB Connection ---
def connectMongo() -> Optional[pymongo.database.Database]:
    print("INFO: Attempting to connect to MongoDB...")
    mongo_host = get_config('FAIRSCAPE_MONGO_HOST', 'localhost')
    mongo_port = get_config('FAIRSCAPE_MONGO_PORT', '27017')
    mongo_db_name = get_config('FAIRSCAPE_MONGO_DATABASE', 'fairscape')
    mongo_user = get_config('FAIRSCAPE_MONGO_ACCESS_KEY')
    mongo_pass = get_config('FAIRSCAPE_MONGO_SECRET_KEY')

    mongo_uri = f"mongodb://{mongo_host}:{mongo_port}/"
    if mongo_user and mongo_pass:
        mongo_uri = f"mongodb://{mongo_user}:{mongo_pass}@{mongo_host}:{mongo_port}/"

    try:
        mongoClient = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        mongoClient.admin.command('ping') # Verify connection
        print(f"INFO: Successfully connected to MongoDB at {mongo_host}:{mongo_port}")
        db = mongoClient[mongo_db_name]
        return db
    except Exception as e:
        print(f"ERROR: Failed to connect to MongoDB. Error: {e}")
        return None

# --- CSV Data Loading ---
def load_csv_data(filepath: str, expected_headers: List[str]) -> List[Dict[str, str]]:
    data_list: List[Dict[str, str]] = []
    print(f"INFO: Loading CSV data from {filepath}...")
    try:
        with open(filepath, mode='r', encoding='utf-8-sig') as csvfile:
            reader = csv.DictReader(csvfile)
            reader.fieldnames = [name.lower().strip() for name in reader.fieldnames or []]
            normalized_expected_headers = [name.lower().strip() for name in expected_headers]

            if not reader.fieldnames or not all(header in reader.fieldnames for header in normalized_expected_headers):
                print(f"ERROR: CSV file {filepath} headers mismatch. Expected: {normalized_expected_headers}, Got: {reader.fieldnames}")
                return []
            
            for row in reader:
                data_list.append({k.lower().strip(): str(v).strip() if v is not None else "" for k, v in row.items()})
        print(f"INFO: Successfully loaded {len(data_list)} rows from {filepath}")
        return data_list
    except FileNotFoundError:
        print(f"ERROR: CSV file not found: {filepath}")
    except Exception as e:
        print(f"ERROR: Error reading CSV file {filepath}. Error: {e}")
    return []

# --- MongoDB User and Group Setup ---
def setupMongoUsersAndGroups(db: pymongo.database.Database):
    print("INFO: Starting MongoDB user and group setup...")
    user_collection_name = get_config('FAIRSCAPE_MONGO_USER_COLLECTION', 'users')
    user_collection = db[user_collection_name]
    try:
        user_collection.create_index("email", unique=True, background=True)
    except Exception as e:
        print(f"WARNING: Could not ensure unique index on email for '{user_collection_name}'. Error: {e}")

    user_csv_path = get_config('USER_DATA_CSV_PATH', '/data/user_data.csv')
    group_csv_path = get_config('GROUP_DATA_CSV_PATH', '/data/group_data.csv')

    users_data = load_csv_data(user_csv_path, ['firstname', 'lastname', 'dn', 'email', 'password'])
    groups_raw_data = load_csv_data(group_csv_path, ['name', 'members'])

    processed_groups: Dict[str, List[str]] = {}
    if groups_raw_data:
        for group_entry in groups_raw_data:
            group_name = group_entry.get('name')
            members_str = group_entry.get('members')
            if group_name: # Process group even if members_str is empty or missing
                processed_groups[group_name] = [dn.strip() for dn in members_str.split(';') if dn.strip()] if members_str else []

    users_created, users_updated, users_failed = 0, 0, 0
    for user_record in users_data:
        try:
            user_dn = user_record.get('dn')
            user_email = user_record.get('email')
            if not user_email:
                print(f"WARNING: Skipping user record due to missing email: {user_record}")
                users_failed +=1
                continue

            user_groups = [name for name, dns in processed_groups.items() if user_dn and user_dn in dns]
            
            user_model_data = {
                "email": user_email,
                "firstName": user_record.get('firstname', ''), # Provide default empty string
                "lastName": user_record.get('lastname', ''),  # Provide default empty string
                "password": user_record.get('password', ''),# Provide default empty string
                "groups": user_groups,
            }
            
            user_write_instance = UserWriteModel.model_validate(user_model_data)
            user_doc = user_write_instance.model_dump(by_alias=True) # uses @type

            update_result = user_collection.update_one(
                {"email": user_email},
                {"$set": user_doc},
                upsert=True
            )
            if update_result.upserted_id: users_created += 1
            elif update_result.modified_count > 0: users_updated += 1
        except Exception as e:
            print(f"ERROR: Failed to process user {user_record.get('email', 'N/A')}. Error: {e}")
            users_failed += 1
            
    print(f"INFO: MongoDB user setup completed. Created: {users_created}, Updated/Matched: {users_updated}, Failed: {users_failed}")

# --- Main Execution ---
if __name__ == "__main__":
    print("INFO: Starting Fairscape setup script...")

    initial_delay_str = get_config('INITIAL_SETUP_DELAY', '0')
    try:
        initial_delay = int(initial_delay_str)
        if initial_delay > 0:
            print(f"INFO: Initial delay of {initial_delay} seconds...")
            time.sleep(initial_delay)
    except ValueError: # Simplified error handling for delay
        if initial_delay_str != '0': # Only warn if it's not the default '0' and invalid
            print(f"WARNING: Invalid INITIAL_SETUP_DELAY: '{initial_delay_str}'. No delay.")

    setupMinio()
    mongo_db_connection = connectMongo()

    if mongo_db_connection is not None: # Corrected truthiness check
        setupMongoUsersAndGroups(mongo_db_connection)
    else:
        print("ERROR: Skipping MongoDB user/group setup due to connection failure.")

    print("INFO: Fairscape setup script finished.")
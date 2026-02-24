# Upload & Fetch Workflow

This page walks through the most common FAIRSCAPE workflow end-to-end: authenticating, uploading an RO-Crate, polling until processing completes, and fetching the result by its ARK identifier.

!!! note "Prerequisites"
    You need a FAIRSCAPE account and a valid RO-Crate packaged as a `.zip` file. Use [fairscape-cli](https://github.com/fairscape/fairscape-cli) to create and package your crate locally.

---

## Step 1 — Authenticate

Exchange your credentials for a Bearer token. All write operations require this token.

=== "Python"

    ```python
    import requests

    BASE_URL = "https://fairscape.net/api"

    response = requests.post(
        f"{BASE_URL}/login",
        data={
            "username": "your_email@example.com",
            "password": "your_password"
        }
    )
    response.raise_for_status()

    token = response.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}
    print("Authenticated. Token acquired.")
    ```

=== "curl"

    ```shell
    curl -X POST "https://fairscape.net/api/login" \
         -d "username=your_email@example.com&password=your_password"
    ```

**Response:**
```json
{
  "access_token": "eyJ...",
  "token_type": "bearer"
}
```

---

## Step 2 — Upload the RO-Crate

Upload your RO-Crate zip file. The server processes it asynchronously and returns a `submissionUUID` you can use to track progress.

=== "Python"

    ```python
    crate_path = "/path/to/my-rocrate.zip"

    with open(crate_path, "rb") as f:
        upload_response = requests.post(
            f"{BASE_URL}/rocrate/upload-async",
            files={"crate": ("my-rocrate.zip", f, "application/zip")},
            headers=headers
        )

    upload_response.raise_for_status()
    upload_job = upload_response.json()

    submission_uuid = upload_job["guid"]
    print(f"Upload started. Submission UUID: {submission_uuid}")
    ```

=== "curl"

    ```shell
    curl -X POST "https://fairscape.net/api/rocrate/upload-async" \
         -H "Authorization: Bearer <your_token>" \
         -F "crate=@/path/to/my-rocrate.zip;type=application/zip"
    ```

**Response:**
```json
{
  "guid": "3f2a1c84-...",
  "status": "PENDING",
  "time_created": "2024-01-15T10:30:00Z"
}
```

---

## Step 3 — Poll Upload Status

The server processes the zip file in the background (extracting metadata, minting identifiers, storing files). Poll the status endpoint until the job reaches `COMPLETE`.

=== "Python"

    ```python
    import time

    status_url = f"{BASE_URL}/rocrate/upload/status/{submission_uuid}"

    while True:
        status_response = requests.get(status_url, headers=headers)
        status_response.raise_for_status()
        job = status_response.json()

        print(f"Status: {job['status']}")

        if job["status"] == "COMPLETE":
            rocrate_id = job["rocrate_id"]  # the ARK identifier
            print(f"Processing complete. RO-Crate ARK: {rocrate_id}")
            break
        elif job["status"] == "FAILED":
            raise RuntimeError(f"Upload failed: {job.get('error')}")

        time.sleep(5)
    ```

=== "curl"

    ```shell
    curl -X GET "https://fairscape.net/api/rocrate/upload/status/<submissionUUID>" \
         -H "Authorization: Bearer <your_token>"
    ```

**Response (while processing):**
```json
{
  "guid": "3f2a1c84-...",
  "status": "PROCESSING",
  "rocrate_id": null
}
```

**Response (when complete):**
```json
{
  "guid": "3f2a1c84-...",
  "status": "COMPLETE",
  "rocrate_id": "ark:59853/my-rocrate-2024"
}
```

---

## Step 4 — Fetch by ARK Identifier

Once the crate is processed, fetch it by its ARK identifier using the universal resolver. This endpoint works for **any** FAIRSCAPE object (ROCrate, Dataset, Software, etc.).

=== "Python"

    ```python
    resolve_response = requests.get(
        f"{BASE_URL}/{rocrate_id}",
        headers={"Accept": "application/json"}
    )
    resolve_response.raise_for_status()

    metadata = resolve_response.json()
    print(f"Name: {metadata['metadata']['name']}")
    print(f"Description: {metadata['metadata']['description']}")
    ```

=== "curl"

    ```shell
    curl -X GET "https://fairscape.net/api/ark:59853/my-rocrate-2024" \
         -H "Accept: application/json"
    ```

**Response:**
```json
{
  "@id": "ark:59853/my-rocrate-2024",
  "@type": "ROCrate",
  "metadata": {
    "@context": {
      "@vocab": "https://schema.org/",
      "EVI": "https://w3id.org/EVI#"
    },
    "name": "My Research RO-Crate",
    "description": "...",
    "keywords": ["proteomics", "b2ai"],
    "datePublished": "2024-01-15"
  }
}
```

---

## Step 5 — Fetch ROCrate Metadata Directly

You can also use the ROCrate-specific endpoint, which returns the full RO-Crate JSON-LD structure. It also supports Croissant format via content negotiation.

=== "Python"

    ```python
    # Standard RO-Crate JSON
    rc_response = requests.get(
        f"{BASE_URL}/rocrate/{rocrate_id}",
        headers={"Accept": "application/json"}
    )
    rocrate_data = rc_response.json()

    # Croissant format (for ML Commons compatibility)
    croissant_response = requests.get(
        f"{BASE_URL}/rocrate/{rocrate_id}",
        headers={"Accept": "application/vnd.mlcommons-croissant+json"}
    )
    croissant_data = croissant_response.json()
    ```

=== "curl"

    ```shell
    # Standard JSON
    curl -X GET "https://fairscape.net/api/rocrate/ark:59853/my-rocrate-2024" \
         -H "Accept: application/json"

    # Croissant format
    curl -X GET "https://fairscape.net/api/rocrate/ark:59853/my-rocrate-2024" \
         -H "Accept: application/vnd.mlcommons-croissant+json"
    ```

---

## Complete Python Script

```python
import requests
import time

BASE_URL = "https://fairscape.net/api"
CRATE_PATH = "/path/to/my-rocrate.zip"

# 1. Authenticate
login = requests.post(
    f"{BASE_URL}/login",
    data={"username": "you@example.com", "password": "your_password"}
)
login.raise_for_status()
token = login.json()["access_token"]
headers = {"Authorization": f"Bearer {token}"}

# 2. Upload
with open(CRATE_PATH, "rb") as f:
    upload = requests.post(
        f"{BASE_URL}/rocrate/upload-async",
        files={"crate": ("crate.zip", f, "application/zip")},
        headers=headers
    )
upload.raise_for_status()
submission_uuid = upload.json()["guid"]

# 3. Poll
while True:
    job = requests.get(
        f"{BASE_URL}/rocrate/upload/status/{submission_uuid}",
        headers=headers
    ).json()
    if job["status"] == "COMPLETE":
        rocrate_id = job["rocrate_id"]
        break
    elif job["status"] == "FAILED":
        raise RuntimeError(f"Failed: {job.get('error')}")
    time.sleep(5)

# 4. Fetch by ARK
result = requests.get(f"{BASE_URL}/{rocrate_id}").json()
print(f"RO-Crate '{result['metadata']['name']}' is live at: {rocrate_id}")
```

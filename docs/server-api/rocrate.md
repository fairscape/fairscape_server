# ROCrate Endpoints

These endpoints manage Research Object Crates (RO-Crates) on the FAIRSCAPE server. All URLs are relative to `https://fairscape.net/api`.

For a step-by-step guide to uploading and retrieving a crate, see [Upload & Fetch Workflow](workflow-upload-fetch.md).

---

## Endpoint Summary

| Method | Path | Auth Required | Description |
|--------|------|:-------------:|-------------|
| `POST` | `/rocrate/upload-async` | ✓ | Upload a zip file asynchronously |
| `POST` | `/rocrate/metadata` | ✓ | Mint a metadata-only ROCrate (no file) |
| `GET` | `/rocrate` | ✓ | List all ROCrates for the current user |
| `GET` | `/rocrate/upload/status/{submissionUUID}` | ✓ | Poll async upload status |
| `GET` | `/rocrate/ark:{NAAN}/{postfix}` | – | Get ROCrate metadata (public) |
| `GET` | `/rocrate/summary/ark:{NAAN}/{postfix}` | – | Get paginated content summary |
| `GET` | `/rocrate/download/ark:{NAAN}/{postfix}` | ✓ | Download the ROCrate as a zip |
| `GET` | `/rocrate/ai-ready-score/ark:{NAAN}/{postfix}` | – | Get or initiate AI-Ready Score |
| `GET` | `/rocrate/ai-ready-score/status/{task_id}` | – | Check AI scoring task status |
| `POST` | `/rocrate/ai-ready-score/ark:{NAAN}/{postfix}/rescore` | – | Trigger a rescore |

---

## `POST /rocrate/upload-async`

Upload a RO-Crate zip file. Processing happens in the background — use the returned `guid` to poll for completion.

**Request:** `multipart/form-data`

| Field | Type | Description |
|-------|------|-------------|
| `crate` | file | The RO-Crate `.zip` file |

=== "Python"

    ```python
    with open("my-rocrate.zip", "rb") as f:
        response = requests.post(
            f"{BASE_URL}/rocrate/upload-async",
            files={"crate": ("my-rocrate.zip", f, "application/zip")},
            headers={"Authorization": f"Bearer {token}"}
        )
    job = response.json()
    submission_uuid = job["guid"]
    ```

=== "curl"

    ```shell
    curl -X POST "https://fairscape.net/api/rocrate/upload-async" \
         -H "Authorization: Bearer <token>" \
         -F "crate=@my-rocrate.zip;type=application/zip"
    ```

**Response (202):**
```json
{ "guid": "3f2a1c84-...", "status": "PENDING" }
```

---

## `POST /rocrate/metadata`

Register a ROCrate record from JSON-LD metadata without uploading any file content. Useful when files are hosted externally or access is embargoed.

**Query Parameters:**

| Parameter | Required | Description |
|-----------|----------|-------------|
| `baseDatasetArk` | No | ARK of an existing dataset to associate this crate with |

**Request body:** ROCrate V1.2 JSON-LD

=== "Python"

    ```python
    import json

    metadata = {
        "@context": "https://w3id.org/ro/crate/1.1/context",
        "@graph": [...]
    }

    response = requests.post(
        f"{BASE_URL}/rocrate/metadata",
        json=metadata,
        headers={"Authorization": f"Bearer {token}"}
    )
    print(response.json())  # includes the minted @id
    ```

=== "curl"

    ```shell
    curl -X POST "https://fairscape.net/api/rocrate/metadata" \
         -H "Authorization: Bearer <token>" \
         -H "Content-Type: application/json" \
         -d @rocrate-metadata.json
    ```

**Response (201):** The minted identifier record.

---

## `GET /rocrate`

List all ROCrates owned by or accessible to the authenticated user.

=== "Python"

    ```python
    response = requests.get(
        f"{BASE_URL}/rocrate",
        headers={"Authorization": f"Bearer {token}"}
    )
    crates = response.json()
    for crate in crates:
        print(crate["@id"], crate.get("name"))
    ```

=== "curl"

    ```shell
    curl -X GET "https://fairscape.net/api/rocrate" \
         -H "Authorization: Bearer <token>"
    ```

**Response (200):** Array of ROCrate objects.

---

## `GET /rocrate/upload/status/{submissionUUID}`

Poll the status of an async upload job.

**Path Parameters:**

| Parameter | Description |
|-----------|-------------|
| `submissionUUID` | UUID returned by `upload-async` |

**Status values:** `PENDING` → `PROCESSING` → `COMPLETE` or `FAILED`

=== "Python"

    ```python
    status = requests.get(
        f"{BASE_URL}/rocrate/upload/status/{submission_uuid}",
        headers={"Authorization": f"Bearer {token}"}
    ).json()
    print(status["status"], status.get("rocrate_id"))
    ```

=== "curl"

    ```shell
    curl "https://fairscape.net/api/rocrate/upload/status/<uuid>" \
         -H "Authorization: Bearer <token>"
    ```

**Response (200):**
```json
{
  "guid": "3f2a1c84-...",
  "status": "COMPLETE",
  "rocrate_id": "ark:59853/my-rocrate-2024"
}
```

---

## `GET /rocrate/ark:{NAAN}/{postfix}`

Retrieve the full metadata for an RO-Crate. This is a public endpoint — no authentication required.

Supports **content negotiation** via the `Accept` header:

| Accept Header | Response Format |
|--------------|-----------------|
| `application/json` (default) | Raw RO-Crate JSON-LD |
| `application/vnd.mlcommons-croissant+json` | Croissant JSON-LD (ML Commons) |

=== "Python"

    ```python
    # Standard RO-Crate JSON
    response = requests.get(
        "https://fairscape.net/api/rocrate/ark:59853/my-rocrate-2024"
    )

    # Croissant format
    response = requests.get(
        "https://fairscape.net/api/rocrate/ark:59853/my-rocrate-2024",
        headers={"Accept": "application/vnd.mlcommons-croissant+json"}
    )
    ```

=== "curl"

    ```shell
    # Standard JSON
    curl "https://fairscape.net/api/rocrate/ark:59853/my-rocrate-2024"

    # Croissant format
    curl "https://fairscape.net/api/rocrate/ark:59853/my-rocrate-2024" \
         -H "Accept: application/vnd.mlcommons-croissant+json"
    ```

---

## `GET /rocrate/summary/ark:{NAAN}/{postfix}`

Return a lightweight summary of a ROCrate's contents, grouped by type, with counts. Supports pagination.

**Query Parameters:**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `limit` | `10` | Max items per category (1–100) |
| `offset` | `0` | Starting index for pagination |

**Response shape:**
```json
{
  "datasets": [...],
  "software": [...],
  "computations": [...],
  "schemas": [...],
  "mlModels": [...],
  "rocrates": [...],
  "other": [...],
  "counts": {
    "datasets": 42,
    "software": 3
  }
}
```

=== "curl"

    ```shell
    curl "https://fairscape.net/api/rocrate/summary/ark:59853/my-rocrate-2024?limit=20&offset=0"
    ```

---

## `GET /rocrate/download/ark:{NAAN}/{postfix}`

Download the original RO-Crate as a zip archive. Returns a `application/zip` stream.

=== "Python"

    ```python
    response = requests.get(
        f"{BASE_URL}/rocrate/download/ark:59853/my-rocrate-2024",
        headers={"Authorization": f"Bearer {token}"},
        stream=True
    )
    with open("downloaded-rocrate.zip", "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    ```

=== "curl"

    ```shell
    curl -o downloaded-rocrate.zip \
         -H "Authorization: Bearer <token>" \
         "https://fairscape.net/api/rocrate/download/ark:59853/my-rocrate-2024"
    ```

---

## AI-Ready Score Endpoints

FAIRSCAPE can automatically score an RO-Crate for AI-readiness. Scoring runs asynchronously.

### `GET /rocrate/ai-ready-score/ark:{NAAN}/{postfix}`

If a score already exists, returns it. If not, initiates scoring and returns a task reference (`202 Accepted`).

=== "curl"

    ```shell
    curl "https://fairscape.net/api/rocrate/ai-ready-score/ark:59853/my-rocrate-2024"
    ```

**Response (200 — score exists):**
```json
{ "@id": "ark:59853/my-rocrate-2024-ai-ready-score", "@type": "evi:AIReadyScore", ... }
```

**Response (202 — scoring initiated):**
```json
{
  "message": "AI-Ready scoring initiated",
  "task_id": "abc-123",
  "status_endpoint": "/rocrate/ai-ready-score/status/abc-123"
}
```

### `GET /rocrate/ai-ready-score/status/{task_id}`

Check the status of an ongoing AI-Ready scoring task.

=== "curl"

    ```shell
    curl "https://fairscape.net/api/rocrate/ai-ready-score/status/abc-123"
    ```

### `POST /rocrate/ai-ready-score/ark:{NAAN}/{postfix}/rescore`

Delete an existing score and trigger a fresh rescore.

=== "curl"

    ```shell
    curl -X POST \
         "https://fairscape.net/api/rocrate/ai-ready-score/ark:59853/my-rocrate-2024/rescore"
    ```

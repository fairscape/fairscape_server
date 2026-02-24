# Dataset Endpoints

Endpoints for creating, retrieving, downloading, and deleting individual dataset records. Datasets can be uploaded with their file content or registered with only metadata (for external or embargoed data).

All URLs are relative to `https://fairscape.net/api`.

---

## Endpoint Summary

| Method | Path | Auth Required | Description |
|--------|------|:-------------:|-------------|
| `POST` | `/dataset` | ✓ | Create a dataset record (with optional file upload) |
| `GET` | `/dataset/ark:{naan}/{postfix}` | – | Get dataset metadata |
| `GET` | `/dataset/download/ark:{naan}/{postfix}` | ✓ | Download dataset file content |
| `DELETE` | `/dataset/ark:{NAAN}/{postfix}` | ✓ | Delete a dataset |

---

## `POST /dataset`

Create a new dataset record. Accepts `multipart/form-data` with a JSON metadata field and an optional file.

**Form Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `datasetMetadata` | JSON string | Dataset metadata (see schema below) |
| `uploadFile` | file | *(Optional)* The actual dataset file |

**Metadata schema (key fields):**

```json
{
  "@id": "ark:59853/my-dataset-2024",
  "@type": "Dataset",
  "name": "AP-MS Embeddings",
  "description": "Protein interaction embeddings",
  "author": "Krogan Lab",
  "version": "1.0",
  "datePublished": "2024-01-15",
  "keywords": ["proteomics"],
  "dataFormat": "CSV"
}
```

=== "Python"

    ```python
    import json

    metadata = {
        "@id": "ark:59853/apms-embeddings-2024",
        "@type": "Dataset",
        "name": "AP-MS Embeddings",
        "description": "APMS embeddings for each protein",
        "author": "Krogan Lab",
        "version": "1.0",
        "datePublished": "2024-01-15",
        "keywords": ["proteomics", "b2ai"],
        "dataFormat": "CSV"
    }

    with open("embeddings.csv", "rb") as f:
        response = requests.post(
            f"{BASE_URL}/dataset",
            data={"datasetMetadata": json.dumps(metadata)},
            files={"uploadFile": ("embeddings.csv", f, "text/csv")},
            headers={"Authorization": f"Bearer {token}"}
        )
    print(response.json())
    ```

=== "curl"

    ```shell
    curl -X POST "https://fairscape.net/api/dataset" \
         -H "Authorization: Bearer <token>" \
         -F 'datasetMetadata={"@id":"ark:59853/apms-embeddings-2024","name":"AP-MS Embeddings","@type":"Dataset",...}' \
         -F "uploadFile=@embeddings.csv;type=text/csv"
    ```

---

## `GET /dataset/ark:{naan}/{postfix}`

Retrieve metadata for a dataset. Public — no authentication required.

=== "Python"

    ```python
    response = requests.get(
        f"{BASE_URL}/dataset/ark:59853/apms-embeddings-2024"
    )
    print(response.json())
    ```

=== "curl"

    ```shell
    curl "https://fairscape.net/api/dataset/ark:59853/apms-embeddings-2024"
    ```

!!! note
    You can also use the universal ARK resolver (`GET /ark:{naan}/{postfix}`) to retrieve dataset metadata — the result is the same.

---

## `GET /dataset/download/ark:{naan}/{postfix}`

Download the file content for a dataset. The response `Content-Type` is inferred from the filename.

=== "Python"

    ```python
    response = requests.get(
        f"{BASE_URL}/dataset/download/ark:59853/apms-embeddings-2024",
        headers={"Authorization": f"Bearer {token}"},
        stream=True
    )
    with open("downloaded-embeddings.csv", "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    ```

=== "curl"

    ```shell
    curl -o embeddings.csv \
         -H "Authorization: Bearer <token>" \
         "https://fairscape.net/api/dataset/download/ark:59853/apms-embeddings-2024"
    ```

---

## `DELETE /dataset/ark:{NAAN}/{postfix}`

Delete a dataset record and its associated file content.

=== "Python"

    ```python
    response = requests.delete(
        f"{BASE_URL}/dataset/ark:59853/apms-embeddings-2024",
        headers={"Authorization": f"Bearer {token}"}
    )
    print(response.status_code)
    ```

=== "curl"

    ```shell
    curl -X DELETE "https://fairscape.net/api/dataset/ark:59853/apms-embeddings-2024" \
         -H "Authorization: Bearer <token>"
    ```

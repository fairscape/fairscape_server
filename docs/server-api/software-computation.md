# Software & Computation Endpoints

Endpoints for registering software artifacts and computation records. These objects form the provenance chain linking input datasets, code, and output results in an EVI Evidence Graph.

All URLs are relative to `https://fairscape.net/api`.

---

## Software

### Endpoint Summary

| Method | Path | Auth Required | Description |
|--------|------|:-------------:|-------------|
| `POST` | `/software` | ✓ | Register a software record |
| `GET` | `/software/ark:{NAAN}/{postfix}` | – | Get software metadata |
| `DELETE` | `/software/ark:{NAAN}/{postfix}` | ✓ | Delete a software record |

### `POST /software`

Register a software artifact. The request body is a JSON object conforming to the Software schema.

**Key metadata fields:**

```json
{
  "@id": "ark:59853/calibrate-pairwise-2024",
  "@type": "Software",
  "name": "calibrate pairwise distance",
  "description": "Script to calibrate pairwise protein distances",
  "author": "Qin, Y.",
  "version": "1.0",
  "dateModified": "2024-01-15",
  "fileFormat": "py",
  "keywords": ["proteomics", "b2ai"]
}
```

=== "Python"

    ```python
    software = {
        "@id": "ark:59853/calibrate-pairwise-2024",
        "@type": "Software",
        "name": "calibrate pairwise distance",
        "description": "Script to calibrate pairwise protein distances",
        "author": "Qin, Y.",
        "version": "1.0",
        "dateModified": "2024-01-15",
        "fileFormat": "py",
        "keywords": ["proteomics", "b2ai"]
    }

    response = requests.post(
        f"{BASE_URL}/software",
        json=software,
        headers={"Authorization": f"Bearer {token}"}
    )
    print(response.json())
    ```

=== "curl"

    ```shell
    curl -X POST "https://fairscape.net/api/software" \
         -H "Authorization: Bearer <token>" \
         -H "Content-Type: application/json" \
         -d '{"@id":"ark:59853/calibrate-pairwise-2024","@type":"Software","name":"calibrate pairwise distance",...}'
    ```

### `GET /software/ark:{NAAN}/{postfix}`

Retrieve software metadata. Public endpoint.

=== "curl"

    ```shell
    curl "https://fairscape.net/api/software/ark:59853/calibrate-pairwise-2024"
    ```

### `DELETE /software/ark:{NAAN}/{postfix}`

Delete a software record.

=== "curl"

    ```shell
    curl -X DELETE "https://fairscape.net/api/software/ark:59853/calibrate-pairwise-2024" \
         -H "Authorization: Bearer <token>"
    ```

---

## Computation

### Endpoint Summary

| Method | Path | Auth Required | Description |
|--------|------|:-------------:|-------------|
| `POST` | `/computation` | ✓ | Register a computation record |
| `GET` | `/computation/ark:{NAAN}/{postfix}` | – | Get computation metadata |
| `DELETE` | `/computation/ark:{NAAN}/{postfix}` | ✓ | Delete a computation record |

### `POST /computation`

Register a computation that links input datasets, software, and output datasets to form a provenance record.

**Key metadata fields:**

```json
{
  "@id": "ark:59853/apms-calibration-run-2024",
  "@type": "Computation",
  "name": "AP-MS Calibration Run",
  "description": "Calibration of pairwise protein distances",
  "runBy": "Qin, Y.",
  "dateCreated": "2024-01-15",
  "keywords": ["proteomics", "b2ai"],
  "usedSoftware": [{"@id": "ark:59853/calibrate-pairwise-2024"}],
  "usedDataset": [{"@id": "ark:59853/apms-raw-2024"}],
  "generated": [{"@id": "ark:59853/apms-embeddings-2024"}]
}
```

=== "Python"

    ```python
    computation = {
        "@id": "ark:59853/apms-calibration-run-2024",
        "@type": "Computation",
        "name": "AP-MS Calibration Run",
        "description": "Calibration of pairwise protein distances",
        "runBy": "Qin, Y.",
        "dateCreated": "2024-01-15",
        "keywords": ["proteomics", "b2ai"],
        "usedSoftware": [{"@id": "ark:59853/calibrate-pairwise-2024"}],
        "usedDataset": [{"@id": "ark:59853/apms-raw-2024"}],
        "generated": [{"@id": "ark:59853/apms-embeddings-2024"}]
    }

    response = requests.post(
        f"{BASE_URL}/computation",
        json=computation,
        headers={"Authorization": f"Bearer {token}"}
    )
    print(response.json())
    ```

=== "curl"

    ```shell
    curl -X POST "https://fairscape.net/api/computation" \
         -H "Authorization: Bearer <token>" \
         -H "Content-Type: application/json" \
         -d '{"@id":"ark:59853/apms-calibration-run-2024","@type":"Computation",...}'
    ```

### `GET /computation/ark:{NAAN}/{postfix}`

Retrieve computation metadata. Public endpoint.

=== "curl"

    ```shell
    curl "https://fairscape.net/api/computation/ark:59853/apms-calibration-run-2024"
    ```

### `DELETE /computation/ark:{NAAN}/{postfix}`

Delete a computation record.

=== "curl"

    ```shell
    curl -X DELETE "https://fairscape.net/api/computation/ark:59853/apms-calibration-run-2024" \
         -H "Authorization: Bearer <token>"
    ```

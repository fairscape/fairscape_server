# Evidence Graph Endpoints

Evidence Graphs (EVI Graphs) capture the full provenance lineage of a research object — linking datasets, software, computations, and their relationships into a queryable graph structure. Building an evidence graph assembles this provenance automatically from the registered relationships.

All URLs are relative to `https://fairscape.net/api`.

---

## Endpoint Summary

| Method | Path | Auth Required | Description |
|--------|------|:-------------:|-------------|
| `POST` | `/evidencegraph` | ✓ | Create an evidence graph record |
| `GET` | `/evidencegraph` | – | List all evidence graphs |
| `GET` | `/evidencegraph/ark:{NAAN}/{postfix}` | – | Get evidence graph by ARK |
| `GET` | `/evidencegraph/query/ark:{NAAN}/{postfix}` | ✓ | Query evidence graph (authenticated) |
| `DELETE` | `/evidencegraph/ark:{NAAN}/{postfix}` | ✓ | Delete an evidence graph |
| `POST` | `/evidencegraph/build/ark:{NAAN}/{postfix}` | ✓ | Initiate async build/rebuild |
| `GET` | `/evidencegraph/build/status/{task_id}` | – | Check build task status |

---

## `POST /evidencegraph`

Create an evidence graph record manually.

=== "Python"

    ```python
    evi_graph = {
        "@id": "ark:59853/apms-evidence-graph-2024",
        "@type": "EvidenceGraph",
        "name": "AP-MS Workflow Evidence Graph",
        "description": "Provenance graph for the AP-MS protein embedding workflow",
        "rootNode": {"@id": "ark:59853/apms-embeddings-2024"}
    }

    response = requests.post(
        f"{BASE_URL}/evidencegraph",
        json=evi_graph,
        headers={"Authorization": f"Bearer {token}"}
    )
    print(response.json())
    ```

=== "curl"

    ```shell
    curl -X POST "https://fairscape.net/api/evidencegraph" \
         -H "Authorization: Bearer <token>" \
         -H "Content-Type: application/json" \
         -d '{"@id":"ark:59853/apms-evidence-graph-2024","@type":"EvidenceGraph",...}'
    ```

---

## `GET /evidencegraph`

List all evidence graphs in the system. Public endpoint.

=== "curl"

    ```shell
    curl "https://fairscape.net/api/evidencegraph"
    ```

---

## `GET /evidencegraph/ark:{NAAN}/{postfix}`

Retrieve an evidence graph by its ARK identifier. Public endpoint.

=== "Python"

    ```python
    response = requests.get(
        f"{BASE_URL}/evidencegraph/ark:59853/apms-evidence-graph-2024"
    )
    print(response.json())
    ```

=== "curl"

    ```shell
    curl "https://fairscape.net/api/evidencegraph/ark:59853/apms-evidence-graph-2024"
    ```

---

## `POST /evidencegraph/build/ark:{NAAN}/{postfix}`

Initiate an asynchronous build (or rebuild) of an evidence graph for the object at the given ARK. The server traverses provenance relationships and assembles the graph in the background.

Returns a `task_id` you can use to poll for completion.

=== "Python"

    ```python
    import time

    # Initiate build
    build_response = requests.post(
        f"{BASE_URL}/evidencegraph/build/ark:59853/apms-embeddings-2024",
        headers={"Authorization": f"Bearer {token}"}
    )
    task_id = build_response.json()["task_id"]
    print(f"Build started. Task ID: {task_id}")

    # Poll for completion
    while True:
        status = requests.get(
            f"{BASE_URL}/evidencegraph/build/status/{task_id}"
        ).json()
        print(f"Status: {status['status']}")
        if status["status"] in ("COMPLETE", "FAILED"):
            break
        time.sleep(5)
    ```

=== "curl"

    ```shell
    # Initiate build
    curl -X POST "https://fairscape.net/api/evidencegraph/build/ark:59853/apms-embeddings-2024" \
         -H "Authorization: Bearer <token>"

    # Check status
    curl "https://fairscape.net/api/evidencegraph/build/status/<task_id>"
    ```

**Response (202):**
```json
{
  "message": "EvidenceGraph build process initiated.",
  "task_id": "abc-123",
  "status_endpoint": "/evidencegraph/build/status/abc-123"
}
```

---

## `GET /evidencegraph/build/status/{task_id}`

Check the status of an evidence graph build task.

=== "curl"

    ```shell
    curl "https://fairscape.net/api/evidencegraph/build/status/abc-123"
    ```

**Response:**
```json
{
  "guid": "abc-123",
  "status": "COMPLETE",
  "naan": "59853",
  "postfix": "apms-embeddings-2024"
}
```

---

## `DELETE /evidencegraph/ark:{NAAN}/{postfix}`

Delete an evidence graph record.

=== "curl"

    ```shell
    curl -X DELETE "https://fairscape.net/api/evidencegraph/ark:59853/apms-evidence-graph-2024" \
         -H "Authorization: Bearer <token>"
    ```

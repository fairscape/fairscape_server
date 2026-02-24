# Search Endpoints

FAIRSCAPE provides two search modes: a fast keyword search over indexed metadata fields, and a semantic search powered by vector embeddings for natural-language queries.

All URLs are relative to `https://fairscape.net/api`.

---

## Endpoint Summary

| Method | Path | Auth Required | Description |
|--------|------|:-------------:|-------------|
| `GET` | `/search/basic?query=…` | – | Keyword search across all metadata |
| `GET` | `/search/semantic?query=…` | – | Semantic / natural-language search |

Both endpoints are **public** — no authentication required.

---

## `GET /search/basic`

Search all registered objects using keyword matching against metadata fields (name, description, keywords, etc.).

**Query Parameters:**

| Parameter | Required | Description |
|-----------|----------|-------------|
| `query` | ✓ | The search string |

=== "Python"

    ```python
    response = requests.get(
        f"{BASE_URL}/search/basic",
        params={"query": "proteomics embeddings"}
    )
    results = response.json()

    print(f"Found {results['total_results']} results")
    for item in results["results"]:
        print(f"  {item['@id']} — {item['name']}")
    ```

=== "curl"

    ```shell
    curl "https://fairscape.net/api/search/basic?query=proteomics+embeddings"
    ```

**Response (200):**
```json
{
  "query": "proteomics embeddings",
  "total_results": 3,
  "results": [
    {
      "@id": "ark:59853/apms-embeddings-2024",
      "type": "Dataset",
      "name": "AP-MS Embeddings",
      "description": "APMS embeddings for each protein",
      "keywords": ["proteomics", "b2ai"],
      "score": 0.92
    }
  ],
  "time_taken_ms": 12.4
}
```

---

## `GET /search/semantic`

Search using natural-language queries. The server forwards the query to a vector embedding service and returns semantically similar results ranked by similarity score.

!!! note
    Semantic search requires the FAIRSCAPE vector search service to be running. If unavailable, the endpoint returns a `503` error.

**Query Parameters:**

| Parameter | Required | Description |
|-----------|----------|-------------|
| `query` | ✓ | Natural-language search query |

=== "Python"

    ```python
    response = requests.get(
        f"{BASE_URL}/search/semantic",
        params={"query": "protein interaction network from mass spectrometry"}
    )
    results = response.json()

    for item in results["results"]:
        print(f"  [{item['score']:.2f}] {item['@id']} — {item['name']}")
    ```

=== "curl"

    ```shell
    curl "https://fairscape.net/api/search/semantic?query=protein+interaction+network+from+mass+spectrometry"
    ```

**Response (200):**
```json
{
  "query": "protein interaction network from mass spectrometry",
  "total_results": 5,
  "results": [
    {
      "@id": "ark:59853/apms-embeddings-2024",
      "type": null,
      "name": "AP-MS Embeddings",
      "description": "Protein interaction embeddings from AP-MS",
      "keywords": ["proteomics"],
      "score": 0.87
    }
  ],
  "time_taken_ms": 145.2
}
```

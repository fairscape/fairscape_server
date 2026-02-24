# Identifier Resolution

The ARK resolver is the universal entry point for fetching **any** FAIRSCAPE object by its identifier—RO-Crates, datasets, software, computations, and more. You do not need to know the object type in advance.

All URLs are relative to `https://fairscape.net/api`.

---

## Endpoint Summary

| Method | Path | Auth Required | Description |
|--------|------|:-------------:|-------------|
| `GET` | `/ark:{NAAN}/{postfix}` | – | Resolve and retrieve metadata for any identifier |
| `PUT` | `/ark:{NAAN}/{postfix}` | ✓ | Update metadata for an identifier you own |
| `DELETE` | `/ark:{NAAN}/{postfix}` | ✓ | Delete an identifier (and optionally its contents) |

---

## `GET /ark:{NAAN}/{postfix}`

Retrieve metadata for any registered object. This is a **public endpoint** — no authentication required.

**Path Parameters:**

| Parameter | Description |
|-----------|-------------|
| `NAAN` | Name Assigning Authority Number (e.g., `59853`) |
| `postfix` | Unique identifier postfix (e.g., `my-dataset-2024`) |

**Content Negotiation** — set the `Accept` header to control output format:

| Accept Header | Format |
|--------------|--------|
| `application/json` (default) | JSON-LD metadata |
| `text/turtle` | Turtle RDF |
| `application/rdf+xml` | RDF/XML |

=== "Python"

    ```python
    BASE_URL = "https://fairscape.net/api"

    # JSON (default)
    response = requests.get(f"{BASE_URL}/ark:59853/my-dataset-2024")
    metadata = response.json()
    print(metadata["metadata"]["name"])

    # Turtle RDF
    turtle_response = requests.get(
        f"{BASE_URL}/ark:59853/my-dataset-2024",
        headers={"Accept": "text/turtle"}
    )
    print(turtle_response.text)
    ```

=== "curl"

    ```shell
    # JSON
    curl "https://fairscape.net/api/ark:59853/my-dataset-2024"

    # Turtle RDF
    curl "https://fairscape.net/api/ark:59853/my-dataset-2024" \
         -H "Accept: text/turtle"

    # RDF/XML
    curl "https://fairscape.net/api/ark:59853/my-dataset-2024" \
         -H "Accept: application/rdf+xml"
    ```

**Response (200 — JSON):**
```json
{
  "@id": "ark:59853/my-dataset-2024",
  "@type": "Dataset",
  "metadata": {
    "@context": {
      "@vocab": "https://schema.org/",
      "EVI": "https://w3id.org/EVI#"
    },
    "@id": "ark:59853/my-dataset-2024",
    "name": "AP-MS Embeddings",
    "description": "Protein interaction embeddings from AP-MS data",
    "keywords": ["proteomics", "b2ai"],
    "datePublished": "2024-01-15"
  }
}
```

!!! note
    The returned JSON includes both a top-level `@id`/`@type` summary and a `metadata` object containing the full JSON-LD document.

---

## `PUT /ark:{NAAN}/{postfix}`

Update the metadata of an existing identifier. You must own the object (authenticated).

**Request body:** Updated metadata object (same shape as the `metadata` field returned by GET)

=== "Python"

    ```python
    updated_metadata = {
        "@id": "ark:59853/my-dataset-2024",
        "name": "AP-MS Embeddings (Updated)",
        "description": "Updated description with additional details.",
        "keywords": ["proteomics", "b2ai", "cm4ai"]
    }

    response = requests.put(
        f"{BASE_URL}/ark:59853/my-dataset-2024",
        json=updated_metadata,
        headers={"Authorization": f"Bearer {token}"}
    )
    print(response.status_code)
    ```

=== "curl"

    ```shell
    curl -X PUT "https://fairscape.net/api/ark:59853/my-dataset-2024" \
         -H "Authorization: Bearer <token>" \
         -H "Content-Type: application/json" \
         -d '{"name": "Updated Name", "description": "..."}'
    ```

---

## `DELETE /ark:{NAAN}/{postfix}`

Delete a registered identifier. By default, deletion is blocked if other objects depend on this one. Use `?force=true` to override.

**Query Parameters:**

| Parameter | Description |
|-----------|-------------|
| `force` | Set to `true` to delete even if other objects reference this identifier |

!!! warning
    Deleting an identifier is irreversible. If the identifier has associated file content (e.g., a Dataset or ROCrate), the files are also removed from storage.

=== "Python"

    ```python
    # Safe delete (fails if other objects depend on this)
    response = requests.delete(
        f"{BASE_URL}/ark:59853/my-dataset-2024",
        headers={"Authorization": f"Bearer {token}"}
    )

    # Force delete
    response = requests.delete(
        f"{BASE_URL}/ark:59853/my-dataset-2024",
        params={"force": "true"},
        headers={"Authorization": f"Bearer {token}"}
    )
    print(response.status_code)
    ```

=== "curl"

    ```shell
    # Safe delete
    curl -X DELETE "https://fairscape.net/api/ark:59853/my-dataset-2024" \
         -H "Authorization: Bearer <token>"

    # Force delete
    curl -X DELETE "https://fairscape.net/api/ark:59853/my-dataset-2024?force=true" \
         -H "Authorization: Bearer <token>"
    ```

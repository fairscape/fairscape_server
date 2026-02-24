# Schema Endpoints

Schemas define the expected structure and constraints for tabular or structured datasets registered in FAIRSCAPE. Associating a schema with a dataset enables automated validation.

All URLs are relative to `https://fairscape.net/api`.

---

## Endpoint Summary

| Method | Path | Auth Required | Description |
|--------|------|:-------------:|-------------|
| `POST` | `/schema` | ✓ | Create a schema record |
| `GET` | `/schema/ark:{NAAN}/{postfix}` | – | Get schema metadata |
| `DELETE` | `/schema/ark:{NAAN}/{postfix}` | ✓ | Delete a schema |

---

## `POST /schema`

Register a schema. The request body is a JSON object describing the schema structure and its properties.

**Key metadata fields:**

```json
{
  "@id": "ark:59853/apms-embeddings-schema-2024",
  "@type": "Schema",
  "name": "AP-MS Embeddings Schema",
  "description": "Schema for the AP-MS protein embedding CSV",
  "author": "Krogan Lab",
  "version": "1.0",
  "datePublished": "2024-01-15",
  "keywords": ["proteomics"],
  "properties": [
    {
      "name": "protein_id",
      "type": "string",
      "description": "UniProt protein identifier",
      "required": true
    },
    {
      "name": "embedding_dim_1",
      "type": "float",
      "description": "First embedding dimension"
    }
  ]
}
```

=== "Python"

    ```python
    schema = {
        "@id": "ark:59853/apms-embeddings-schema-2024",
        "@type": "Schema",
        "name": "AP-MS Embeddings Schema",
        "description": "Schema for the AP-MS protein embedding CSV",
        "author": "Krogan Lab",
        "version": "1.0",
        "datePublished": "2024-01-15",
        "keywords": ["proteomics"],
        "properties": [
            {"name": "protein_id", "type": "string", "description": "UniProt ID", "required": True},
            {"name": "embedding_dim_1", "type": "float", "description": "Embedding dim 1"}
        ]
    }

    response = requests.post(
        f"{BASE_URL}/schema",
        json=schema,
        headers={"Authorization": f"Bearer {token}"}
    )
    print(response.json())
    ```

=== "curl"

    ```shell
    curl -X POST "https://fairscape.net/api/schema" \
         -H "Authorization: Bearer <token>" \
         -H "Content-Type: application/json" \
         -d @schema.json
    ```

---

## `GET /schema/ark:{NAAN}/{postfix}`

Retrieve schema metadata. Public endpoint.

=== "Python"

    ```python
    response = requests.get(
        f"{BASE_URL}/schema/ark:59853/apms-embeddings-schema-2024"
    )
    print(response.json())
    ```

=== "curl"

    ```shell
    curl "https://fairscape.net/api/schema/ark:59853/apms-embeddings-schema-2024"
    ```

---

## `DELETE /schema/ark:{NAAN}/{postfix}`

Delete a schema record.

=== "curl"

    ```shell
    curl -X DELETE "https://fairscape.net/api/schema/ark:59853/apms-embeddings-schema-2024" \
         -H "Authorization: Bearer <token>"
    ```

!!! note
    Use [fairscape-cli](https://github.com/fairscape/fairscape-cli) to infer schemas directly from CSV or HDF5 files with `fairscape-cli schema infer`.

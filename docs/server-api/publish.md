# Publish & Content Endpoints

These endpoints control publication status of registered objects and provide direct access to stored file content.

All URLs are relative to `https://fairscape.net/api`.

---

## Endpoint Summary

| Method | Path | Auth Required | Description |
|--------|------|:-------------:|-------------|
| `PUT` | `/publish` | ✓ | Update the publication status of an identifier |
| `GET` | `/view/ark:{NAAN}/{postfix}` | – | View file content inline in the browser |
| `GET` | `/download/ark:{NAAN}/{postfix}` | – | Force-download file content |
| `GET` | `/healthz` | – | Server health check |

---

## `PUT /publish`

Update the publication status of a registered identifier. Use this to make objects publicly visible or to withdraw them.

**Request body:**

```json
{
  "@id": "ark:59853/my-dataset-2024",
  "published": true
}
```

=== "Python"

    ```python
    response = requests.put(
        f"{BASE_URL}/publish",
        json={
            "@id": "ark:59853/my-dataset-2024",
            "published": True
        },
        headers={"Authorization": f"Bearer {token}"}
    )
    print(response.status_code, response.json())
    ```

=== "curl"

    ```shell
    curl -X PUT "https://fairscape.net/api/publish" \
         -H "Authorization: Bearer <token>" \
         -H "Content-Type: application/json" \
         -d '{"@id": "ark:59853/my-dataset-2024", "published": true}'
    ```

---

## `GET /view/ark:{NAAN}/{postfix}`

Stream the file content for a dataset or software object, served **inline** (suitable for previewing in a browser). The `Content-Type` is inferred from the filename.

=== "Python"

    ```python
    response = requests.get(
        f"{BASE_URL}/view/ark:59853/apms-embeddings-2024",
        stream=True
    )
    # Content-Type will be text/csv, image/png, etc.
    print(response.headers["Content-Type"])
    ```

=== "curl"

    ```shell
    curl "https://fairscape.net/api/view/ark:59853/apms-embeddings-2024"
    ```

---

## `GET /download/ark:{NAAN}/{postfix}`

Stream the file content as an attachment download. The browser will prompt the user to save the file.

=== "Python"

    ```python
    response = requests.get(
        f"{BASE_URL}/download/ark:59853/apms-embeddings-2024",
        stream=True
    )
    filename = response.headers.get("Content-Disposition", "").split("filename=")[-1].strip('"')
    with open(filename or "download", "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    ```

=== "curl"

    ```shell
    curl -O -J "https://fairscape.net/api/download/ark:59853/apms-embeddings-2024"
    ```

!!! note
    `/view/` and `/download/` serve the same file content but differ only in the `Content-Disposition` header (`inline` vs `attachment`). The dataset-specific endpoint `GET /dataset/download/ark:…` requires authentication, while these endpoints are public for published objects.

---

## `GET /healthz`

Check whether the server is running. Returns `200 OK` when healthy.

=== "curl"

    ```shell
    curl "https://fairscape.net/api/healthz"
    ```

**Response:**
```json
{ "status": "healthy" }
```

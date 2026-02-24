# FAIRSCAPE Server API

The FAIRSCAPE Server is the core backend service of the FAIRSCAPE ecosystem, providing a REST API for storing, retrieving, and managing research objects and their provenance metadata.

## Base URL

```
https://fairscape.net/api
```

The interactive Swagger UI (with "Try it out" support) is available at:

```
https://fairscape.net/api/docs
```

## Authentication

Most write operations require a **Bearer token**. Obtain one by posting credentials to `/login`:

```
POST /login
```

Include the token in subsequent requests:

```
Authorization: Bearer <your_token>
```

## ARK Identifiers

Every object registered in FAIRSCAPE receives a persistent **ARK identifier**:

```
ark:{NAAN}/{postfix}
```

For example: `ark:59853/my-rocrate-2024`

ARK identifiers are resolvable universally via `GET /ark:{NAAN}/{postfix}` and work across all resource types.

## Quickstart

The most common workflow is:

1. **Authenticate** — `POST /login`
2. **Upload a RO-Crate** — `POST /rocrate/upload-async`
3. **Poll upload status** — `GET /rocrate/upload/status/{submissionUUID}`
4. **Fetch by ARK identifier** — `GET /ark:{NAAN}/{postfix}`

See the [Upload & Fetch Workflow](server-api/workflow-upload-fetch.md) for a complete walkthrough with Python and curl examples.

## Endpoint Groups

| Section | Endpoints |
|---------|-----------|
| [Upload & Fetch Workflow](server-api/workflow-upload-fetch.md) | End-to-end guide: authenticate, upload, poll, fetch |
| [ROCrate](server-api/rocrate.md) | Upload, list, download, metadata, AI-Ready scoring |
| [Identifier Resolution](server-api/identifier-resolution.md) | Resolve, update, delete any ARK identifier |
| [Dataset](server-api/dataset.md) | Create, fetch metadata, download, delete |
| [Software & Computation](server-api/software-computation.md) | Register and retrieve software and computation records |
| [Schema](server-api/schema.md) | Create and retrieve data schemas |
| [Evidence Graph](server-api/evidence-graph.md) | Build and query provenance graphs |
| [Search](server-api/search.md) | Keyword and semantic search across all objects |
| [Publish & Content](server-api/publish.md) | Update publish status, view or download file content |

## GitHub Repository

Source code: [github.com/fairscape/mds_python](https://github.com/fairscape/mds_python)

!!! note
    When the repository is renamed to `fairscape_server`, the docs URL will move to `https://fairscape.github.io/fairscape_server/` — update `site_url` in `mkdocs.yml` at that time.

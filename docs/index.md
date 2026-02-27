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

## Deployment

### Docker Compose (Local)

The repository includes a `compose.yaml` for running a complete local stack:

```bash
git clone https://github.com/fairscape/mds_python
cd mds_python
docker compose up --build
```

This starts the following services:

| Service | Description | Port |
|---------|-------------|------|
| `fairscape-api` | This REST API server | `8080` |
| `fairscape-frontend` | React web UI | `5173` |
| `mongo` | MongoDB metadata store | `27017` (internal) |
| `mongo-express` | MongoDB admin UI | `8081` |
| `minio` | S3-compatible object storage | `9000` / `9001` |
| `redis` | Async job queue | `6379` (internal) |
| `fairscape-worker` | Background job processor | — |

Configuration is loaded from `deploy/docker_compose.env`. See the [Installation Guide](https://fairscape.github.io/getting-started/installation/) for default credentials and full setup details.

### Optional Environment Variables

Two environment variables enable optional server features. Docker Compose will warn at startup if they are not set — this is expected and the server functions normally without them.

#### `GEMINI_API_KEY` — LLM Assist

Set a Google Gemini API key to enable AI-powered metadata enrichment features. When absent, LLM-related endpoints degrade gracefully. Used by `fairscape_mds/crud/llm_assist.py`.

#### `GITHUB_TOKEN` — D4D GitHub Integration

Set a GitHub personal access token to enable the `/api/github/*` endpoint group. These endpoints power the D4D (Data Datasheet for Datasets) interactive creation workflow, which integrates with GitHub Issues (default repo: `bridge2ai/data-sheets-schema`).

Without `GITHUB_TOKEN`, all `/api/github/*` requests return:
```json
HTTP 503: "GitHub integration is not configured. Please set GITHUB_TOKEN environment variable."
```

Optionally pair with `GITHUB_REPO_NAME` to point the integration at a different repository (default: `bridge2ai/data-sheets-schema`).

Full variable reference: [Configuration Documentation](https://fairscape.github.io/getting-started/configuration/)

## GitHub Repository

Source code: [github.com/fairscape/mds_python](https://github.com/fairscape/mds_python)

!!! note
    When the repository is renamed to `fairscape_server`, the docs URL will move to `https://fairscape.github.io/fairscape_server/` — update `site_url` in `mkdocs.yml` at that time.

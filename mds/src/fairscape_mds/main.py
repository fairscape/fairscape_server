from fairscape_mds.deps import getCurrentUser
from fairscape_mds.routers.auth import authRouter
from fairscape_mds.routers.dataset import datasetRouter
from fairscape_mds.routers.software import softwareRouter
from fairscape_mds.routers.computation import computationRouter
from fairscape_mds.routers.rocrate import rocrateRouter
from fairscape_mds.routers.schema import schemaRouter
from fairscape_mds.routers.resolver import resolverRouter
from fairscape_mds.routers.credentitals import router as credentials_router
from fairscape_mds.routers.evidence_graph import router as evidence_graph_router
from fairscape_mds.routers.search import router as search_router
from fairscape_mds.routers.publish import publishRouter
from fairscape_mds.routers.llm_assist import router as llm_assist_router
from fairscape_mds.routers.github import router as github_router
from fairscape_mds.routers.mlmodel import mlModelRouter

from fairscape_mds.core.logging import requestLogger
from fairscape_mds.core.config import settings

from fastapi.middleware.cors import CORSMiddleware 
from fastapi import FastAPI, Request

import logfire

app = FastAPI(
	root_path="/api",
	title="Fairscape API",
	description="Backend Fairscape API for storing EVI Providence Graphs and rich provenance metadata"
)

if settings.FAIRSCAPE_LOGFIRE_ENV and settings.FAIRSCAPE_LOGFIRE_TOKEN:
    logfire.configure(
        environment = settings.FAIRSCAPE_LOGFIRE_ENV,
        token = settings.FAIRSCAPE_LOGFIRE_TOKEN
    )
    logfire.instrument_fastapi(app)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"], 
    allow_headers=["*"],
)


@app.middleware('http')
def LogRequestMiddleware(
    request: Request,
    call_next
):
    # track user agent
    requestPath = request.url.path
    requestUserAgent = request.headers.get("User-Agent")
    if request.client:
        requestClientAddress = request.client.host
    else:
        requestClientAddress = None

    #log the request
    requestLogger.info(f"Path: {requestPath}\tUserAgent: {requestUserAgent}\tIP: {requestClientAddress}")

    response = call_next(request)
    return response

app.include_router(resolverRouter)
app.include_router(authRouter)
app.include_router(datasetRouter)
app.include_router(softwareRouter)
app.include_router(computationRouter)
app.include_router(rocrateRouter)
app.include_router(schemaRouter)
app.include_router(credentials_router)
app.include_router(evidence_graph_router)
app.include_router(search_router)
app.include_router(publishRouter)
app.include_router(mlModelRouter)
app.include_router(llm_assist_router)
app.include_router(github_router)


@app.get("/healthz")
def health_check():
    return {"status": "healthy"}
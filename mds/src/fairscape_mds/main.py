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

from fairscape_mds.core.logging import requestLogger

from fastapi.middleware.cors import CORSMiddleware 
from fastapi import FastAPI, Request


app = FastAPI(
	root_path="/api",
	title="Fairscape API",
	description="Backend Fairscape API for storing EVI Providence Graphs and rich provenance metadata"
)


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


@app.get("/healthz")
def health_check():
    return {"status": "healthy"}
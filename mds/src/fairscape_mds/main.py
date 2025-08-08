from fairscape_mds.deps import getCurrentUser
from fairscape_mds.routers.auth import authRouter
from fairscape_mds.routers.dataset import datasetRouter
from fairscape_mds.routers.software import softwareRouter
from fairscape_mds.routers.computation import computationRouter
from fairscape_mds.routers.rocrate import rocrateRouter
from fairscape_mds.routers.schema import schemaRouter
from fairscape_mds.routers.resolver import resolverRouter
from fairscape_mds.backend.credentitals_router import router as credentials_router
from fairscape_mds.backend.evidence_graph_router import router as evidence_graph_router
from fairscape_mds.backend.search_router import router as search_router

from fastapi import FastAPI, Depends, HTTPException, Path, UploadFile, Form, File
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import HTTPException
from fastapi.security import OAuth2PasswordRequestForm, OAuth2PasswordBearer
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware 
from typing import Annotated
import pathlib
import mimetypes


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


@app.get("/healthz")
def health_check():
    return {"status": "healthy"}
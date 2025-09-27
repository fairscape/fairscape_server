from fastapi import APIRouter, Header
from fastapi.responses import JSONResponse, Response
from fairscape_mds.crud.resolver import FairscapeResolverRequest
from fairscape_mds.core.config import appConfig
from typing import Optional
import json
from rdflib import Graph

resolverRequest = FairscapeResolverRequest(appConfig)
resolverRouter = APIRouter(prefix="", tags=['evi', 'rocrate'])

@resolverRouter.get("/ark:{NAAN}/{postfix}")
def resolveARK(
    NAAN: str,
    postfix: str,
    accept: Optional[str] = Header(default="application/json")
):
    guid = f"ark:{NAAN}/{postfix}"
    response = resolverRequest.resolveIdentifier(guid)
    
    if not response.success:
        return JSONResponse(
            status_code=response.statusCode,
            content=response.error
        )
    
    metadata = response.model
    if isinstance(metadata, dict) and "@context" not in metadata:
        metadata["@context"] = {
            "@vocab": "https://schema.org/",
            "EVI": "https://w3id.org/EVI#"
        }
    
    if "turtle" in accept.lower() or accept.lower() == "text/turtle":
        g = Graph()
        g.parse(data=metadata, format='json-ld')
        turtle_data = g.serialize(format='turtle')
        return Response(
            content=turtle_data,
            status_code=response.statusCode,
            media_type="text/turtle"
        )
    elif "rdf" in accept.lower() or accept.lower() == "application/rdf+xml":
        g = Graph()
        g.parse(data=metadata, format='json-ld')
        rdf_data = g.serialize(format='xml')
        return Response(
            content=rdf_data,
            status_code=response.statusCode,
            media_type="application/rdf+xml"
        )
    else:
        return JSONResponse(
            content=metadata,
            status_code=response.statusCode,
            media_type="application/json"
        )
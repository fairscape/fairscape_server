from fastapi import (
	APIRouter, 
	Header,
    Depends
)
from fastapi.responses import JSONResponse, Response
from fairscape_mds.crud.resolver import FairscapeResolverRequest
from fairscape_mds.core.config import appConfig
from fairscape_mds.crud.identifier import IdentifierRequest
from fairscape_mds.models.identifier import MetadataUnion
from fairscape_mds.models.user import UserWriteModel
from fairscape_mds.deps import getCurrentUser
from typing import Optional, Annotated
import json
from rdflib import Graph

resolverRequest = FairscapeResolverRequest(appConfig)
identifierRequest = IdentifierRequest(appConfig)
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


@resolverRouter.put("/ark:{NAAN}/{postfix}")
def updateARK(
	currentUser: Annotated[UserWriteModel, Depends(getCurrentUser)],
    NAAN: str,
    postfix: str,
    newMetadata: MetadataUnion
):
    fullArk = f"ark:{NAAN}/{postfix}"

    updateResponse = identifierRequest.updateMetadata(
        guid=fullArk,
        user=currentUser,
        newMetadata=newMetadata
    )

    if not updateResponse.success:
        return JSONResponse(
            status_code = updateResponse.statusCode,
            content = updateResponse.error
        )

    else:
        return JSONResponse(
            status_code = updateResponse.statusCode,
            content = updateResponse.model.model_dump(by_alias=True, mode="json")
        )


@resolverRouter.delete("/ark:{NAAN}/{postfix}")
def deleteARK(
	currentUser: Annotated[UserWriteModel, Depends(getCurrentUser)],
    NAAN: str,
    postfix: str,
    force: str | None = None,
):
    fullArk = f"ark:{NAAN}/{postfix}"

    if force == "true":
        queryForceDelete = True
    else:
        queryForceDelete = False

    deleteResponse = identifierRequest.deleteIdentifier(
        guid = fullArk,
        forceDelete = queryForceDelete, 
        user = currentUser
    )

    if deleteResponse.success:
        return JSONResponse(
            status_code = deleteResponse.statusCode,
            content= deleteResponse.jsonResponse
        )
    else:
        return JSONResponse(
            status_code = deleteResponse.statusCode,
            content= deleteResponse.error
        )



import pymongo
import datetime
from fairscape_mds.crud.fairscape_request import FairscapeRequest
from fairscape_mds.crud.fairscape_response import FairscapeResponse
from fairscape_mds.models.user import UserWriteModel, Permissions
from fairscape_mds.models.evidence_graph import EvidenceGraph, EvidenceGraphCreate
from fairscape_mds.models.identifier import StoredIdentifier, PublicationStatusEnum, MetadataTypeEnum

class FairscapeEvidenceGraphRequest(FairscapeRequest):

    def create_evidence_graph(
        self,
        requesting_user: UserWriteModel,
        evi_graph_create_model: EvidenceGraphCreate
    ) -> FairscapeResponse:
        if self.config.identifierCollection.find_one({"@id": evi_graph_create_model.guid}):
            return FairscapeResponse(
                success=False,
                statusCode=409,
                error={"message": f"EvidenceGraph with @id '{evi_graph_create_model.guid}' already exists."}
            )

        evidence_graph_data = {
            "@id": evi_graph_create_model.guid,
            "name": evi_graph_create_model.name,
            "description": evi_graph_create_model.description,
            "owner": requesting_user.email,
            "@type": "evi:EvidenceGraph",
            "graph": None
        }

        try:
            evidence_graph = EvidenceGraph.model_validate(evidence_graph_data)

            default_permissions = Permissions(
                owner=requesting_user.email,
                group=[],
                public=True
            )

            now = datetime.datetime.utcnow()
            stored_identifier = StoredIdentifier(
                guid=evidence_graph.guid,
                metadataType=MetadataTypeEnum.EVIDENCE_GRAPH,
                metadata=evidence_graph,
                publicationStatus=PublicationStatusEnum.PUBLISHED,
                permissions=default_permissions,
                distribution=None,
                descriptiveStatistics={},
                dateCreated=now,
                dateModified=now
            )

            insert_data = stored_identifier.model_dump(by_alias=True, mode="json")
            result = self.config.identifierCollection.insert_one(insert_data)
            if result.inserted_id:
                return FairscapeResponse(success=True, statusCode=201, model=stored_identifier)
            else:
                return FairscapeResponse(success=False, statusCode=500, error={"message": "Failed to insert evidence graph."})
        except Exception as e: # Handles Pydantic ValidationError and other exceptions
            return FairscapeResponse(success=False, statusCode=500, error={"message": f"Error creating evidence graph: {str(e)}"})

    def get_evidence_graph(self, evidence_id: str) -> FairscapeResponse:
        graph_data = self.config.identifierCollection.find_one({"@id": evidence_id}, {"_id": 0})
        if not graph_data:
            return FairscapeResponse(success=False, statusCode=404, error={"message": "EvidenceGraph not found"})
        try:
            stored_identifier = StoredIdentifier.model_validate(graph_data)
            return FairscapeResponse(success=True, statusCode=200, model=stored_identifier)
        except Exception as e:
            return FairscapeResponse(success=False, statusCode=500, error={"message": f"Data validation error for EvidenceGraph {evidence_id}: {str(e)}"})

    def delete_evidence_graph(self, requesting_user: UserWriteModel, evidence_id: str) -> FairscapeResponse:
        graph_data = self.config.identifierCollection.find_one({"@id": evidence_id})
        if not graph_data:
            return FairscapeResponse(success=False, statusCode=404, error={"message": "EvidenceGraph not found"})

        if graph_data.get("owner") != requesting_user.email:
            return FairscapeResponse(success=False, statusCode=403, error={"message": "User not authorized to delete this EvidenceGraph"})

        try:
            result = self.config.identifierCollection.delete_one({"@id": evidence_id})
            if result.deleted_count == 1:
                self.config.identifierCollection.update_many(
                    {"metadata.hasEvidenceGraph.@id": evidence_id},
                    {"$unset": {"metadata.hasEvidenceGraph": ""}}
                )
                return FairscapeResponse(success=True, statusCode=200, model={"deleted": {"@id": evidence_id}})
            else:
                return FairscapeResponse(success=False, statusCode=404, error={"message": "EvidenceGraph not found during delete, or already deleted."})
        except Exception as e:
            return FairscapeResponse(success=False, statusCode=500, error={"message": f"Error deleting evidence graph: {str(e)}"})

    def list_evidence_graphs(self) -> FairscapeResponse:
        from fairscape_mds.models.evidencegraph import list_evidence_graphs_from_db
        return list_evidence_graphs_from_db(self.identifierCollection)

    def build_evidence_graph_for_node(
        self,
        requesting_user: UserWriteModel,
        naan: str,
        postfix: str
    ) -> FairscapeResponse:
        node_id = f"ark:{naan}/{postfix}"
        evidence_graph_id = f"ark:{naan}/evidence-graph-{postfix}"

        source_node_data = self.config.identifierCollection.find_one({"@id": node_id})
        if not source_node_data:
            return FairscapeResponse(
                success=False,
                statusCode=404,
                error={"message": f"Source node {node_id} not found."}
            )

        existing_graph_id = (
            source_node_data.get("metadata", {})
            .get("hasEvidenceGraph", {})
            .get("@id")
        )
        if existing_graph_id:
            existing_graph_data = self.config.identifierCollection.find_one(
                {"@id": existing_graph_id}, {"_id": 0}
            )
            if existing_graph_data:
                try:
                    print("Returning existing graph")
                    stored_identifier = StoredIdentifier.model_validate(existing_graph_data)
                    return FairscapeResponse(
                        success=True,
                        statusCode=200,
                        model=stored_identifier
                    )
                except Exception as e:
                    return FairscapeResponse(
                        success=False,
                        statusCode=500,
                        error={"message": f"Error validating existing EvidenceGraph {existing_graph_id}: {str(e)}"}
                    )

        evidence_graph_data_to_validate = {
            "@id": evidence_graph_id,
            "name": f"Evidence Graph for {node_id}",
            "description": f"Automatically generated Evidence Graph for node {node_id}",
            "owner": requesting_user.email,
            "@type": "evi:EvidenceGraph",
            "graph": None
        }

        try:
            evidence_graph = EvidenceGraph.model_validate(evidence_graph_data_to_validate)
            evidence_graph.build_graph(node_id, self.config.identifierCollection)
        except Exception as e:
            return FairscapeResponse(
                success=False,
                statusCode=500,
                error={"message": f"Error building evidence graph: {str(e)}"}
            )

        default_permissions = Permissions(
            owner=requesting_user.email,
            group=[],
            public=True
        )

        now = datetime.datetime.utcnow()
        stored_identifier = StoredIdentifier(
            guid=evidence_graph.guid,
            metadataType=MetadataTypeEnum.EVIDENCE_GRAPH,
            metadata=evidence_graph,
            publicationStatus=PublicationStatusEnum.PUBLISHED,
            permissions=default_permissions,
            distribution=None,
            descriptiveStatistics={},
            dateCreated=now,
            dateModified=now
        )

        try:
            insert_data = stored_identifier.model_dump(by_alias=True, mode="json")
            self.config.identifierCollection.insert_one(insert_data)
        except pymongo.errors.DuplicateKeyError:
            return FairscapeResponse(
                success=False,
                statusCode=409,
                error={"message": f"EvidenceGraph with @id '{evidence_graph_id}' already exists (race condition)."}
            )
        except Exception as e:
            return FairscapeResponse(
                success=False,
                statusCode=500,
                error={"message": f"Error storing new evidence graph: {str(e)}"}
            )

        try:
            self.config.identifierCollection.update_one(
                {"@id": node_id},
                {"$set": {"metadata.hasEvidenceGraph": {"@id": stored_identifier.guid}}}
            )
        except Exception as e:
            return FairscapeResponse(
                success=False,
                statusCode=500,
                model=stored_identifier,
                error={
                    "message": f"EvidenceGraph created but failed to link to source node {node_id}: {str(e)} (graph @id: {stored_identifier.guid})"
                }
            )

        return FairscapeResponse(success=True, statusCode=201, model=stored_identifier)

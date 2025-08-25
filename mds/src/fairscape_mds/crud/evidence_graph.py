import pymongo
from fairscape_mds.crud.fairscape_request import FairscapeRequest
from fairscape_mds.crud.fairscape_response import FairscapeResponse
from fairscape_mds.models.user import UserWriteModel
from fairscape_mds.backend.evidence_graph import EvidenceGraph, EvidenceGraphCreate

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
            insert_data = evidence_graph.model_dump(by_alias=True)
            result = self.config.identifierCollection.insert_one(insert_data)
            if result.inserted_id:
                return FairscapeResponse(success=True, statusCode=201, model=evidence_graph)
            else:
                return FairscapeResponse(success=False, statusCode=500, error={"message": "Failed to insert evidence graph."})
        except Exception as e: # Handles Pydantic ValidationError and other exceptions
            return FairscapeResponse(success=False, statusCode=500, error={"message": f"Error creating evidence graph: {str(e)}"})

    def get_evidence_graph(self, evidence_id: str) -> FairscapeResponse:
        graph_data = self.config.identifierCollection.find_one({"@id": evidence_id}, {"_id": 0})
        if not graph_data:
            return FairscapeResponse(success=False, statusCode=404, error={"message": "EvidenceGraph not found"})
        try:
            evidence_graph = EvidenceGraph.model_validate(graph_data)
            return FairscapeResponse(success=True, statusCode=200, model=evidence_graph)
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
            return FairscapeResponse(success=False, statusCode=404, error={"message": f"Source node {node_id} not found."})

        existing_graph_id_from_node = source_node_data.get("metadata", {}).get("hasEvidenceGraph", {}).get("@id")
        existing_graph_with_target_id = self.config.identifierCollection.find_one({"@id": evidence_graph_id})

        ids_to_delete = set()
        if existing_graph_id_from_node:
            ids_to_delete.add(existing_graph_id_from_node)
        if existing_graph_with_target_id:
            ids_to_delete.add(evidence_graph_id)

        for graph_id_to_delete in ids_to_delete:
            delete_resp = self.delete_evidence_graph(requesting_user, graph_id_to_delete)
            if not delete_resp.success and delete_resp.statusCode != 404:
                print(f"Warning: Could not delete existing evidence graph {graph_id_to_delete}: {delete_resp.error}")

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
            return FairscapeResponse(success=False, statusCode=500, error={"message": f"Error building evidence graph: {str(e)}"})

        try:
            insert_data = evidence_graph.model_dump(by_alias=True)
            self.config.identifierCollection.insert_one(insert_data)
        except pymongo.errors.DuplicateKeyError:
            return FairscapeResponse(success=False, statusCode=409, error={"message": f"EvidenceGraph with @id '{evidence_graph_id}' already exists (race condition or failed cleanup)."})
        except Exception as e:
            return FairscapeResponse(success=False, statusCode=500, error={"message": f"Error storing new evidence graph: {str(e)}"})

        try:
            self.config.identifierCollection.update_one(
                {"@id": node_id},
                {"$set": {"metadata.hasEvidenceGraph": {"@id": evidence_graph.guid}}}
            )
        except Exception as e:
            return FairscapeResponse(
                success=False, 
                statusCode=500, 
                model=evidence_graph, 
                error={
                    "message": f"EvidenceGraph created but failed to link to source node {node_id}: {str(e)} (graph @id: {evidence_graph.guid})"
                }
            )

        return FairscapeResponse(success=True, statusCode=201, model=evidence_graph)
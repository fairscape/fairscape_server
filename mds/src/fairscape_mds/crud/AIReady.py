from typing import List, Dict, Any, Optional
import datetime

from fairscape_models.conversion.models.AIReady import AIReadyScore

from fairscape_mds.crud.fairscape_request import FairscapeRequest
from fairscape_mds.crud.fairscape_response import FairscapeResponse
from fairscape_mds.models.identifier import StoredIdentifier, PublicationStatusEnum, MetadataTypeEnum
from fairscape_mds.models.user import Permissions

class FairscapeAIReadyScoreRequest(FairscapeRequest):
    
    def create_ai_ready_score(
        self,
        rocrate_id: str,
        score: AIReadyScore,
        owner_email: Optional[str] = None
    ) -> FairscapeResponse:
        score_id = f"{rocrate_id}-ai-ready-score"

        existing = self.config.identifierCollection.find_one({"@id": score_id})
        if existing:
            return FairscapeResponse(
                success=False,
                statusCode=409,
                error={"message": f"AI-Ready Score {score_id} already exists"}
            )

        # Fetch the RO-Crate to get its name
        rocrate_entity = self.config.identifierCollection.find_one({"@id": rocrate_id}, {"_id": 0})
        rocrate_name = "Unknown RO-Crate"
        if rocrate_entity:
            if "metadata" in rocrate_entity and isinstance(rocrate_entity["metadata"], dict):
                rocrate_name = rocrate_entity["metadata"].get("name", rocrate_entity.get("name", rocrate_id))
            else:
                rocrate_name = rocrate_entity.get("name", rocrate_id)

        score_metadata = score 

        default_permissions = Permissions(
            owner=owner_email,
            group=[],
            public=True
        )

        now = datetime.datetime.utcnow()
        stored_identifier = StoredIdentifier(
            guid=score_id,
            metadataType=MetadataTypeEnum.AI_READY_SCORE,
            metadata=score_metadata,
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

            self.config.identifierCollection.update_one(
                {"@id": rocrate_id},
                {"$set": {"metadata.hasAIReadyScore": {"@id": score_id}}}
            )

            return FairscapeResponse(
                success=True,
                statusCode=201,
                model=stored_identifier
            )
        except Exception as e:
            return FairscapeResponse(
                success=False,
                statusCode=500,
                error={"message": f"Error creating AI-Ready Score: {str(e)}"}
            )
    
    def build_metadata_graph_for_rocrate(
        self,
        rocrate_id: str
    ) -> List[Dict[str, Any]]:
        metadata_graph = []
        processed_ids = set()
        
        def fetch_entity(entity_id: str):
            if entity_id in processed_ids:
                return
            
            processed_ids.add(entity_id)
            entity = self.config.identifierCollection.find_one({"@id": entity_id}, {"_id": 0})
            
            if not entity:
                return
            
            if "metadata" in entity:
                flattened = {k: v for k, v in entity.items() if k != "metadata"}
                if isinstance(entity["metadata"], dict):
                    flattened.update(entity["metadata"])
                entity = flattened
            
            metadata_graph.append(entity)
            
            entity_type = entity.get("@type", [])
            if isinstance(entity_type, str):
                entity_type = [entity_type]
            
            is_rocrate = any("ROCrate" in t for t in entity_type)
            
            if entity.get("hasPart"):
                parts = entity["hasPart"]
                if not isinstance(parts, list):
                    parts = [parts]
                
                for part in parts:
                    if isinstance(part, dict) and part.get("@id"):
                        part_id = part["@id"]
                        fetch_entity(part_id)
            
            if is_rocrate and entity.get("outputs"):
                outputs = entity["outputs"]
                if not isinstance(outputs, list):
                    outputs = [outputs]
                
                for output in outputs:
                    if isinstance(output, dict) and output.get("@id"):
                        fetch_entity(output["@id"])
        
        fetch_entity(rocrate_id)
        return metadata_graph
    
    def delete_ai_ready_score(
        self,
        rocrate_id: str
    ) -> FairscapeResponse:
        score_id = f"{rocrate_id}-ai-ready-score"
        
        try:
            delete_result = self.config.identifierCollection.delete_one({"@id": score_id})
            
            if delete_result.deleted_count == 0:
                return FairscapeResponse(
                    success=False,
                    statusCode=404,
                    error={"message": f"AI-Ready Score {score_id} not found"}
                )
            
            self.config.identifierCollection.update_one(
                {"@id": rocrate_id},
                {"$unset": {"metadata.hasAIReadyScore": ""}}
            )
            
            return FairscapeResponse(
                success=True,
                statusCode=200,
                model={"message": f"AI-Ready Score {score_id} deleted successfully"}
            )
            
        except Exception as e:
            return FairscapeResponse(
                success=False,
                statusCode=500,
                error={"message": f"Error deleting AI-Ready Score: {str(e)}"}
            )
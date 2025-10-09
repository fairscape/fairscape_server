from typing import List, Dict, Any, Optional
import datetime

from fairscape_models.conversion.models.AIReady import AIReadyScore

from fairscape_mds.crud.fairscape_request import FairscapeRequest
from fairscape_mds.crud.fairscape_response import FairscapeResponse

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
        
        score_doc = {
            "@id": score_id,
            "@type": "AIReadyScore",
            "name": rocrate_name,
            "sourceROCrate": {"@id": rocrate_id},
            "owner": owner_email or "system@fairscape.org",
            "dateCreated": datetime.datetime.utcnow().isoformat(),
            "fairness": score.fairness.model_dump(),
            "provenance": score.provenance.model_dump(),
            "characterization": score.characterization.model_dump(),
            "pre_model_explainability": score.pre_model_explainability.model_dump(),
            "ethics": score.ethics.model_dump(),
            "sustainability": score.sustainability.model_dump(),
            "computability": score.computability.model_dump()
        }
        
        try:
            self.config.identifierCollection.insert_one(score_doc)
            
            self.config.identifierCollection.update_one(
                {"@id": rocrate_id},
                {"$set": {"metadata.hasAIReadyScore": {"@id": score_id}}}
            )
            
            return FairscapeResponse(
                success=True,
                statusCode=201,
                model=score_doc
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
from pydantic import BaseModel, Field, ValidationError
from typing import Dict, List, Optional, Any, Union
import pymongo
from fairscape_mds.backend.models import FairscapeResponse, UserWriteModel


class EvidenceNode: 
   def __init__(self, id: str, type: str):
       self.id = id
       self.type = type
       # For Computation nodes
       self.usedSoftware: Optional[List[str]] = None
       self.usedDataset: Optional[List[str]] = None
       self.usedSample: Optional[List[str]] = None
       self.usedInstrument: Optional[List[str]] = None
       # For Dataset/Sample/Instrument nodes
       self.generatedBy: Optional[str] = None

class EvidenceGraph(BaseModel):
    metadataType: str = Field(default="evi:EvidenceGraph", alias="@type")
    guid: str = Field(alias="@id")
    owner: str 
    description: str
    name: str = Field(default="Evidence Graph")
    graph: Optional[Dict[str, Any]] = Field(default=None, alias="@graph")

    class Config:
        extra = 'allow' 
        populate_by_name = True 

    def _flatten_metadata(self, node: Dict) -> Dict:
        """Flatten metadata field if it exists, preserving top-level fields."""
        if "metadata" not in node:
            return node

        flattened = {k: v for k, v in node.items() if k != "metadata"}
        metadata_content = node["metadata"]
        if isinstance(metadata_content, dict):
            for key, value in metadata_content.items():
                if key not in flattened:
                    flattened[key] = value
        return flattened

    def _build_graph_recursive(self, node_id: str, collection: pymongo.collection.Collection, processed: set) -> Dict:
        if node_id in processed:
            return {"@id": node_id}

        node_data = collection.find_one({"@id": node_id}, {"_id": 0})
        # print(f"node_data for {node_id}: {node_data}") # Debug
        if not node_data:
            return {"@id": node_id, "error": "not found"} # Mark as not found

        # Flatten metadata here
        node = self._flatten_metadata(node_data)
        processed.add(node_id)

        result_node = {
            "@id": node.get("@id"),
            "@type": node.get("@type"),
            "name": node.get("name"),
            "description": node.get("description")
        }

        node_type_field = node.get("@type", "")
        # Handle if @type is a list
        current_node_type_str = ""
        if isinstance(node_type_field, list):
            # Prioritize specific types if multiple are present
            if "Dataset" in node_type_field: current_node_type_str = "Dataset"
            elif "Computation" in node_type_field: current_node_type_str = "Computation"
            elif "Sample" in node_type_field: current_node_type_str = "Sample"
            # Add other types as needed
            elif node_type_field: current_node_type_str = node_type_field[0] # Fallback to first type
        elif isinstance(node_type_field, str):
            current_node_type_str = node_type_field


        if "Dataset" in current_node_type_str or \
           "Sample" in current_node_type_str or \
           "Instrument" in current_node_type_str: # simplified check
            generated_by_info = node.get("generatedBy")
            if generated_by_info:
                if isinstance(generated_by_info, list) and generated_by_info:
                    comp_id = generated_by_info[0].get("@id")
                elif isinstance(generated_by_info, dict):
                    comp_id = generated_by_info.get("@id")
                else:
                    comp_id = None

                if comp_id:
                    result_node["generatedBy"] = self._build_graph_recursive(comp_id, collection, processed)
                elif generated_by_info : 
                    result_node["generatedBy"] = generated_by_info 

        elif "Computation" in current_node_type_str or \
             "Experiment" in current_node_type_str: # simplified check
            used_dataset_info = node.get("usedDataset")
            if used_dataset_info:
                if isinstance(used_dataset_info, list):
                    result_node["usedDataset"] = [self._build_graph_recursive(item.get("@id"), collection, processed) for item in used_dataset_info if item.get("@id")]
                elif isinstance(used_dataset_info, dict) and used_dataset_info.get("@id"):
                     result_node["usedDataset"] = [self._build_graph_recursive(used_dataset_info.get("@id"), collection, processed)]


            used_software_info = node.get("usedSoftware")
            if used_software_info:
                # Software usually isn't recursively built, just referenced
                if isinstance(used_software_info, list) and used_software_info:
                    result_node["usedSoftware"] = [{"@id": item.get("@id")} for item in used_software_info if item.get("@id")]
                elif isinstance(used_software_info, dict) and used_software_info.get("@id"):
                    result_node["usedSoftware"] = [{"@id": used_software_info.get("@id")}]


            used_sample_info = node.get("usedSample")
            if used_sample_info:
                if isinstance(used_sample_info, list):
                    result_node["usedSample"] = [self._build_graph_recursive(item.get("@id"), collection, processed) for item in used_sample_info if item.get("@id")]
                elif isinstance(used_sample_info, dict) and used_sample_info.get("@id"):
                    result_node["usedSample"] = [self._build_graph_recursive(used_sample_info.get("@id"), collection, processed)]


            used_instrument_info = node.get("usedInstrument")
            if used_instrument_info:
                if isinstance(used_instrument_info, list):
                    result_node["usedInstrument"] = [self._build_graph_recursive(item.get("@id"), collection, processed) for item in used_instrument_info if item.get("@id")]
                elif isinstance(used_instrument_info, dict) and used_instrument_info.get("@id"):
                    result_node["usedInstrument"] = [self._build_graph_recursive(used_instrument_info.get("@id"), collection, processed)]


        return result_node

    def build_graph(self, start_node_id: str, mongo_collection: pymongo.collection.Collection):
        """Builds the graph starting from node_id and stores it in self.graph."""
        processed_nodes = set()
        self.graph = {"@graph": [self._build_graph_recursive(start_node_id, mongo_collection, processed_nodes)]}


class EvidenceGraphCreate(BaseModel):
    guid: str = Field(alias="@id")
    description: str
    name: str = Field(default="Evidence Graph")

    class Config:
        populate_by_name = True


def list_evidence_graphs_from_db(mongo_collection: pymongo.collection.Collection) -> FairscapeResponse:
    try:
        cursor = mongo_collection.find({"@type": "evi:EvidenceGraph"}, {"_id": 0, "@id": 1, "name": 1, "@type": 1, "description": 1, "owner": 1})
        graphs = [EvidenceGraph.model_validate(graph_data) for graph_data in cursor]
        return FairscapeResponse(success=True, statusCode=200, model=graphs)
    except Exception as e:
        return FairscapeResponse(success=False, statusCode=500, error={"message": f"Error listing evidence graphs: {str(e)}"})
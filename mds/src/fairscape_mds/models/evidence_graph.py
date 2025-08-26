from pydantic import BaseModel, Field, ValidationError
from typing import Dict, List, Optional, Any, Union
import pymongo
import datetime
from fairscape_mds.crud.fairscape_response import FairscapeResponse


class EvidenceNode: 
   def __init__(self, id: str, type: str):
       self.id = id
       self.type = type
       self.usedSoftware: Optional[List[str]] = None
       self.usedDataset: Optional[List[str]] = None
       self.usedSample: Optional[List[str]] = None
       self.usedInstrument: Optional[List[str]] = None
       self.generatedBy: Optional[str] = None

class EvidenceGraph(BaseModel):
    metadataType: str = Field(default="evi:EvidenceGraph", alias="@type")
    guid: str = Field(alias="@id")
    owner: str 
    description: str
    name: str = Field(default="Evidence Graph")
    outputs: Optional[List[Dict[str, str]]] = Field(default=None)
    graph: Optional[Dict[str, Dict[str, Any]]] = Field(default=None, alias="@graph")

    class Config:
        extra = 'allow' 
        populate_by_name = True 

    def _flatten_metadata(self, node: Dict) -> Dict:
        if "metadata" not in node:
            return node

        flattened = {k: v for k, v in node.items() if k != "metadata"}
        metadata_content = node["metadata"]
        if isinstance(metadata_content, dict):
            for key, value in metadata_content.items():
                if key not in flattened:
                    flattened[key] = value
        return flattened

    def _is_rocrate(self, node_type_field: Any) -> bool:
        if isinstance(node_type_field, list):
            return any("ROCrate" in str(t) for t in node_type_field)
        elif isinstance(node_type_field, str):
            return "ROCrate" in node_type_field
        return False

    def _get_rocrate_outputs(self, node: Dict) -> List[Dict]:
        output_fields = ["https://w3id.org/EVI#outputs", "EVI:outputs", "outputs"]
        
        for field in output_fields:
            if field in node:
                outputs = node[field]
                if isinstance(outputs, list):
                    return outputs
                elif isinstance(outputs, dict):
                    return [outputs]
        return []

    def _process_used_dataset(self, dataset_info: Any, collection: pymongo.collection.Collection, graph_dict: Dict[str, Dict]) -> List[Dict[str, str]]:
        datasets_to_process = []
        
        if isinstance(dataset_info, list):
            datasets_to_process = dataset_info
        elif isinstance(dataset_info, dict):
            datasets_to_process = [dataset_info]
        else:
            return []
        
        result_refs = []
        
        for dataset_ref in datasets_to_process:
            if not dataset_ref.get("@id"):
                continue
                
            dataset_id = dataset_ref.get("@id")
            dataset_node = collection.find_one({"@id": dataset_id}, {"_id": 0})
            
            if dataset_node:
                dataset_node = self._flatten_metadata(dataset_node)
                node_type = dataset_node.get("@type", "")
                
                if self._is_rocrate(node_type):
                    outputs = self._get_rocrate_outputs(dataset_node)
                    if outputs:
                        for output_ref in outputs:
                            if output_ref.get("@id"):
                                self._add_node_to_graph(output_ref.get("@id"), collection, graph_dict)
                                result_refs.append({"@id": output_ref.get("@id")})
                    else:
                        self._add_node_to_graph(dataset_id, collection, graph_dict)
                        result_refs.append({"@id": dataset_id})
                else:
                    self._add_node_to_graph(dataset_id, collection, graph_dict)
                    result_refs.append({"@id": dataset_id})
            else:
                self._add_node_to_graph(dataset_id, collection, graph_dict)
                result_refs.append({"@id": dataset_id})
                
        return result_refs

    def _add_node_to_graph(self, node_id: str, collection: pymongo.collection.Collection, graph_dict: Dict[str, Dict]) -> None:
        if node_id in graph_dict:
            return

        node_data = collection.find_one({"@id": node_id}, {"_id": 0})
        if not node_data:
            graph_dict[node_id] = {"@id": node_id, "error": "not found"}
            return

        node = self._flatten_metadata(node_data)
        
        result_node = {
            "@id": node.get("@id"),
            "@type": node.get("@type"),
            "name": node.get("name"),
            "description": node.get("description")
        }

        graph_dict[node_id] = result_node

        node_type_field = node.get("@type", "")
        current_node_type_str = ""
        if isinstance(node_type_field, list):
            if "Dataset" in node_type_field: current_node_type_str = "Dataset"
            elif "Computation" in node_type_field: current_node_type_str = "Computation"
            elif "Sample" in node_type_field: current_node_type_str = "Sample"
            elif "Software" in node_type_field: current_node_type_str = "Software"
            elif "Experiment" in node_type_field: current_node_type_str = "Experiment"
            elif node_type_field: current_node_type_str = node_type_field[0]
        elif isinstance(node_type_field, str):
            current_node_type_str = node_type_field

        if "Dataset" in current_node_type_str or \
           "Sample" in current_node_type_str or \
           "Instrument" in current_node_type_str or \
           "Software" in current_node_type_str: 
            generated_by_info = node.get("generatedBy")
            if generated_by_info:
                if isinstance(generated_by_info, list) and generated_by_info:
                    comp_id = generated_by_info[0].get("@id")
                elif isinstance(generated_by_info, dict):
                    comp_id = generated_by_info.get("@id")
                else:
                    comp_id = None

                if comp_id:
                    self._add_node_to_graph(comp_id, collection, graph_dict)
                    result_node["generatedBy"] = {"@id": comp_id}
                elif generated_by_info:
                    result_node["generatedBy"] = generated_by_info

        elif "Computation" in current_node_type_str or \
             "Experiment" in current_node_type_str: 
            used_dataset_info = node.get("usedDataset")
            if used_dataset_info:
                dataset_refs = self._process_used_dataset(used_dataset_info, collection, graph_dict)
                if dataset_refs:
                    result_node["usedDataset"] = dataset_refs

            used_software_info = node.get("usedSoftware")
            if used_software_info:
                software_refs = []
                if isinstance(used_software_info, list):
                    for item in used_software_info:
                        if item.get("@id"):
                            self._add_node_to_graph(item.get("@id"), collection, graph_dict)
                            software_refs.append({"@id": item.get("@id")})
                elif isinstance(used_software_info, dict) and used_software_info.get("@id"):
                    self._add_node_to_graph(used_software_info.get("@id"), collection, graph_dict)
                    software_refs.append({"@id": used_software_info.get("@id")})
                if software_refs:
                    result_node["usedSoftware"] = software_refs

            used_sample_info = node.get("usedSample")
            if used_sample_info:
                sample_refs = []
                if isinstance(used_sample_info, list):
                    for item in used_sample_info:
                        if item.get("@id"):
                            self._add_node_to_graph(item.get("@id"), collection, graph_dict)
                            sample_refs.append({"@id": item.get("@id")})
                elif isinstance(used_sample_info, dict) and used_sample_info.get("@id"):
                    self._add_node_to_graph(used_sample_info.get("@id"), collection, graph_dict)
                    sample_refs.append({"@id": used_sample_info.get("@id")})
                if sample_refs:
                    result_node["usedSample"] = sample_refs

            used_instrument_info = node.get("usedInstrument")
            if used_instrument_info:
                instrument_refs = []
                if isinstance(used_instrument_info, list):
                    for item in used_instrument_info:
                        if item.get("@id"):
                            self._add_node_to_graph(item.get("@id"), collection, graph_dict)
                            instrument_refs.append({"@id": item.get("@id")})
                elif isinstance(used_instrument_info, dict) and used_instrument_info.get("@id"):
                    self._add_node_to_graph(used_instrument_info.get("@id"), collection, graph_dict)
                    instrument_refs.append({"@id": used_instrument_info.get("@id")})
                if instrument_refs:
                    result_node["usedInstrument"] = instrument_refs

        graph_dict[node_id] = result_node

    def build_graph(self, start_node_id: str, mongo_collection: pymongo.collection.Collection):
        graph_dict = {}
        output_nodes = []
        
        start_node = mongo_collection.find_one({"@id": start_node_id}, {"_id": 0})
        
        if start_node:
            start_node = self._flatten_metadata(start_node)
            node_type = start_node.get("@type", "")
            
            if self._is_rocrate(node_type):
                rocrate_outputs = self._get_rocrate_outputs(start_node)
                if rocrate_outputs:
                    for output_ref in rocrate_outputs:
                        if output_ref.get("@id"):
                            output_id = output_ref.get("@id")
                            output_nodes.append({"@id": output_id})
                            self._add_node_to_graph(output_id, mongo_collection, graph_dict)
                else:
                    output_nodes.append({"@id": start_node_id})
                    self._add_node_to_graph(start_node_id, mongo_collection, graph_dict)
            else:
                output_nodes.append({"@id": start_node_id})
                self._add_node_to_graph(start_node_id, mongo_collection, graph_dict)
        else:
            output_nodes.append({"@id": start_node_id})
            graph_dict[start_node_id] = {"@id": start_node_id, "error": "not found"}
        
        self.outputs = output_nodes
        self.graph = graph_dict


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
    
class EvidenceGraphBuildRequest(BaseModel):
    guid: str
    task_type: str = Field(default="EvidenceGraphBuild")
    owner_email: str
    naan: str
    postfix: str
    status: str = Field(default="PENDING")
    time_created: datetime.datetime = Field(default_factory=datetime.datetime.utcnow)
    time_started: Optional[datetime.datetime] = None
    time_finished: Optional[datetime.datetime] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[Dict[str, Any]] = None

    class Config:
        populate_by_name = True
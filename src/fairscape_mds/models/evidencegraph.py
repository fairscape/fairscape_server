from pydantic import BaseModel, Field, constr, Extra
from typing import Dict, List, Optional, Any, Union
from fairscape_mds.utilities.operation_status import OperationStatus
import pymongo
from bson import SON

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

class EvidenceGraph(BaseModel, extra=Extra.allow):
    metadataType: str = Field(default="evi:EvidenceGraph", alias="@type")
    guid: str = Field(alias="@id")
    owner: str
    description: str  
    name: str = Field(default="Evidence Graph")
    graph: Optional[Dict[str, Any]] = Field(default=None, alias="@graph")
    
    def build_graph(self, node_id: str, mongo_collection: pymongo.collection.Collection):
       processed = set()
       self.graph = self._build_graph_recursive(node_id, mongo_collection, processed)

    def _flatten_metadata(self, node: Dict) -> Dict:
        """Flatten metadata field if it exists, preserving top-level fields."""
        if "metadata" not in node:
            return node
            
        # Create a copy of the node without metadata
        flattened = {k: v for k, v in node.items() if k != "metadata"}
        
        # Update with metadata contents, preserving any existing top-level fields
        metadata = node["metadata"]
        for key, value in metadata.items():
            if key not in flattened:  # Don't overwrite existing top-level fields
                flattened[key] = value
                
        return flattened

    def _build_graph_recursive(self, node_id: str, collection: pymongo.collection.Collection, processed: set) -> Dict:
        if node_id in processed:
            return {"@id": node_id}
            
        node = collection.find_one({"@id": node_id}, {"_id": 0})
        print(f"node: {node}") 
        if not node:
            return {"@id": node_id}
        
        node = self._flatten_metadata(node)
        
            
        processed.add(node_id)
        result = self._build_base_node(node)
        
        node_type = node.get("@type", "")
        if "Dataset" in node_type:
            node_type = "Dataset"
        elif "Computation" in node_type:
            node_type = "Computation"
        elif "Sample" in node_type:
            node_type = "Sample"
        elif "Instrument" in node_type:
            node_type = "Instrument"
        elif "Experiment" in node_type:
            node_type = "Experiment"
        print(f"node_type: {node_type}")
    
        if node_type in ["Dataset", "Sample", "Instrument"]:
            if "generatedBy" in node:
                result["generatedBy"] = self._build_computation_node(node, collection, processed)
        elif node_type in ["Computation", "Experiment"]:
            if "usedDataset" in node:
                result["usedDataset"] = self._build_used_resources(node["usedDataset"], collection, processed)
            if "usedSoftware" in node:
                result["usedSoftware"] = self._build_software_reference(node["usedSoftware"], collection)
            if "usedSample" in node:
                result["usedSample"] = self._build_used_resources(node["usedSample"], collection, processed)
            if "usedInstrument" in node:
                result["usedInstrument"] = self._build_used_resources(node["usedInstrument"], collection, processed)
                
        return result

    def _build_base_node(self, node: Dict) -> Dict:
        return {
            "@id": node["@id"],
            "@type": node.get("@type"),
            "name": node.get("name"),
            "description": node.get("description")
        }

    def _build_computation_node(self, parent_node: Dict, collection: pymongo.collection.Collection, processed: set) -> Dict:
        comp_id = (parent_node["generatedBy"][0]["@id"] 
                    if isinstance(parent_node["generatedBy"], list) 
                    else parent_node["generatedBy"]["@id"])
        
        comp = collection.find_one({"@id": comp_id}, {"_id": 0})
        if not comp:
            return {"@id": comp_id}
        return self._build_graph_recursive(comp_id, collection, processed)

    def _build_used_resources(self, used_resources: Union[Dict, List], collection: pymongo.collection.Collection, processed: set) -> List:
        if isinstance(used_resources, dict):
            print(used_resources)
            return [self._build_graph_recursive(used_resources["@id"], collection, processed)]
        return [self._build_graph_recursive(resource["@id"], collection, processed) for resource in used_resources]

    def _build_software_reference(self, used_software: Union[Dict, List], collection: pymongo.collection.Collection) -> Dict:
        software_id = used_software[0]["@id"] if isinstance(used_software, list) else used_software["@id"]
        
        software = collection.find_one({"@id": software_id}, {"_id": 0})
        if software:
            return self._build_base_node(software)
        return {"@id": software_id}
    
    def create(self, mongo_collection: pymongo.collection.Collection) -> OperationStatus:
       if mongo_collection.find_one({"@id": self.guid}):
           return OperationStatus(False, "evidence graph already exists", 400)

       graph_dict = self.dict(by_alias=True)
       add_graph_update = {
           "$push": {
               "evidencegraphs": SON([
                   ("@id", self.guid),
                   ("@type", "evi:EvidenceGraph"), 
                   ("name", self.name)
               ])
           }
       }

       bulk_write = [
           pymongo.InsertOne(graph_dict),
           pymongo.UpdateOne({"@id": self.owner}, add_graph_update)
       ]

       try:
           result = mongo_collection.bulk_write(bulk_write)
           if result.inserted_count != 1:
               return OperationStatus(False, "error creating evidence graph", 500)
           return OperationStatus(True, "", 201)
       except pymongo.errors.BulkWriteError as e:
           return OperationStatus(False, f"bulk write error: {e}", 500)

    def read(self, mongo_collection: pymongo.collection.Collection) -> OperationStatus:
       return super().read(mongo_collection)

    def update(self, mongo_collection: pymongo.collection.Collection) -> OperationStatus:
       return super().update(mongo_collection)

    def delete(self, mongo_collection: pymongo.collection.Collection) -> OperationStatus:
       read_status = self.read(mongo_collection)
       if not read_status.success:
           return OperationStatus(False, "graph not found", 404)

       pull_op = {"$pull": {"evidencegraphs": {"@id": self.guid}}}
       bulk_edit = [
           pymongo.UpdateOne({"@id": self.owner}, pull_op),
           pymongo.DeleteOne({"@id": self.guid})
       ]

       try:
           result = mongo_collection.bulk_write(bulk_edit)
           if result.deleted_count == 1:
               return OperationStatus(True, "", 200)
           return OperationStatus(False, f"error: {result.bulk_api_result}", 500)
       except pymongo.errors.BulkWriteError as e:
           return OperationStatus(False, f"delete error: {e}", 500)

def list_evidence_graphs(mongo_collection: pymongo.collection.Collection):
   cursor = mongo_collection.find({"@type": "evi:EvidenceGraph"}, {"_id": 0})
   return {
       "evidencegraphs": [
           {
               "@id": graph.get("@id"),
               "@type": "evi:EvidenceGraph",
               "name": graph.get("name")
           } for graph in cursor
       ]
   }
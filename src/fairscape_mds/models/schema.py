from typing import (
        Optional,
        Dict
)
import re
from fairscape_mds.models.fairscape_base import *
from fairscape_mds.utilities.operation_status import OperationStatus

def validate_type(value):
    valid_types = {'integer', 'number', 'string', 'array','boolean'}
    if value is not None:
        if value not in valid_types:
            raise ValueError(f"Type must be one of {valid_types}")
    return value

class Item(BaseModel):
    type: str = Field(...)
    _validate_type = validator('type', allow_reuse=True)(validate_type)

class Property(BaseModel, extra = 'allow'):
    description: str = Field(...)
    index: Union[str, int] = Field(...)
    type: str = Field(...)
    value_url: Optional[str] = Field(default = None, alias = 'value-url')
    pattern: Optional[str] = Field(default = None)
    items: Optional[Item] = Field(default = None, alias = 'items')
    min_items: Optional[int] = Field(default = None, alias = 'min-items')
    max_items: Optional[int] = Field(default = None, alias = 'max-items')
    unique_items: Optional[bool] = Field(default = None, alias = 'unique-items')

    @validator('index')
    def validate_index(cls, value):
        if isinstance(value, str):
            # Allow something like int::int for index. Raise error if else
            pattern = r'^\d+$|^-?\d+::|^-?\d+::-?\d+$|^::-?\d+'
            if not re.match(pattern, value):
                raise ValueError("Index must match the pattern 'int::int'")
        return value

    _validate_type = validator('type', allow_reuse=True)(validate_type)

    @validator('pattern')
    def validate_pattern(cls, value):
        if value is not None:
            try:
                re.compile(value)
            except re.error:
                raise ValueError("Pattern must be a valid regular expression")
        return value

class Schema(FairscapeEVIBaseModel, extra='allow'):
    context: Dict[str, str] = Field( 
        default= {"@vocab": "https://schema.org/", "evi": "https://w3id.org/EVI#"},
        alias="@context" 
    )
    metadataType: str = Field(alias="@type", default= "evi:Schema")
    properties: Dict[str, Property]
    schemaType: Optional[str] = Field(default="object", alias="type")
    additionalProperties: Optional[bool] = Field(default=True)
    required: Optional[List[str]] = []  
    separator: Optional[str] = Field(default=",")
    header: Optional[bool] = Field(default=True)
    examples: Optional[List[Dict]] = []  

    def create(self, MongoCollection: pymongo.collection.Collection) -> OperationStatus:
        return super().create(MongoCollection)

    def read(self, MongoCollection: pymongo.collection.Collection) -> OperationStatus:
        return super().read(MongoCollection)

    def update(self, MongoCollection: pymongo.collection.Collection) -> OperationStatus:
        return super().update(MongoCollection)

    def delete(self, MongoCollection: pymongo.collection.Collection) -> OperationStatus:
        return super().delete(MongoCollection)

def list_schemas(mongo_collection: pymongo.collection.Collection):
    cursor = mongo_collection.find(
        filter = {"@type": {"$regex": "^evi:Schema$|^EVI:Schema$|^Schema$", "$options": "i"}},
        projection={"_id": False}
    )
    return {"schemas":  [{"@id": schema.get("@id"), "@type": "evi:Schema", "name": schema.get("name")} for schema in cursor] }


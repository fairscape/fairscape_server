from pydantic import (
    BaseModel, 
    validator,
    ConfigDict,
    Field,
    constr,
    BeforeValidator
)
from pydantic.networks import AnyUrl
from typing import (
    List,
    Optional,
    Dict,
    Union,
    Annotated
)
from typing_extensions import Annotated
import pymongo
from pymongo.collection import Collection
from fairscape_mds.utilities.utils import validate_ark
from fairscape_mds.utilities.operation_status import OperationStatus
from enum import Enum


IdentifierPattern = "^ark:[0-9]{5}\\/[a-zA-Z0-9_\\-]*.$"

# TODO get from config
DEFAULT_ARK_NAAN = "59852"
DEFAULT_LICENSE = "https://creativecommons.org/licenses/by/4.0/"
defaultContext = {
    "@vocab": "https://schema.org/",
    "evi": "https://w3id.org/EVI#",

    # TODO fully specify default context
    "usedSoftware": {
        "@id": "https://w3id.org/EVI#",
        "@type": "@id"
    },
    "usedDataset": {
        "@id": "https://w3id.org/EVI#",
        "@type": "@id"
    },
    "generatedBy": {
        "@id": "https://w3id.org/EVI#generated",
        "@type": "@id"
    },
    "generated": {
        "@id": "https://w3id.org/EVI#generatedBy",
        "@type": "@id"
    },
    "hasDistribution": {
        "@id": "https://w3id.org/EVI#hasDistribution",
        "@type": "@id"
    }
}

class ClassType(str, Enum):
    DATASET = 'Dataset'
    SOFTWARE = 'Software'
    COMPUTATION = 'Computation'
    SCHEMA = 'Schema'
    EVIDENCE_GRAPH = 'EvidenceGraph'
    ROCRATE = 'ROCrate'

def normalize_class_type(value: Union[str, ClassType]) -> ClassType:
    """Normalizes various formats of class type identifiers to standard form.
    
    Handles formats like:
    - Plain name: "ROCrate"
    - URL: "https://w3id.org/EVI#ROCrate"
    - Prefixed: "EVI:ROCrate"
    """
    if isinstance(value, ClassType):
        return value
        
    value_str = str(value).strip()
    
    # Handle URL format
    if value_str.startswith('https://'):
        value_str = value_str.split('#')[-1].split('/')[-1]
    
    # Handle prefixed format (e.g., EVI:ROCrate)
    if ':' in value_str:
        value_str = value_str.split(':')[-1]

    try:
        return ClassType(value_str)
    except ValueError:
        for enum_value in ClassType:
            if enum_value.value.lower() == value_str.lower():
                return enum_value
                
        raise ValueError(f"Invalid class type: {value_str}")
    
ValidatedClassType = Annotated[ClassType, BeforeValidator(normalize_class_type)]

class IdentifierValue(BaseModel):
    guid: str = Field(alias="@id")

class IdentifierPropertyValue(BaseModel):
    metadataType: str = Field(default="PropertyValue")
    value: str
    name: str


class Identifier(BaseModel):
    model_config = ConfigDict(extra='allow')
    guid: str = Field(
        title="guid",
        alias="@id"
    )
    metadataType: ValidatedClassType = Field(
        title="metadataType",
        alias="@type"
    )
    name: str = Field(...)


class FairscapeBaseModel(Identifier):
    """Refers to the Fairscape BaseModel inherited from Pydantic

    Args:
        BaseModel (Default Pydantic): Every instance of the Fairscape BaseModel must contain
        an id, a type, and a name
    """
    model_config = ConfigDict(
        populate_by_name=True,
        validate_assignment=True,
        extra='allow'
    )
    context: Optional[Dict[str, str]] = Field(
        default=defaultContext,
        title="context",
        alias="@context"
    )
    url: Optional[AnyUrl] = Field(default=None)

    def generate_guid(self) -> str:
        # TODO url encode values
        # TODO add random hash digest
        return f"ark:{DEFAULT_ARK_NAAN}/rocrate-{self.name.replace(' ', '')}"

    def create(self, MongoCollection: Collection, bson=None) -> OperationStatus:
        """Persist instance of model in mongo

        This is the superclass method for create operations in fairscape.
        It will check for collision of the submitted @id, then attempt to insert it into the collection.

        Args:
            MongoCollection (pymongo.collection.Collection): Collection which may contain instance of FairscapeBaseModel
            bson (None): A representation of the object using bson, allows for use of embedded document storage in Mongo

        Returns:
            OperationStatus: containing success, message, status code and error type (Default to None)
        """

        if bson is None:
            insert_document = self.dict(by_alias=True)
        else:
            insert_document = bson

        try:
            if MongoCollection.find_one({"@id": self.guid}):
                return OperationStatus(False, "document already exists", 400)

            create_request = MongoCollection.insert_one(insert_document)

            if create_request.acknowledged and create_request.inserted_id:
                return OperationStatus(True, "", 200)
            else:
                return OperationStatus(False, "", 400)

        # write specific exception handling
        except pymongo.errors.DocumentTooLarge as e:
            return OperationStatus(False, f"Mongo Document Too Large Error: {str(e)}", 500)

        except pymongo.errors.DuplicateKeyError as e:
            return OperationStatus(False, f"Mongo Duplicate Key Error: {str(e)}", 500)

        except pymongo.errors.WriteError as e:
            return OperationStatus(False, f"Mongo Write Error: {str(e)}", 500)

        # default exceptions for all mongo operations
        except pymongo.errors.CollectionInvalid as e:
            return OperationStatus(False, f"Mongo Connection Invalid Error: {str(e)}", 500)

        except pymongo.errors.ConnectionFailure as e:
            return OperationStatus(False, f"Mongo Connection Failure Error: {str(e)}", 500)

        except pymongo.errors.ExecutionTimeout as e:
            return OperationStatus(False, f"Mongo Execution Timeout Error: {str(e)}", 500)

        except pymongo.errors.InvalidName as e:
            return OperationStatus(False, f"Mongo Invalid Name Error: {str(e)}", 500)

        except pymongo.errors.NetworkTimeout as e:
            return OperationStatus(False, f"Mongo Network Timeout Error: {str(e)}", 500)

        except pymongo.errors.OperationFailure as e:
            return OperationStatus(False, f"Mongo Error Operation Failure: {str(e)}", 500)

        # catch all exceptions
        except Exception as e:
            return OperationStatus(False, f"Error: {str(e)}", 500)

    def read(self, MongoCollection: pymongo.collection.Collection, exclude: List[str] = None) -> OperationStatus:
        """Read an instance of a model in mongo and unpack the values into the current
        FairscapeBaseModel attributes

        This is the superclass method for read operations in fairscape.

        Args:
            MongoCollection (pymongo.collection.Collection): Collection which may contain instance of FairscapeBaseModel
            exclude (List[str], optional): a list of field names to exclude from the returned document. Defaults to None.

        Returns:
            OperationStatus: containing success, message, status code and error type (Default to None)
        """

        # given passed list of fields to exclude from query
        # form the projection argument to the find_one mongo command
        if exclude:
            query_projection = {excluded_field: False for excluded_field in exclude}
            query_projection['_id'] = False
        else:
            query_projection = {'_id': False}

        try:
            # run the query
            query = MongoCollection.find_one(
                {'@id': self.guid},
                projection=query_projection
            )

            # check that the results are no empty
            if query:
                # If you use the model_construct it creates a new model without validating
                # then after every setattr it trys to validate and fails
                # so you need to update all at once which is what I changed it to
                # update class with values from database
                updated_instance = self.model_validate({**self.model_dump(), **query})
                self.__dict__.update(updated_instance.model_dump())
                return OperationStatus(True, "", 200)
            else:
                return OperationStatus(False, "No record found", 404)

        # default exceptions for all mongo operations
        except pymongo.errors.CollectionInvalid as e:
            return OperationStatus(False, f"Mongo Connection Invalid: {str(e)}", 500)

        except pymongo.errors.ConnectionFailure as e:
            return OperationStatus(False, f"Mongo Connection Failure: {str(e)}", 500)

        except pymongo.errors.ExecutionTimeout as e:
            return OperationStatus(False, f"Mongo Execution Timeout: {str(e)}", 500)

        except pymongo.errors.InvalidName as e:
            return OperationStatus(False, f"Mongo Error Invalid Name: {str(e)}", 500)

        except pymongo.errors.NetworkTimeout as e:
            return OperationStatus(False, f"Mongo Error Network Timeout: {str(e)}", 500)

        except pymongo.errors.OperationFailure as e:
            return OperationStatus(False, f"Mongo Error Operation Failure: {str(e)}", 500)

        # catch all exceptions
        except Exception as e:
            return OperationStatus(False, f"Error: {str(e)}", 500)

    def update(self, MongoCollection: pymongo.collection.Collection) -> OperationStatus:
        """Update an instance of a model in mongo

        This is the superclass method for update operations in fairscape.
        It will check that a document with @id exists in the database before updating
        It will check the set properties and then preform the mongo replace operation.
        This is equivalent to an overwrite of the passed data properties.

        Args:
            MongoCollection (pymongo.collection.Collection): Collection which may contain instance of FairscapeBaseModel

        Returns:
            OperationStatus: containing success, message, status code and error type (Default to None)
        """

        try:
            new_values = {
                "$set":
                    {k: value for k, value in self.dict(by_alias=True).items() if value is not None}
            }

            update_result = MongoCollection.update_one({"@id": self.guid}, new_values)

            if update_result.acknowledged and update_result.modified_count == 1:
                return OperationStatus(True, "", 200)

            if update_result.matched_count == 0:
                return OperationStatus(False, "object not found", 404)

            else:
                return OperationStatus(False, "", 500)

        # update-specific mongo exceptions
        except pymongo.errors.DocumentTooLarge as e:
            return OperationStatus(False, f"Mongo Error Document Too Large: {str(e)}", 500)

        except pymongo.errors.DuplicateKeyError as e:
            return OperationStatus(False, f"Mongo Duplicate Key Error: {str(e)}", 500)

        except pymongo.errors.WriteError as e:
            return OperationStatus(False, f"Mongo Write Error: {str(e)}", 500)

        # default exceptions for all mongo operations
        except pymongo.errors.CollectionInvalid as e:
            return OperationStatus(False, f"Mongo Connection Invalid: {str(e)}", 500)

        except pymongo.errors.ConnectionFailure as e:
            return OperationStatus(False, f"Mongo Connection Failure: {str(e)}", 500)

        except pymongo.errors.ExecutionTimeout as e:
            return OperationStatus(False, f"Mongo Execution Timeout: {str(e)}", 500)

        except pymongo.errors.InvalidName as e:
            return OperationStatus(False, f"Mongo Error Invalid Name: {str(e)}", 500)

        except pymongo.errors.NetworkTimeout as e:
            return OperationStatus(False, f"Mongo Error Network Timeout: {str(e)}", 500)

        except pymongo.errors.OperationFailure as e:
            return OperationStatus(False, f"Mongo Error Operation Failure: {str(e)}", 500)

        # catch all exceptions
        except Exception as e:
            return OperationStatus(False, f"Error: {str(e)}", 500)

    def delete(self, MongoCollection: pymongo.collection.Collection) -> OperationStatus:
        """Delete an instance of a model in mongo

        This is the superclass method for delete operations in fairscape.
        It will check that a document with @id exists in the database before deleting

        Args:
            MongoCollection (pymongo.collection.Collection): Collection which may contain instance of FairscapeBaseModel

        Returns:
            OperationStatus: containing success, message, status code and error type (Default to None)
        """

        try:
            # make sure the object exists, return 404 otherwise
            if MongoCollection.find_one({"@id": self.guid}) is None:
                return OperationStatus(False, "Object not found", 404)

            # perform delete one operation
            delete_result = MongoCollection.delete_one({"@id": self.guid})

            # if deletion is successful, return success message
            if delete_result.acknowledged and delete_result.deleted_count == 1:
                return OperationStatus(True, "", 200)

            else:
                return OperationStatus(False, f"delete error: str({delete_result})", 404)

        # default exceptions for all mongo operations
        except pymongo.errors.CollectionInvalid as e:
            return OperationStatus(False, f"Mongo Connection Invalid: {str(e)}", 500)

        except pymongo.errors.ConnectionFailure as e:
            return OperationStatus(False, f"Mongo Connection Failure: {str(e)}", 500)

        except pymongo.errors.ExecutionTimeout as e:
            return OperationStatus(False, f"Mongo Execution Timeout: {str(e)}", 500)

        except pymongo.errors.InvalidName as e:
            return OperationStatus(False, f"Mongo Error Invalid Name: {str(e)}", 500)

        except pymongo.errors.NetworkTimeout as e:
            return OperationStatus(False, f"Mongo Error Network Timeout: {str(e)}", 500)

        except pymongo.errors.OperationFailure as e:
            return OperationStatus(False, f"Mongo Error Operation Failure: {str(e)}", 500)

        # catch all exceptions
        except Exception as e:
            return OperationStatus(False, f"Error: {str(e)}", 500)


    def update_append(self, MongoCollection, Field: str, Item) -> OperationStatus:
        # TODO read update result output to determine success
        update_result = MongoCollection.update_one(
            {"@id": self.guid},
            {"$addToSet": {Field: Item}}
        )

        return OperationStatus(True, "", 200)


    def update_remove(self, MongoCollection, field: str, item_id: str) -> OperationStatus:
        """
        update_remove

        Updates a document removing an element from a list where the item matches a member on the field '@id'

        Parameters
        - self: FairscapeBaseClass
        - MongoCollection
        - Field: str
        - Item
        """

        # TODO read update result output to determine success
        update_result = MongoCollection.update_one(
            {"@id": self.guid},
            {"$pull": {field:  {"@id": item_id} }}
        )

        return OperationStatus(True, "", 200)



class FairscapeEVIBaseModel(FairscapeBaseModel):
    description: str = Field(min_length=5)
    workLicense: Optional[str] = Field(default=DEFAULT_LICENSE, alias="license")
    keywords: List[str] = Field(default=[])
    published: bool = Field(default=True)


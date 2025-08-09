from fairscape_models.schema import Schema
from fairscape_mds.models.user import Permissions
from pydantic import BaseModel, Field

class SchemaWriteModel(Schema):
	permissions: Permissions
	published: bool = Field(default=True)
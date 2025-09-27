from fairscape_models.software import Software
from fairscape_mds.models.user import Permissions
from pydantic import BaseModel, Field
from typing import Optional

class SoftwareWriteModel(Software):
	permissions: Permissions
	published: Optional[bool] = Field(default=True)


class SoftwareUpdateModel(BaseModel):
	name: Optional[str]
	description: Optional[str]

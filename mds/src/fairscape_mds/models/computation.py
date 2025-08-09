from fairscape_mds.models.user import Permissions
from fairscape_models.computation import Computation
from pydantic import BaseModel, Field
from typing import Optional

class ComputationWriteModel(Computation):
	permissions: Permissions
	published: Optional[bool] = Field(default=True)
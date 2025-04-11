# pydantic model for Distributions
from pydantic import BaseModel, Field
from typing import Union, Optional, List
from fairscape_models import (
	ROCrateV1_2,
	Dataset,
	Software,
	Computation,
	ROCrateMetadataElem, 
	GenericMetadataElem,
)
from fairscape_mds.models.schema import Schema

class FairscapeDataDistribution(BaseModel):
	distributionType: str = 'minio'
	objectPath: str
	objectBucket: str


class MongoDocument(BaseModel):
	guid: str = Field(alias="@id")
	metadataType: str = Field(alias="@type")
	owner: str
	metadata: Union[
		ROCrateV1_2, 
		ROCrateMetadataElem, 
		Dataset, 
		Software, 
		Computation, 
		Schema,
		GenericMetadataElem
		]
	distribution: Optional[FairscapeDataDistribution] = Field(default=None)

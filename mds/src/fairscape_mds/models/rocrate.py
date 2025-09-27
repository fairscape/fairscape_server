from fairscape_models.rocrate import ROCrateV1_2, ROCrateMetadataElem
from fairscape_mds.models.user import Permissions
from fairscape_mds.models.dataset import DatasetDistribution

from pydantic import Field, BaseModel
from typing import Optional, List, Union
import datetime


class ROCrateUploadRequest(BaseModel):
	""" Pydantic Model for ROCrate Upload Request

	Created when an ROCrate Zip is uploaded and processed
	"""
	guid: str
	permissions: Permissions
	uploadPath: str
	rocrateGUID: Optional[str] = Field(default=None)
	timeStarted: Optional[datetime.datetime] = Field(default_factory=datetime.datetime.now)
	timeFinished: Optional[datetime.datetime] = Field(default=None)
	completed: Optional[bool] = Field(default=False)
	error: Optional[str] = Field(default=None)
	identifiersMinted: Optional[Union[int, List[str]]] = Field(default=None)
	rocrateIdentifier: Optional[str] = Field(default=None)
	transactionFolder: Optional[str] = Field(default=None)
	status: Optional[str] = Field(default=None)
	stage: Optional[str] = Field(default=None)
	success: Optional[bool] = Field(default=False)


class ROCrateMetadataElemWrite(ROCrateMetadataElem):
	permissions: Permissions
	published: Optional[bool] = Field(default=True)
	hasPart: Optional[List[dict]]
	distribution: Optional[DatasetDistribution]
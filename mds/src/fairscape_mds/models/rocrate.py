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
	error: Optional[Union[str, list]] = Field(default=None)
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


class ContentSummaryItem(BaseModel):
	"""Minimal info for a single element in the summary"""
	guid: str = Field(alias="@id")
	name: str
	metadataType: Optional[str] = Field(default=None, alias="@type")


class ContentCounts(BaseModel):
	"""Counts of each element type"""
	datasets: int = 0
	software: int = 0
	computations: int = 0
	schemas: int = 0
	samples: int = 0
	mlModels: int = 0
	rocrates: int = 0
	other: int = 0
	total: int = 0


class ROCrateContentSummary(BaseModel):
	"""Pre-computed summary of RO-Crate contents"""
	datasets: List[ContentSummaryItem] = []
	software: List[ContentSummaryItem] = []
	computations: List[ContentSummaryItem] = []
	schemas: List[ContentSummaryItem] = []
	samples: List[ContentSummaryItem] = []
	mlModels: List[ContentSummaryItem] = []
	rocrates: List[ContentSummaryItem] = []
	other: List[ContentSummaryItem] = []
	counts: ContentCounts = ContentCounts()
	generatedAt: datetime.datetime = Field(default_factory=datetime.datetime.now)
from pydantic import BaseModel, Field, ConfigDict
from fairscape_models.dataset import Dataset
from fairscape_mds.models.user import Permissions
from enum import Enum
from typing import Union, Optional, List
import datetime


class DatasetCreateModel(Dataset):
	guid: Optional[str] = Field(
		title="guid",
		alias="@id",
		default=None
	)
	dateRegistered: Optional[datetime.datetime] = Field(default_factory=datetime.datetime.now)

class DistributionTypeEnum(str, Enum):
	MINIO = 'minio'
	URL = 'url'
	GLOBUS = 'globus'
	FTP = 'ftp'

class MinioDistribution(BaseModel):
	path: str

class URLDistribution(BaseModel):
	uri: str

class DatasetDistribution(BaseModel):
	distributionType: DistributionTypeEnum
	location: Union[MinioDistribution, URLDistribution]


class DatasetWriteModel(DatasetCreateModel):
	published: Optional[bool] = Field(default=True)
	distribution: Optional[DatasetDistribution] = Field(default=None)
	permissions: Permissions


class DatasetSetProperties(BaseModel):
	name: Optional[str] = Field(default=None)
	description: Optional[str] = Field(default=None)
	author: Optional[Union[str, List[str]]] = Field(default=None)
	version: Optional[str] = Field(default=None)
	keywords: Optional[List[str]] = Field(default=None)
	fileFormat: Optional[str] = Field(default=None)
	sameAs: Optional[List[str]] = Field(default=None)
	additionalDocumentation: Optional[str] = Field(default=None)


class DatasetPushProperties(BaseModel):
	associatedPublication: Optional[Union[str, List[str]]] = Field(default=None)
	usedByComputation: Optional[Union[str, List[str]]] = Field(default=None)
	derivedFrom: Optional[Union[str, List[str]]] = Field(default=None)
	generatedBy: Optional[Union[str, List[str]]] = Field(default=None)
	contentUrl: Optional[Union[str, List[str]]] = Field(default=None)


class DatasetUpdateModel(BaseModel):
	guid: str = Field(alias="@id")
	set: Optional[DatasetSetProperties] = Field(default=None)
	push: Optional[DatasetPushProperties] = Field(default=None)

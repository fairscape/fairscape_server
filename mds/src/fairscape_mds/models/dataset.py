from pydantic import BaseModel, Field
from fairscape_models.dataset import Dataset
from fairscape_mds.models.user import Permissions
from enum import Enum
from typing import Union, Optional
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
from pydantic import BaseModel, Field
from typing import Optional, Union
from fairscape_mds.models.dataset import Dataset
from fairscape_mds.models.software import Software
from fairscape_mds.models.user import Permissions

from enum import Enum

class PublicationStatusEnum(Enum):
	DRAFT = "DRAFT"
	PUBLISHED = "PUBLISHED"
	EMBARGOED = "EMBARGOED"
	ARCHIVED = "ARCHIVED"


class MetadataTypeEnum(Enum):
	DATASET = "https://w3id.org/EVI#Dataset"
	SOFTWARE ="https://w3id.org/EVI#Software" 
	COMPUTATION ="https://w3id.org/EVI#Computation" 
	SCHEMA ="https://w3id.org/EVI#SCHEMA" 
	BIOCHEMENTITY = "https://schema.org/BioChemEntity"
	ROCRATE = ""



class StoredIdentifier(BaseModel):
	guid: str = Field(alias="@id")
	metadataType: MetadataTypeEnum
	metadata: Union[Dataset, Software, ROCrateV1_2]
	publicationStatus: PublicationStatusEnum
	permissions: Permissions
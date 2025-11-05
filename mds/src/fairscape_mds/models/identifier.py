from pydantic import BaseModel, Field
from typing import Optional, Union
from fairscape_mds.models.user import Permissions
from fairscape_mds.models.dataset import DatasetDistribution

from fairscape_models.rocrate import ROCrateV1_2, ROCrateMetadataElem, GenericMetadataElem
from fairscape_models.dataset import Dataset
from fairscape_models.software import Software
from fairscape_models.computation import Computation
from fairscape_models.schema import Schema
from fairscape_models.sample import Sample
from fairscape_models.biochem_entity import BioChemEntity
from fairscape_models.experiment import Experiment
from fairscape_models.instrument import Instrument
from fairscape_models.medical_condition import MedicalCondition

import datetime


from enum import Enum

class PublicationStatusEnum(str, Enum):
	DRAFT = "DRAFT"
	PUBLISHED = "PUBLISHED"
	EMBARGOED = "EMBARGOED"
	ARCHIVED = "ARCHIVED"

	def __repr__(self):
		return self.value


class MetadataTypeEnum(Enum):
	DATASET = "https://w3id.org/EVI#Dataset"
	SOFTWARE ="https://w3id.org/EVI#Software" 
	COMPUTATION ="https://w3id.org/EVI#Computation" 
	SCHEMA ="https://w3id.org/EVI#Schema" 
	ROCRATE = ["https://w3id.org/EVI#Dataset", "https://w3id.org/EVI#ROCrate"]
	SAMPLE = "https://w3id.org/EVI#Sample"
	BIOCHEM_ENTITY = "https://schema.org/BioChemEntity"
	EXPERIMENT = "https://w3id.org/EVI#Experiment"
	INSTRUMENT = "https://w3id.org/EVI#Instrument"
	MEDICAL_CONDITION = "https://schema.org/MedicalCondition"
	CREATIVE_WORK = "https://schema.org/CreativeWork"
	ML_MODEL = "https://w3id.org/EVI#MLModel"


MetadataUnion = Union[
	Dataset, 
	Software, 
	Computation, 
	ROCrateV1_2, 
	ROCrateMetadataElem,
	Schema, 
	Sample,
	BioChemEntity,
	Experiment,
	Instrument,
	MedicalCondition,
	GenericMetadataElem
	]

class StoredIdentifier(BaseModel):
	guid: str = Field(alias="@id")
	metadataType: MetadataTypeEnum = Field(alias="@type")
	metadata: MetadataUnion
	publicationStatus: PublicationStatusEnum
	permissions: Permissions
	distribution: Optional[DatasetDistribution]
	dateCreated: datetime.datetime
	dateModified: datetime.datetime


class UpdatePublishRequest(BaseModel):
	guid: str = Field(alias="@id")
	publicationStatus: PublicationStatusEnum


def determineMetadataType(inputType)->MetadataTypeEnum:
	# TODO future proof for more list types
	if isinstance(inputType, list):
		return MetadataTypeEnum.ROCRATE
	elif 'Dataset' in inputType: 
		return MetadataTypeEnum.DATASET
	elif 'Software' in inputType:
		return MetadataTypeEnum.SOFTWARE
	elif 'Computation' in inputType:
		return MetadataTypeEnum.COMPUTATION
	elif 'Schema' in inputType:
		return MetadataTypeEnum.SCHEMA
	elif 'BioChemEntity' in inputType:
		return MetadataTypeEnum.BIOCHEM_ENTITY
	elif 'Sample' in inputType:
		return MetadataTypeEnum.SAMPLE
	elif 'Experiment' in inputType:
		return MetadataTypeEnum.EXPERIMENT	
	elif 'Instrument' in inputType:
		return MetadataTypeEnum.INSTRUMENT
	elif 'MedicalCondition' in inputType:
		return MetadataTypeEnum.MEDICAL_CONDITION
	elif 'CreativeWork' in inputType:
		return MetadataTypeEnum.CREATIVE_WORK
	elif 'MLModel' in inputType:
		return MetadataTypeEnum.ML_MODEL
	else:
		raise Exception(f"Type not found for value {inputType}")
	

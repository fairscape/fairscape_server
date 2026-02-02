from pydantic import BaseModel, Field, ConfigDict, model_validator
from typing import Optional, Union, Dict, TYPE_CHECKING, List
from fairscape_mds.models.user import Permissions
from fairscape_mds.models.dataset import DatasetDistribution
from fairscape_mds.models.statistics import DescriptiveStatistics
from fairscape_mds.models.evidence_graph import EvidenceGraph

from fairscape_models.rocrate import ROCrateV1_2, ROCrateMetadataElem, GenericMetadataElem

if TYPE_CHECKING:
	from fairscape_mds.models.rocrate import ROCrateContentSummary
from fairscape_models.dataset import Dataset
from fairscape_models.software import Software
from fairscape_models.computation import Computation
from fairscape_models.schema import Schema
from fairscape_models.sample import Sample
from fairscape_models.biochem_entity import BioChemEntity
from fairscape_models.experiment import Experiment
from fairscape_models.instrument import Instrument
from fairscape_models.medical_condition import MedicalCondition
from fairscape_models.annotation import Annotation
from fairscape_models.conversion.models.AIReady import AIReadyScore
from fairscape_models.model_card import ModelCard
from fairscape_models.fairscape_base import IdentifierValue

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
	DATASET = ["prov:Entity","https://w3id.org/EVI#Dataset"]
	SOFTWARE = ["prov:Entity","https://w3id.org/EVI#Software"]
	COMPUTATION =["prov:Activity","https://w3id.org/EVI#Computation"]
	SCHEMA ="https://w3id.org/EVI#Schema"
	ROCRATE = ["https://w3id.org/EVI#Dataset", "https://w3id.org/EVI#ROCrate"]
	SAMPLE = ["prov:Entity","https://w3id.org/EVI#Sample"]
	BIOCHEM_ENTITY = ["prov:Entity","https://w3id.org/EVI#BioChemEntity"]
	EXPERIMENT = ["prov:Activity","https://w3id.org/EVI#Experiment"]
	INSTRUMENT = ["prov:Entity","https://w3id.org/EVI#Instrument"]
	MEDICAL_CONDITION = "https://schema.org/MedicalCondition"
	CREATIVE_WORK = "https://schema.org/CreativeWork"
	ML_MODEL = ["prov:Entity","https://w3id.org/EVI#MLModel"]
	ANNOTATION = "https://w3id.org/EVI#Annotation"
	EVIDENCE_GRAPH = "evi:EvidenceGraph"
	AI_READY_SCORE = "evi:AIReadyScore"


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
	Annotation,
	EvidenceGraph,
	AIReadyScore,
	ModelCard,
	GenericMetadataElem
	]

class StoredIdentifier(BaseModel):
	model_config = ConfigDict(populate_by_name=True)

	guid: str = Field(alias="@id")
	metadataType: MetadataTypeEnum = Field(alias="@type")
	metadata: MetadataUnion
	publicationStatus: PublicationStatusEnum
	permissions: Permissions
	distribution: Optional[DatasetDistribution]
	descriptiveStatistics: Optional[Dict[str, DescriptiveStatistics]] = Field(default = {})
	contentSummary: Optional[Dict] = Field(default=None)
	dateCreated: datetime.datetime
	dateModified: datetime.datetime
	isPartOf: Optional[List[IdentifierValue]] = None

	@model_validator(mode='before')
	@classmethod
	def validate_metadata_type(cls, data):
		"""Ensure metadata is validated against the correct type based on metadataType field"""
		if isinstance(data, dict):
			metadata_type = data.get('@type') or data.get('metadataType')
			metadata_dict = data.get('metadata')

			if metadata_dict and isinstance(metadata_dict, dict):
				type_map = {
					'evi:EvidenceGraph': EvidenceGraph,
					MetadataTypeEnum.EVIDENCE_GRAPH: EvidenceGraph,
					'evi:AIReadyScore': AIReadyScore,
					MetadataTypeEnum.AI_READY_SCORE: AIReadyScore,
				}

				if metadata_type in type_map:
					model_class = type_map[metadata_type]
					data['metadata'] = model_class.model_validate(metadata_dict)

		return data


class UpdatePublishRequest(BaseModel):
	guid: str = Field(alias="@id")
	publicationStatus: PublicationStatusEnum


def determineMetadataType(inputType)->MetadataTypeEnum:
	# ASSUMES LAST TYPE OF LIST IS OUR CLASSISFER
	if isinstance(inputType, list):
		inputType = inputType[-1]
	if 'ROCrate' in inputType:
		return MetadataTypeEnum.ROCRATE
	elif 'Dataset' in inputType:
		return MetadataTypeEnum.DATASET
	elif 'Software' in inputType:
		return MetadataTypeEnum.SOFTWARE
	elif 'Computation' in inputType:
		return MetadataTypeEnum.COMPUTATION
	elif 'EvidenceGraph' in inputType:
		return MetadataTypeEnum.EVIDENCE_GRAPH
	elif 'AIReadyScore' in inputType:
		return MetadataTypeEnum.AI_READY_SCORE
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
	elif 'Annotation' in inputType:
		return MetadataTypeEnum.ANNOTATION
	else:
		raise Exception(f"Type not found for value {inputType}")
	

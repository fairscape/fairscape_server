from fairscape_mds.models.identifier import MetadataTypeEnum, determineMetadataType
from fairscape_models.rocrate import ROCrateV1_2
from fairscape_models.experiment import Experiment
from fairscape_models.instrument import Instrument
from fairscape_models.sample import Sample
from fairscape_models import (
	Dataset,
	Software,
	Schema,
	Computation,
	BioChemEntity,
	MedicalCondition
)
from zipfile import ZipFile
import json
import logging

testLogger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def initROCrateMetadata(inputData):
	""" Given input JSON serialize into the pydantic model
	"""

	metadataGraph = []
	errors = []

	# metadataGraph
	for element in inputData.get("@graph"):
		try:
			elementMetadataType = determineMetadataType(element.get("@type"))
		except Exception as e:
			errors.append({
				"metadata": element,
				"errorType": "UnknownType",
				"exception": e
			})
			continue
		
		match elementMetadataType:

			case MetadataTypeEnum.DATASET:
				testLogger.info(f"Found Dataset: {element.get('@id')}")
				elementInstance = Dataset.model_validate(element)
				metadataGraph.append(elementInstance)

			case MetadataTypeEnum.CREATIVE_WORK:
				# TODO pass for ro-crate-metadata.json
				pass

			case MetadataTypeEnum.SOFTWARE:
				elementInstance = Software.model_validate(element)
				metadataGraph.append(elementInstance)

			case MetadataTypeEnum.COMPUTATION:
				elementInstance = Computation.model_validate(element)
				metadataGraph.append(elementInstance)

			case MetadataTypeEnum.SCHEMA:
				elementInstance = Schema.model_validate(element)	
				metadataGraph.append(elementInstance)

			case MetadataTypeEnum.BIOCHEM_ENTITY:
				elementInstance = BioChemEntity.model_validate(element)
				metadataGraph.append(elementInstance)

			case MetadataTypeEnum.EXPERIMENT:
				elementInstance = Experiment.model_validate(element)
				metadataGraph.append(elementInstance)

			case MetadataTypeEnum.INSTRUMENT:
				elementInstance = Instrument.model_validate(element)
				metadataGraph.append(elementInstance)

			case MetadataTypeEnum.MEDICAL_CONDITION:
				elementInstance = MedicalCondition.model_validate(element)
				metadataGraph.append(elementInstance)

			case MetadataTypeEnum.SAMPLE:
				elementInstance = Sample.model_validate(element)
				metadataGraph.append(elementInstance)


	# TODO check errors
	
	# TODO create rocrate metadata
	rocrate = ROCrateV1_2.model_validate({
		"@context": inputData.get("@context"),
		"@graph": metadataGraph 
	})

	return rocrate



def test_rocrate_metadata(caplog):

	caplog.set_level(logging.INFO, logger=__name__)
	test_zip = "tests/data/Example.zip"
	with ZipFile(test_zip, 'r') as zip_obj:
		# read the content 
		rocratePath = "Example/ro-crate-metadata.json"
		with zip_obj.open(rocratePath, 'r') as metadataFile:
			metadataContent = json.loads(metadataFile.read())

	rocrateInstance = initROCrateMetadata(metadataContent)

	assert rocrateInstance is not None
	assert isinstance(rocrateInstance, ROCrateV1_2)




def example():

	rocrateInstance = ROCrateV1_2.model_validate(metadataContent)

	crateMetadataInstance = rocrateInstance.getCrateMetadata()

	testLogger.info("Found ROCrate Metadata:\n" + crateMetadataInstance.model_dump_json())

	# for element in rocrate graph
	for element in rocrateInstance.metadataGraph:
		testLogger.info(f"Found Element: {type(element)} Value: {element}")


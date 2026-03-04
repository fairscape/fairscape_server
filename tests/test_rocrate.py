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
    MedicalCondition,
)
from zipfile import ZipFile
import json
import logging

testLogger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


_TYPE_MODEL_MAP = {
    MetadataTypeEnum.DATASET: Dataset,
    MetadataTypeEnum.SOFTWARE: Software,
    MetadataTypeEnum.COMPUTATION: Computation,
    MetadataTypeEnum.SCHEMA: Schema,
    MetadataTypeEnum.BIOCHEM_ENTITY: BioChemEntity,
    MetadataTypeEnum.EXPERIMENT: Experiment,
    MetadataTypeEnum.INSTRUMENT: Instrument,
    MetadataTypeEnum.MEDICAL_CONDITION: MedicalCondition,
    MetadataTypeEnum.SAMPLE: Sample,
}


def initROCrateMetadata(input_data):
    """Given input JSON, serialize into the pydantic model."""
    metadata_graph = []
    errors = []

    for element in input_data.get("@graph"):
        try:
            element_type = determineMetadataType(element.get("@type"))
        except Exception as e:
            errors.append({"metadata": element, "errorType": "UnknownType", "exception": e})
            continue

        if element_type == MetadataTypeEnum.CREATIVE_WORK:
            # skip ro-crate-metadata.json descriptor
            continue

        model_cls = _TYPE_MODEL_MAP.get(element_type)
        if model_cls is None:
            continue

        testLogger.info(f"Found {element_type.value}: {element.get('@id')}")
        metadata_graph.append(model_cls.model_validate(element))

    return ROCrateV1_2.model_validate({
        "@context": input_data.get("@context"),
        "@graph": metadata_graph,
    })


def test_rocrate_metadata(caplog):
    caplog.set_level(logging.INFO, logger=__name__)

    with ZipFile("tests/data/Example.zip", "r") as zip_obj:
        with zip_obj.open("Example/ro-crate-metadata.json", "r") as metadata_file:
            metadata_content = json.loads(metadata_file.read())

    rocrate_instance = initROCrateMetadata(metadata_content)

    assert rocrate_instance is not None
    assert isinstance(rocrate_instance, ROCrateV1_2)

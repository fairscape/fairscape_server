import pytest
import datetime
from fairscape_mds.core.config import appConfig
from fairscape_mds.models.identifier import StoredIdentifier
from fairscape_mds.crud.identifier import (
    IdentifierRequest,
    MetadataTypeEnum,
    PublicationStatusEnum,
)
from fairscape_mds.tests.crud.utils import load_test_data
from fairscape_models.dataset import Dataset


@pytest.fixture(scope="module")
def dataset_instance(current_user):
    dataset_metadata = load_test_data("dataset_content.json")
    instance = Dataset.model_validate(dataset_metadata)

    appConfig.identifierCollection.delete_many({"@id": instance.guid})

    stored = StoredIdentifier.model_validate({
        "@id": instance.guid,
        "@type": MetadataTypeEnum.DATASET,
        "metadata": instance.model_dump(by_alias=True, mode="json"),
        "permissions": current_user.getPermissions(),
        "distribution": None,
        "publicationStatus": PublicationStatusEnum.DRAFT,
        "dateCreated": datetime.datetime.now(),
        "dateModified": datetime.datetime.now(),
    })
    appConfig.identifierCollection.insert_one(stored.model_dump(by_alias=True, mode="json"))
    return instance


@pytest.fixture(scope="module")
def identifier_request():
    return IdentifierRequest(appConfig)


def test_update_identifier(identifier_request, current_user, dataset_instance):
    dataset_instance.name = "new name"

    response = identifier_request.updateMetadata(
        guid=dataset_instance.guid,
        user=current_user,
        newMetadata=dataset_instance,
    )

    assert response.success
    assert response.statusCode == 200
    assert isinstance(response.model, StoredIdentifier)
    assert response.model

import pytest
import httpx
import time
import logging
import zipfile
import pathlib
import hashlib
from fairscape_models.model_card import ModelCard

testLogger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)

root_url = "http://localhost:8080/api"


@pytest.fixture
def root_url_fixture():
    return root_url


def calculate_sha256(filepath):
    sha256 = hashlib.sha256()
    try:
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256.update(chunk)
        return sha256.hexdigest()
    except FileNotFoundError:
        return None


def login(username, password):
    response = httpx.post(root_url + "/login", data={"username": username, "password": password})
    assert response.status_code == 200
    token = response.json()["access_token"]
    return {"Authorization": f"Bearer {token}"}


def poll_upload_status(submission_uuid, auth_headers, timeout=60):
    """Poll upload status until complete or timeout (seconds)."""
    waited = 0
    while waited < timeout:
        response = httpx.get(
            root_url + f"/rocrate/upload/status/{submission_uuid}",
            headers=auth_headers,
        )
        assert response.status_code == 200
        status = response.json()
        if status.get("completed"):
            return status
        time.sleep(5)
        waited += 5
    return status


@pytest.mark.skip(reason="skipping to test ml model")
def test_upload_rocrate(caplog):
    rocrate_path = "tests/data/Example.zip"
    rocrate_name = pathlib.Path(rocrate_path).name

    caplog.set_level(logging.INFO, logger=__name__)

    auth_headers = login("test@example.org", "test")
    testLogger.info("Login Success")

    upload_response = httpx.post(
        root_url + "/rocrate/upload-async",
        files={"crate": (rocrate_name, open(rocrate_path, "rb"), "application/zip")},
        headers=auth_headers,
    )
    assert upload_response.status_code == 200
    upload_json = upload_response.json()
    assert upload_json
    testLogger.info("Upload Success")

    submission_uuid = upload_json["guid"]
    assert submission_uuid

    status_json = poll_upload_status(submission_uuid, auth_headers, timeout=60)
    testLogger.info("Check Upload Status Success")

    assert status_json.get("completed")
    assert not status_json.get("error")

    rocrate_guid = status_json["rocrateGUID"]
    assert rocrate_guid
    testLogger.info(f"Found ROCrate GUID: {rocrate_guid}")

    resolve_url = root_url + f"/{rocrate_guid}"
    resolve_response = httpx.get(resolve_url, headers=auth_headers)
    rocrate_identifier = resolve_response.json()
    assert rocrate_identifier
    testLogger.info(f"Resolved ROCrate GUID: {rocrate_guid}")

    metadata_graph = rocrate_identifier.get("metadata", {}).get("hasPart", [])
    assert metadata_graph

    for elem in metadata_graph:
        guid = elem.get("@id")
        if guid == "ro-crate-metadata.json":
            continue
        testLogger.info(f"Resolving ROCrate Member GUID: {guid}")
        resolve = httpx.get(root_url + f"/{guid}", headers=auth_headers)
        assert resolve.status_code == 200
        assert resolve.json()

    testLogger.info(f"Downloading ROCrate File: {rocrate_guid}")
    download_response = httpx.get(
        root_url + f"/rocrate/download/{rocrate_guid}",
        headers=auth_headers,
    )
    assert download_response.status_code == 200

    with open("/tmp/Example.zip", "wb") as f:
        f.write(download_response.content)

    with zipfile.ZipFile("/tmp/Example.zip", "r") as crate_zip:
        crate_zip.extractall("/tmp/ExampleCrate")

    example_crate = pathlib.Path("/tmp/ExampleCrate/Example")
    assert (example_crate / "Demo PreMo_export_2025-05-06.csv").exists()
    assert (example_crate / "ro-crate-metadata.json").exists()

    dataset_guid = "ark:59852/dataset-example-data export-55c2016c"
    testLogger.info(f"Downloading Dataset: {dataset_guid}")
    with httpx.stream("GET", root_url + f"/dataset/download/{dataset_guid}", headers=auth_headers) as resp:
        resp.raise_for_status()
        with open("/tmp/dataset.csv", "wb") as f:
            for chunk in resp.iter_bytes():
                f.write(chunk)

    # Re-login and test publication
    auth_headers = login("test@example.org", "test")

    for guid, expected_status in [
        (dataset_guid, "PUBLISHED"),
        (rocrate_guid, "PUBLISHED"),
    ]:
        update_response = httpx.put(
            root_url + "/publish",
            json={"@id": guid, "publicationStatus": expected_status},
            headers=auth_headers,
        )
        assert update_response.status_code == 200
        update_json = update_response.json()
        assert update_json["@id"] == guid
        assert update_json["publicationStatus"] == expected_status

    # Download published dataset
    dataset_download_path = "/tmp/dataset_download.csv"
    with httpx.stream("GET", root_url + f"/download/{dataset_guid}") as resp:
        resp.raise_for_status()
        with open(dataset_download_path, "wb") as f:
            for chunk in resp.iter_bytes():
                f.write(chunk)
    assert pathlib.Path(dataset_download_path).exists()

    # Download published rocrate
    with httpx.stream("GET", root_url + f"/download/{rocrate_guid}") as resp:
        resp.raise_for_status()
        with open("/tmp/rocrate_download.zip", "wb") as f:
            for chunk in resp.iter_bytes():
                f.write(chunk)

    # Reset dataset to DRAFT
    httpx.put(
        root_url + "/publish",
        json={"@id": dataset_guid, "publicationStatus": "DRAFT"},
        headers=auth_headers,
    )


def test_upload_mlmodel(caplog):
    caplog.set_level(logging.INFO, logger=__name__)

    auth_headers = login("test@example.org", "test")
    testLogger.info("Login Success")

    model_data = {
        "@id": "ark:59853/test-model",
        "@type": "EVI:MLModel",
        "name": "example models",
        "description": "a fake ml model card for testing",
        "author": "Max Levinson",
        "keywords": ["test", "example"],
        "version": "0.1.0",
        "modelType": "Image Classification/Feature Backbone",
        "framework": "Pytorch",
        "modelFormat": "safetensor",
        "generatedBy": {"@id": "ark:59853/training-computation"},
        "trainingDataset": "https://huggingface.co/datasets/ILSVRC/imagenet-1k",
        "parameters": "8000000",
        "inputSize": "224x224",
        "indendedUseCase": None,
        "usageInformation": None,
        "contentUrl": "https://huggingface.co/timm/densenet121.tv_in1k",
        "url": "https://huggingface.co/timm/densenet121.tv_in1k",
        "dataLicense": None,
        "citation": "Densely Connected Convolutional Networks: https://arxiv.org/abs/1608.06993",
    }

    model_instance = ModelCard.model_validate(model_data)
    upload_response = httpx.post(
        root_url + "/mlmodel",
        json=model_instance.model_dump(by_alias=True, mode="json"),
        headers=auth_headers,
        timeout=30,
    )
    assert upload_response.status_code == 200
    assert upload_response.json()

    # Upload with content file
    model_data_2 = {**model_data, "@id": "ark:59853/test-model-2"}
    model_instance_2 = ModelCard.model_validate(model_data_2)

    with open("data/example.csv", "rb") as content_file:
        upload_response_2 = httpx.post(
            root_url + "/mlmodel",
            data={"metadata": model_instance_2.model_dump_json(by_alias=True)},
            files=[("content", content_file)],
            headers=auth_headers,
            timeout=30,
        )
    assert upload_response_2.status_code == 200
    assert upload_response_2.json()

import pytest
import httpx
import logging
import pathlib
import time

root_url = "http://localhost:8080"

testLogger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)


testROCratePaths = [
    "/mnt/e/Work/Data/Uploads/upload_06_29_2025/vorinostat.zip",
]


@pytest.mark.parametrize("rocrate_path", testROCratePaths)
def test_upload(rocrate_path, caplog):
    caplog.set_level(logging.INFO, logger=__name__)

    rocrate_name = pathlib.Path(rocrate_path).name

    login_response = httpx.post(
        root_url + "/login",
        data={"username": "test@example.org", "password": "test"},
    )
    assert login_response.status_code == 200
    login_json = login_response.json()
    assert login_json.get("access_token")
    testLogger.info("Success: Logged In User")

    auth_headers = {"Authorization": f"Bearer {login_json['access_token']}"}

    upload_response = httpx.post(
        root_url + "/rocrate/upload-async",
        files={"crate": (rocrate_name, open(rocrate_path, "rb"), "application/zip")},
        headers=auth_headers,
        timeout=600,
    )
    assert upload_response.status_code == 200
    upload_json = upload_response.json()
    assert upload_json

    submission_uuid = upload_json["guid"]

    status_response = httpx.get(
        root_url + f"/rocrate/upload/status/{submission_uuid}",
        headers=auth_headers,
        timeout=60,
    )
    assert status_response.status_code == 200
    status_json = status_response.json()
    assert status_json

    waited = 0
    while not status_json.get("completed") and waited < 600:
        testLogger.info("Awaiting Job Completion")
        time.sleep(5)
        waited += 5

        status_response = httpx.get(
            root_url + f"/rocrate/upload/status/{submission_uuid}",
            headers=auth_headers,
        )
        assert status_response.status_code == 200
        status_json = status_response.json()

    testLogger.info("Check Upload Status Success")

    rocrate_guid = status_json.get("rocrateIdentifier")

    update_response = httpx.put(
        root_url + "/publish",
        headers=auth_headers,
        json={"@id": rocrate_guid, "publicationStatus": "PUBLISHED"},
        timeout=600,
    )
    assert update_response.status_code == 200

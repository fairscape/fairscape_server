import os
import sys

srcPath = os.path.abspath('../mds/src/')
sys.path.insert(0, srcPath)

from fastapi.testclient import TestClient
from fairscape_mds.main import app

testApp = TestClient(app)


def test_read_main():
	response = testApp.get("/")
	assert response.status_code == 200
VERSION = RELEASE.2026-06-09.v1
IMAGE = ghcr.io/fairscape/mds_python

run:
	cd src/ && python -m fairscape_mds

run-docker: 
	docker compose up --build -d

run-local:
	# need to import environment variables
	# source deploy/local.env

	# run all backend services
	# docker compose up --build -d ldap mongo minio redis fairscape-worker
	
	# run server in current session
	cd src/ && python -m fairscape_mds

setup: requirements.txt
	pip install -r requirements.txt

clean:
	rm -rf __pycache__

build:
	docker build --no-cache -f Dockerfile -t $(IMAGE):$(VERSION) .

build-local:
	cd .. && docker build --no-cache -f fairscape_server/Dockerfile.local -t $(IMAGE):$(VERSION) .

push:
	docker push ghcr.io/fairscape/mds_python:${VERSION}

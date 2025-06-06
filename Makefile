VERSION = RELEASE.2025-06-06.v4

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
	docker build --no-cache -t ghcr.io/fairscape/mds_python:${VERSION} .

push:
	docker push ghcr.io/fairscape/mds_python:${VERSION}

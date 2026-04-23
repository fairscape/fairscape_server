#!/bin/bash

VERSION=v1

# Generate date in YYYY-MM-DD format
DATE=$(date '+%Y-%m-%d')

# Build the Docker image
sudo docker build \
  --no-cache \
  -f Dockerfile \
  -t ghcr.io/fairscape/mds_python:RELEASE.${DATE}.${VERSION} .

sudo docker push ghcr.io/fairscape/mds_python:RELEASE.${DATE}.${VERSION}

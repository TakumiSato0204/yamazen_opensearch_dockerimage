#!/bin/bash

set -ex

REGISTRY_URI="007465388569.dkr.ecr.ap-northeast-1.amazonaws.com"
CONTAINER_NAME="genbato-cms-ecr-lambda/search-query"
IMAGE_TAG="search-5.0"
REPOSITORY_URI=${REGISTRY_URI}/${CONTAINER_NAME}
aws ecr get-login-password --region ap-northeast-1 | docker login --username AWS --password-stdin 007465388569.dkr.ecr.ap-northeast-1.amazonaws.com
docker build -t $CONTAINER_NAME .
docker tag ${CONTAINER_NAME} ${REPOSITORY_URI}:${IMAGE_TAG}
docker push ${REPOSITORY_URI}:${IMAGE_TAG}
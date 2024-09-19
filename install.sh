#!/bin/bash

# Check if Docker is installed
if ! command -v docker &> /dev/null
then
    echo "Docker is not installed. Please install Docker and try again."
    exit 1
fi

# If Docker is installed, run the docker build command
echo "Docker is installed. Proceeding with the build."

# Customize the path to Dockerfile and image tag as needed
DOCKERFILE_PATH="./templates/containers/nosograph_assemblers/Dockerfile"
BUILD_DIR="./templates/containers/nosograph_assemblers"
IMAGE_NAME="nosograph-assemblers"
OUTPUT_PATH="./templates/containers/build"

[ -d ${OUTPUT_PATH} ] || mkdir ${OUTPUT_PATH}

docker build -t ${IMAGE_NAME} -f ${DOCKERFILE_PATH} ${BUILD_DIR}

# Check if the build succeeded
if [ $? -eq 0 ]; then
    echo "Docker image built successfully: ${IMAGE_NAME}"
else
    echo "Docker build failed."
    exit 1
fi

docker save ${IMAGE_NAME} -o ${OUTPUT_PATH}/${IMAGE_NAME}.tar
DIND_CONTAINER_NAME=dind-service
[ -d ./certs  ] || mkdir ./certs

docker compose build
docker compose up -d
docker cp ${OUTPUT_PATH}/${IMAGE_NAME}.tar ${DIND_CONTAINER_NAME}:/${IMAGE_NAME}.tar
docker exec ${DIND_CONTAINER_NAME} docker load -i /${IMAGE_NAME}.tar
docker exec ${DIND_CONTAINER_NAME} docker images

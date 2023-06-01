#!/bin/bash

# Check if version is supplied
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 version"
    exit 1
fi

VERSION=$1
GIT_TAG="N.$VERSION"
BASE_IMAGES=("arweave-base:18.04" "arweave-base:20.04" "arweave-base:22.04")
LINUX_VERSIONS=("ubuntu18" "ubuntu20" "ubuntu22")
BASE_DOCKERFILES=("Dockerfile.base.ubuntu18.04" "Dockerfile.base.ubuntu20.04" "Dockerfile.base.ubuntu22.04")

# Build base images first
for i in "${!BASE_DOCKERFILES[@]}"; do
    BASE_DOCKERFILE=${BASE_DOCKERFILES[$i]}
    BASE_IMAGE=${BASE_IMAGES[$i]}

    echo "Building base image $BASE_IMAGE..."

    # Build the base Docker image
    docker build -f $BASE_DOCKERFILE -t $BASE_IMAGE .
done

for i in "${!BASE_IMAGES[@]}"; do
    BASE_IMAGE=${BASE_IMAGES[$i]}
    LINUX_VERSION=${LINUX_VERSIONS[$i]}
    IMAGE_NAME="arweave:$VERSION-$LINUX_VERSION"
    OUTPUT_FILE="./output/arweave-$VERSION.$LINUX_VERSION-x86_64.tar.gz"

    DOCKERFILE="Dockerfile.ubuntu"
    if [[ $BASE_IMAGE == "centos:9" ]]; then
        DOCKERFILE="Dockerfile.centos"
    fi

    echo "Building $IMAGE_NAME..."

    # Build the Docker image
    docker build -f $DOCKERFILE --build-arg BASE_IMAGE=$BASE_IMAGE --build-arg GIT_TAG=$GIT_TAG -t $IMAGE_NAME .

    echo "Running $IMAGE_NAME..."

    # Run the Docker container
    docker run -v $(pwd)/output:/output $IMAGE_NAME

    echo "Renaming output file..."

    # Rename the output file
    mv "./output/arweave-$VERSION.tar.gz" "$OUTPUT_FILE"
done

cp "./output/arweave-$VERSION.ubuntu22-x86_64.tar.gz" "./output/arweave-$VERSION.linux-x86_64.tar.gz"
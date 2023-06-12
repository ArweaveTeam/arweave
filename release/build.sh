#!/bin/bash

ECHO_ONLY=0
PRE_RELEASE=0
BRANCH=""

# Parse flags
while getopts 'eb:' flag; do
  case "${flag}" in
    e) ECHO_ONLY=1 ;;
    b) PRE_RELEASE=1
       BRANCH="${OPTARG}" ;;
    *) echo "Usage: $0 [-e] [-b <pre-release branch>] version" 
       exit 1 ;;
  esac
done
shift $((OPTIND-1))

# Check if version is supplied
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 [-e] [-b <pre-release branch>] version"
    exit 1
fi

VERSION=$1
if [ $PRE_RELEASE -eq 1 ]; then
  GIT_TAG="$BRANCH"
else
  GIT_TAG="N.$VERSION"
fi

BASE_IMAGES=("arweave-base:18.04" "arweave-base:20.04" "arweave-base:22.04")
LINUX_VERSIONS=("ubuntu18" "ubuntu20" "ubuntu22")
BASE_DOCKERFILES=("Dockerfile.base.ubuntu18.04" "Dockerfile.base.ubuntu20.04" "Dockerfile.base.ubuntu22.04")

# Limit the build to only ubuntu-22.04 for pre-release
if [ $PRE_RELEASE -eq 1 ]; then
  BASE_IMAGES=("arweave-base:22.04")
  LINUX_VERSIONS=("ubuntu22")
  BASE_DOCKERFILES=("Dockerfile.base.ubuntu22.04")
fi

# Function to execute a command, optionally just echoing it
function run_cmd {
  if [ $ECHO_ONLY -eq 1 ]; then
    echo $1
  else
    eval $1
  fi
}

# Build base images first
for i in "${!BASE_DOCKERFILES[@]}"; do
    BASE_DOCKERFILE=${BASE_DOCKERFILES[$i]}
    BASE_IMAGE=${BASE_IMAGES[$i]}

    echo "Building base image $BASE_IMAGE..."

    # Build the base Docker image
    run_cmd "docker build -f $BASE_DOCKERFILE -t $BASE_IMAGE ."
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
    run_cmd "docker build -f $DOCKERFILE --build-arg BASE_IMAGE=$BASE_IMAGE --build-arg GIT_TAG=$GIT_TAG -t $IMAGE_NAME ."

    echo "Running $IMAGE_NAME..."

    # Run the Docker container
    run_cmd "docker run --rm -v $(pwd)/output:/output $IMAGE_NAME"

    echo "Renaming output file..."

    # Rename the output file
    run_cmd "mv './output/arweave.tar.gz' '$OUTPUT_FILE'"
done

if [ $PRE_RELEASE -eq 0 ]; then
  run_cmd "cp './output/arweave-$VERSION.ubuntu22-x86_64.tar.gz' './output/arweave-$VERSION.linux-x86_64.tar.gz'"
fi

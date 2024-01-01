#!/bin/bash

ECHO_ONLY=0
PRE_RELEASE=0
BRANCH=""
LINUX_VERSION=""

# Parse flags
while getopts 'eb:l:' flag; do
  case "${flag}" in
    e) ECHO_ONLY=1 ;;
    b) PRE_RELEASE=1
       BRANCH="${OPTARG}" ;;
    l) LINUX_VERSION="${OPTARG}" ;;
    *) echo "Usage: $0 [-e] [-b <pre-release branch>] [-l <linux version>] version" 
       exit 1 ;;
  esac
done
shift $((OPTIND-1))

# Check if version is supplied
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 [-e] [-b <pre-release branch>] [-l <linux version>] version"
    exit 1
fi

VERSION=$1
if [ $PRE_RELEASE -eq 1 ]; then
  GIT_TAG="$BRANCH"
else
  GIT_TAG="N.$VERSION"
fi

BASE_IMAGES=("arweave-base:18.04" "arweave-base:20.04" "arweave-base:22.04" "")
LINUX_VERSIONS=("ubuntu18" "ubuntu20" "ubuntu22" "rocky9")
BASE_DOCKERFILES=("Dockerfile.base.ubuntu18.04" "Dockerfile.base.ubuntu20.04" "Dockerfile.base.ubuntu22.04" "")

# If specific Linux version is supplied, filter the arrays
if [ ! -z "$LINUX_VERSION" ]; then
  for i in "${!LINUX_VERSIONS[@]}"; do
    if [ "${LINUX_VERSIONS[$i]}" = "$LINUX_VERSION" ]; then
      BASE_IMAGES=("${BASE_IMAGES[$i]}")
      LINUX_VERSIONS=("${LINUX_VERSIONS[$i]}")
      BASE_DOCKERFILES=("${BASE_DOCKERFILES[$i]}")
      break
    fi
  done
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

    if [ ! -z "$BASE_DOCKERFILE" ] && [ ! -z "$BASE_IMAGE" ]; then
      echo "Building base image $BASE_IMAGE..."

      # Build the base Docker image
      run_cmd "docker build -f $BASE_DOCKERFILE -t $BASE_IMAGE ."
    fi
done

for i in "${!LINUX_VERSIONS[@]}"; do
    LINUX_VERSION=${LINUX_VERSIONS[$i]}
    IMAGE_NAME="arweave:$VERSION-$LINUX_VERSION"
    OUTPUT_FILE="./output/arweave-$VERSION.$LINUX_VERSION-x86_64.tar.gz"

    DOCKERFILE="Dockerfile.ubuntu"
    if [ "$LINUX_VERSION" == "rocky9" ]; then
        DOCKERFILE="Dockerfile.rocky"
    fi

    echo "Building $IMAGE_NAME..."

    if [ ! -z "${BASE_IMAGES[$i]}" ]; then
        # Build the Docker image
        run_cmd "docker build -f $DOCKERFILE --build-arg BASE_IMAGE=${BASE_IMAGES[$i]} -t $IMAGE_NAME ."
    else
        run_cmd "docker build -f $DOCKERFILE -t $IMAGE_NAME ."
    fi

    echo "Running $IMAGE_NAME..."

    # Run the Docker container
    run_cmd "docker run --rm -e GIT_TAG=$GIT_TAG -v $(pwd)/output:/output $IMAGE_NAME"

    echo "Renaming output file..."

    # Rename the output file
    run_cmd "mv './output/arweave.tar.gz' '$OUTPUT_FILE'"
done

if [ $PRE_RELEASE -eq 0 ]; then
  run_cmd "cp './output/arweave-$VERSION.ubuntu22-x86_64.tar.gz' './output/arweave-$VERSION.linux-x86_64.tar.gz'"
fi

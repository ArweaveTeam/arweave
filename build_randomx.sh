#!/bin/bash

# Check if a parameter is provided
if [ -z "$1" ]; then
  echo "Usage: $0 {512|4096|squared}"
  exit 1
fi

# Set the build directory and cmake options based on the parameter
case "$1" in
  512)
    BUILD_DIR="build512"
    CMAKE_OPTIONS="-DUSE_HIDDEN_VISIBILITY=ON \
                   -DRANDOMX_ARGON_MEMORY=262144 \
                   -DRANDOMX_DATASET_BASE_SIZE=536870912"
    ;;
  4096)
    BUILD_DIR="build4096"
    CMAKE_OPTIONS="-DUSE_HIDDEN_VISIBILITY=ON \
                   -DRANDOMX_ARGON_MEMORY=524288 \
                   -DRANDOMX_DATASET_BASE_SIZE=4294967296"
    ;;
  squared)
    BUILD_DIR="buildsquared"
    CMAKE_OPTIONS="-DUSE_HIDDEN_VISIBILITY=ON \
                   -DRANDOMX_ARGON_MEMORY=524288 \
                   -DRANDOMX_DATASET_BASE_SIZE=2147483648 \
                   -DRANDOMX_SCRATCHPAD_L1=2097152 \
                   -DRANDOMX_SCRATCHPAD_L2=2097152 \
                   -DRANDOMX_SCRATCHPAD_L3=2097152"
    ;;
  *)
    echo "Invalid option: $1. Use 512, 4096, or squared."
    exit 1
    ;;
esac

# Create the build directory and run cmake with the appropriate options
mkdir -p "apps/arweave/lib/RandomX/$BUILD_DIR"
cd "apps/arweave/lib/RandomX/$BUILD_DIR"
cmake $CMAKE_OPTIONS ..
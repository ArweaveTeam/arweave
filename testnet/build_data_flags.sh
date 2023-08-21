#!/bin/bash

ARWEAVE_DIR="$(readlink -f "$(dirname "$0")")/.."

if ! $ARWEAVE_DIR/testnet/assert_testnet.sh; then
	echo "Error: This script must be run on a testnet server."
	exit 1
fi

flags=""
address=""

# look in /arweave-data to determine this node's mining address and storage modules
for dir in /arweave-data/storage_modules/storage_module_*; do
	# get the storage_module directory names
	filename=$(basename $dir)

	# remove the leading storage_module_
	prefix="storage_module_"
	str="${filename#$prefix}"

    # Extract size, index, and address
    size=$(awk -F'_' '{print $1}' <<< "$str")
	index=$(awk -F'_' '{print $2}' <<< "$str")
	prefix="${size}_${index}_"
	address="${str#$prefix}"
    
    # Append to the screen command
    flags="$flags storage_module $index,$size,$address"
done

echo "$flags mining_addr $address"
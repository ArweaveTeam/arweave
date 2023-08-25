#!/bin/bash

ARWEAVE_DIR="$(readlink -f "$(dirname "$0")")/.."

if ! $ARWEAVE_DIR/testnet/assert_testnet.sh; then
	echo "Error: This script must be run on a testnet server."
	exit 1
fi

peers="$2"
prefix="$1"
flags=""

# Read the testnet servers and build the peer flags
while IFS= read -r line || [[ -n "$line" ]]; do
    flags+=" $prefix $line"
done < $ARWEAVE_DIR/testnet/$peers.txt

echo "$flags"

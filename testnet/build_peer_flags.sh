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
source $ARWEAVE_DIR/testnet/testnet_vdf.sh
for server in "${TESTNET_SERVERS[@]}"; do
	flags+=" $prefix $server"
done

echo "$flags"

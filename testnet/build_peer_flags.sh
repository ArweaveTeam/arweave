#!/bin/bash

ARWEAVE_DIR="$(cd "$(dirname "$0")/.." && pwd)"

if ! $ARWEAVE_DIR/testnet/assert_testnet.sh; then
	echo "Error: This script must be run on a testnet server."
	exit 1
fi


prefix="$1"
shift
flags=""

# Read the testnet nodes and build the peer flags
for node in "$@"; do
	if [ "$node" != "$(hostname)" ]; then
		flags+=" $prefix $node.arweave.net"
	fi
done

echo "$flags"

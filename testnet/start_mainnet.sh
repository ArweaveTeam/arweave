#!/bin/bash

ARWEAVE_DIR="$(cd "$(dirname "$0")/.." && pwd)"

if ! $ARWEAVE_DIR/testnet/assert_testnet.sh; then
	echo "Error: This script must be run on a testnet server."
	exit 1
fi

if [[ ! -f "/arweave-build/mainnet/bin/start" ]]; then
    echo "Arweave start script not found. Please run rebuild_mainnet.sh first."
	exit 1
fi

config_file="$ARWEAVE_DIR/testnet/config/$(hostname -f).json"
SCREEN_CMD="screen -dmsL arweave /arweave-build/mainnet/bin/start config_file $config_file"

echo "$SCREEN_CMD"
echo "$SCREEN_CMD" > /arweave-build/mainnet/run.sh
chmod +x /arweave-build/mainnet/run.sh

cd /arweave-build/mainnet
./run.sh
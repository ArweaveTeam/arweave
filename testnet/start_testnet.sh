#!/bin/bash

# Function to display help
display_help() {
    echo "Usage: $0 [<extra flags>]"
    echo "   <extra flags>: start_from_block <block> or start_from_latest_state is required when "
    echo "                  launching the pilot node with the start_from_block flag."
}

ARWEAVE_DIR="$(cd "$(dirname "$0")/.." && pwd)"

if ! $ARWEAVE_DIR/testnet/assert_testnet.sh; then
	echo "Error: This script must be run on a testnet server."
	exit 1
fi

if [[ ! -f "/arweave-build/testnet/bin/start" ]]; then
    echo "Arweave start script not found. Please run rebuild_testnet.sh first."
	exit 1
fi

node=$(hostname -f)
config_file="$ARWEAVE_DIR/testnet/config/$(hostname -f).json"
SCREEN_CMD="screen -dmsL arweave /arweave-build/testnet/bin/start config_file $config_file $*"

echo "$SCREEN_CMD"
echo "$SCREEN_CMD" > /arweave-build/testnet/run.sh
chmod +x /arweave-build/testnet/run.sh

cd /arweave-build/testnet
./run.sh

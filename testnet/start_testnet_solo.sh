#!/bin/bash

ARWEAVE_DIR="$(readlink -f "$(dirname "$0")")/.."

if ! $ARWEAVE_DIR/testnet/assert_testnet.sh; then
	echo "Error: This script must be run on a testnet server."
	exit 1
fi

if [[ ! -f "/arweave-build/testnet/bin/start" ]]; then
    echo "Arweave start script not found. Please run rebuild_testnet.sh first."
	exit 1
fi

screen_cmd="screen -dmsL arweave /arweave-build/testnet/bin/start"
screen_cmd+=$($ARWEAVE_DIR/testnet/build_data_flags.sh)
screen_cmd+=$($ARWEAVE_DIR/testnet/build_peer_flags.sh peer testnet_client)
screen_cmd+=$($ARWEAVE_DIR/testnet/build_peer_flags.sh peer testnet_pilot)

screen_cmd+=" debug mine \
max_vdf_validation_thread_count 2 enable remove_orphaned_storage_module_data \
enable pack_served_chunks data_dir /arweave-data"

echo "$screen_cmd"
echo "$screen_cmd" > /arweave-build/testnet/run.command

cd /arweave-build/testnet

eval "$screen_cmd"

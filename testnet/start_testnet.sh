#!/bin/bash

# Function to display help
display_help() {
    echo "Usage: $0 [<block>]"
    echo "   <block>: is required when launching the pilot node with the start_from_block flag."
}

ARWEAVE_DIR="$(readlink -f "$(dirname "$0")")/.."

if ! $ARWEAVE_DIR/testnet/assert_testnet.sh; then
	echo "Error: This script must be run on a testnet server."
	exit 1
fi

if [[ ! -f "/arweave-build/testnet/bin/start" ]]; then
    echo "Arweave start script not found. Please run rebuild_testnet.sh first."
	exit 1
fi

node=$(hostname -f)
# If an argument is provided, start from that block in local state, otherwise start from peers
if [ "$#" -gt 0 ]; then
    block=$1
fi

source $ARWEAVE_DIR/testnet/testnet_nodes.sh

screen_cmd="screen -dmsL arweave /arweave-build/testnet/bin/start"
screen_cmd+=$($ARWEAVE_DIR/testnet/build_data_flags.sh)


if is_node_in_array "$node" "${VDF_SERVER_NODES[@]}"; then
    # VDF Server and Pilot node flags
    screen_cmd+=$($ARWEAVE_DIR/testnet/build_peer_flags.sh vdf_client_peer "${VDF_CLIENT_NODES[@]}")
    if [ -z "$block" ]; then
        screen_cmd+=$($ARWEAVE_DIR/testnet/build_peer_flags.sh peer "${ALL_NODES[@]}")
    else
        screen_cmd+=" header_sync_jobs 0 start_from_block $block"
    fi
else
    screen_cmd+=$($ARWEAVE_DIR/testnet/build_peer_flags.sh peer "${ALL_NODES[@]}")
    if is_node_in_array "$node" "${VDF_CLIENT_NODES[@]}"; then
        # VDF Client node flags
        screen_cmd+=$($ARWEAVE_DIR/testnet/build_peer_flags.sh vdf_server_trusted_peer \
            "${VDF_SERVER_NODES[@]}")
    fi
fi

if is_node_in_array "$node" "${CM_EXIT_NODE[@]}"; then
    screen_cmd+=" coordinated_mining coordinated_mining_secret testnet_cm_secret"
    screen_cmd+=$($ARWEAVE_DIR/testnet/build_peer_flags.sh cm_peer "${CM_MINER_NODES[@]}")
elif is_node_in_array "$node" "${CM_MINER_NODES[@]}"; then
    screen_cmd+=" coordinated_mining coordinated_mining_secret testnet_cm_secret"
    screen_cmd+=$($ARWEAVE_DIR/testnet/build_peer_flags.sh cm_exit_peer "${CM_EXIT_NODE[@]}")
    screen_cmd+=$($ARWEAVE_DIR/testnet/build_peer_flags.sh cm_peer "${CM_MINER_NODES[@]}")
fi



screen_cmd+=" debug mine enable remove_orphaned_storage_module_data \
enable pack_served_chunks data_dir /arweave-data"

echo "$screen_cmd"
echo "$screen_cmd" > /arweave-build/testnet/run.command

cd /arweave-build/testnet

eval "$screen_cmd"

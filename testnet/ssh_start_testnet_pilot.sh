#!/bin/bash

# Function to display help
display_help() {
    echo "Usage: $0 <branch-name> [<block>]"
    echo "   branch-name: Name of the git branch."
    echo "   <block>: is required when launching the pilot node with the start_from_block flag."
}

# Check for required arguments
if [ -z "$1" ]; then
    display_help
    exit 1
fi

branch="$1"
# If an argument is provided, start from that block in local state, otherwise start from peers
if [ "$#" -gt 1 ]; then
    block=$2
fi

ARWEAVE_DIR="$(readlink -f "$(dirname "$0")")/.."
source "$ARWEAVE_DIR/testnet/testnet_nodes.sh"

for node in "${VDF_SERVER_NODES[@]}"; do
    ssh_start_node "$node" "$branch" "$block"
done

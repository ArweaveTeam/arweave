#!/bin/bash

# Function to display help
display_help() {
    echo "Usage: $0 <branch-name>]"
    echo "   branch-name: Name of the git branch."
    echo ""
    echo "Starts all nodes except the pilot node"
}

# Check for required arguments
if [ -z "$1" ]; then
    display_help
    exit 1
fi

branch="$1"

ARWEAVE_DIR="$(readlink -f "$(dirname "$0")")/.."
source "$ARWEAVE_DIR/testnet/testnet_nodes.sh"

for node in "${ALL_NODES[@]}"; do
    if is_node_in_array "$node" "${VDF_SERVER_NODES[@]}"; then
        continue
    fi
    ssh_start_node "$node" "$branch"
done

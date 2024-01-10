#!/bin/bash

# Function to display help
display_help() {
    echo "Usage: $0 <branch-name> <node> [<node> <node> ...]"
    echo "   branch-name: Name of the git branch."
    echo "   node: testnet node name (e.g. testnet-1, testnet-2, etc...)."
}

# Check for required arguments
if [ -z "$1" ] || [ -z "$2" ]; then
    display_help
    exit 1
fi

branch="$1"
shift # Shift off the first two arguments
nodes=("$@")

ARWEAVE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
source "$ARWEAVE_DIR/testnet/testnet_nodes.sh"

for node in "${nodes[@]}"; do
    ssh_start_node "$node" "$branch"
done

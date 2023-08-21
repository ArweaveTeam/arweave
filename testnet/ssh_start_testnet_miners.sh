#!/bin/bash

if [ -z "$1" ]; then
    echo "Error: Branch name not provided."
    echo "Usage: $0 <branch-name>"
    exit 1
fi

ARWEAVE_DIR="$(readlink -f "$(dirname "$0")")/.."
source $ARWEAVE_DIR/testnet/testnet_miners.sh
BRANCH="$1"

for server in "${TESTNET_SERVERS[@]}"; do
    echo "Rebuilding $server"
    ssh -q -t "$server" 'bash --norc --noprofile' << ENDSSH
    cd /opt/arweave/arweave
    git fetch --all
    git checkout $BRANCH
    git pull origin $BRANCH
    /opt/arweave/arweave/testnet/rebuild_testnet.sh
    exit
ENDSSH

    echo "Starting $server"
    ssh -q -t "$server" 'bash --norc --noprofile' << ENDSSH
    /opt/arweave/arweave/testnet/start_testnet_miner.sh
    exit
ENDSSH

    echo ""
    echo "=============================================="
    echo "Node is ready when the height is no longer -1:"
    echo "curl http://$server:1984"
done

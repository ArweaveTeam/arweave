#!/bin/bash

# Function to display help
display_help() {
    echo "Usage: $0 <type: pilot|client|solo> <branch-name> [additional arguments...]"
    echo "   type: Must be one of 'pilot', 'client', or 'solo'."
    echo "   branch-name: Name of the git branch."
    echo "   additional arguments: Optional arguments passed to the start_testnet_pilot.sh script."
}

# Check for required arguments
if [ -z "$1" ] || [ -z "$2" ]; then
    display_help
    exit 1
fi

TYPE="$1"
BRANCH="$2"
shift 2 # Shift off the first two arguments
OTHER_ARGS="$@"

# Check if first argument is one of the required values
if [[ "$TYPE" != "pilot" && "$TYPE" != "client" && "$TYPE" != "solo" ]]; then
    echo "Error: Invalid type provided. Must be one of 'pilot', 'client', or 'solo'."
    display_help
    exit 1
fi

ARWEAVE_DIR="$(readlink -f "$(dirname "$0")")/.."
source "$ARWEAVE_DIR/testnet/testnet_$TYPE.sh"


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

    echo "Starting $server: start_testnet_$TYPE.sh $OTHER_ARGS"
    ssh -q -t "$server" 'bash --norc --noprofile' << ENDSSH
    /opt/arweave/arweave/testnet/start_testnet_$TYPE.sh $OTHER_ARGS
    exit
ENDSSH

    echo ""
    echo "=============================================="
    echo "Node is ready when the height is no longer -1:"
    echo "curl http://$server:1984"
done

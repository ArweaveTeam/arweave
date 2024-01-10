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

ARWEAVE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
source "$ARWEAVE_DIR/testnet/testnet_$TYPE.sh"


for server in "${TESTNET_SERVERS[@]}"; do
    echo "Stopping $server"

    ssh -q -t "$server" 'bash --norc --noprofile' << 'ENDSSH'
    END_TIME=$((SECONDS+30))
    while (( SECONDS < END_TIME )); do
        NO_SCREENS=$(screen -list | grep -c 'No Sockets found')

        if (( NO_SCREENS > 0 )); then
            echo "Arweave node no longer running"
            break
        else
            echo "Found an Arweave node. Stopping..."
            /arweave-build/testnet/bin/stop
            sleep 5
        fi
    done

    if (( SECONDS >= END_TIME )); then
        echo "Timeout reached! Moving on."
    fi
    exit
ENDSSH

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

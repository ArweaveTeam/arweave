ALL_NODES+=(
testnet-1
testnet-2
testnet-3
testnet-4
testnet-5
testnet-6
)

VDF_SERVER_NODES+=(
testnet-4
)

VDF_CLIENT_NODES+=(
testnet-1
testnet-2
testnet-6
)

CM_EXIT_NODE+=(
testnet-2
)

CM_MINER_NODES+=(
testnet-1
testnet-3
testnet-6
)

MINER_NODES+=(
testnet-4
testnet-5
)

# Function to check if a specific array contains a value
# Arguments: $1 - nodename, $2 - array to search in
function is_node_in_array() {
	local search_value="$1"
	shift  # Shift parameters to the left, so $2 becomes $1, $3 becomes $2, and so on.
	local array=("$@")

	for value in "${array[@]}"; do
	if [[ $value == $search_value ]]; then
		return 0  # Return true (in shell script, 0 is true/success)
	fi
	done

	return 1  # Return false (non-zero is false/failure in shell script)
}

function ssh_start_node() {
	local node="$1"
	local branch="$2"
	shift 2
	local other_args="$@"

	local server="$node.arweave.net"

    echo "Rebuilding $server"
    ssh -q -t "$server" 'bash --norc --noprofile' << ENDSSH
    cd /opt/arweave/arweave
    git fetch --all
    git checkout $branch
    git pull origin $branch
    /opt/arweave/arweave/testnet/rebuild_testnet.sh
    exit
ENDSSH

    echo "Starting $server: start_testnet.sh $other_args"
    ssh -q -t "$server" 'bash --norc --noprofile' << ENDSSH
    /opt/arweave/arweave/testnet/start_testnet.sh $other_args
    exit
ENDSSH

    echo ""
    echo "=============================================="
    echo "Node is ready when the height is no longer -1:"
    echo "curl http://$server:1984"
}
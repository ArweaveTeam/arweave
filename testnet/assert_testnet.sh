#!/bin/bash

# List of testnet servers
mapfile -t TESTNET_MINERS < testnet_miners.txt
mapfile -t TESTNET_VDF < testnet_vdf.txt

TESTNET_SERVERS=("${TESTNET_MINERS[@]}" "${TESTNET_VDF[@]}")

# Get the current hostname
current_host=$(hostname -f)

# Check if current hostname is in the list of testnet servers
is_testnet_server=0
for server in "${TESTNET_SERVERS[@]}"; do
    # Extract subdomain from the server in the list
    subdomain="${server%%.*}"

    if [[ "$current_host" == "$subdomain" ]]; then
        is_testnet_server=1
        break
    fi
done

# If not a testnet server, abort
if [[ "$is_testnet_server" -eq 0 ]]; then
    exit 1
fi

mkdir -p /arweave-build/mainnet
mkdir -p /arweave-build/testnet

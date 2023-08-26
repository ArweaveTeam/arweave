#!/bin/bash

TESTNET_SERVERS=$(cat testnet_miners.txt)
TESTNET_SERVERS+=" "
TESTNET_SERVERS+=$(cat testnet_vdf.txt)

for server in $TESTNET_SERVERS; do
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
        echo "Timeout reached! Moving on to the next server."
    fi
    exit
ENDSSH
done

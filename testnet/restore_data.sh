#!/bin/bash

ARWEAVE_DIR="$(cd "$(dirname "$0")/.." && pwd)"

if ! $ARWEAVE_DIR/testnet/assert_testnet.sh; then
	echo "Error: This script must be run on a testnet server."
	exit 1
fi

if [ $# -ne 1 ]; then
	echo "restore_data.sh <backup name>"
    exit 1
fi

NAME=$1
BACKUP_DIR="/arweave-backups/${NAME}/"

if [ ! -d "$BACKUP_DIR" ]; then
    echo "Error: Backup directory $BACKUP_DIR does not exist."
    exit 1
fi

DIRECTORIES=(
	"data_sync_state"
	"header_sync_state"
    "ar_tx_blacklist"
    "disk_cache"
    "rocksdb"
    "txs"
    "wallet_lists"
    "wallets"
)

# Warn about the deletion
echo "The following files/directories will be DELETED:"
for DIR in "${DIRECTORIES[@]}"; do
    echo "/arweave-data/$DIR"
done

# Prompt for confirmation
echo "Are you sure you want to continue? (yes/no)"
read -r RESPONSE

if [[ "$RESPONSE" == "yes" ]]; then
    # Proceed with deletion
	
    for DIR in "${DIRECTORIES[@]}"; do
		FULL_PATH="/arweave-data/$DIR"
        if [ -e "$FULL_PATH" ]; then
			set -x
            rm -rf "$FULL_PATH"
			{ set +x; } 2>/dev/null
        fi
    done
else
    # Abort the operation
    echo "Operation aborted."
	exit 0
fi

for DIR in "${DIRECTORIES[@]}"; do
	set -x
	cp -rf $BACKUP_DIR/$DIR /arweave-data/$DIR
	{ set +x; } 2>/dev/null
done

echo


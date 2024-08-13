#!/bin/bash

ARWEAVE_DIR="$(cd "$(dirname "$0")/.." && pwd)"

if ! $ARWEAVE_DIR/testnet/assert_testnet.sh; then
	echo "Error: This script must be run on a testnet server."
	exit 1
fi

mkdir -p /arweave-build/mainnet
rm -rf /arweave-build/mainnet/*

echo "$0 $@" > /arweave-build/mainnet/build.command

cd $ARWEAVE_DIR
rm -rf $ARWEAVE_DIR/_build/prod/rel/arweave/*
$ARWEAVE_DIR/rebar3 as prod tar
tar xf $ARWEAVE_DIR/_build/prod/rel/arweave/arweave-*.tar.gz -C /arweave-build/mainnet


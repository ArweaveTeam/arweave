#!/bin/bash

if [ $# -ne 2 ]; then
	echo "ar-rebar3 <profile> <command>"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROFILE=$1
COMMAND=$2

echo Removing build artifacts...
set -x
rm -f "${SCRIPT_DIR}/bin/arweave-dev"
rm -f "${SCRIPT_DIR}/lib"
rm -f "${SCRIPT_DIR}/releases"
{ set +x; } 2>/dev/null
echo

${SCRIPT_DIR}/rebar3 as ${PROFILE} ${COMMAND}

if [ "${COMMAND}" = "release" ]; then
	RELEASE_PATH=$(${SCRIPT_DIR}/rebar3 as ${ARWEAVE_BUILD_TARGET:-default} path --rel)
    echo
    echo Copying and linking build artifacts
    set -x
    cp ${RELEASE_PATH}/arweave/bin/arweave ${SCRIPT_DIR}/bin/arweave-dev
    ln -s ${RELEASE_PATH}/arweave/releases ${SCRIPT_DIR}/releases
    ln -s ${RELEASE_PATH}/arweave/lib ${SCRIPT_DIR}/lib
    { set +x; } 2>/dev/null
    echo
fi

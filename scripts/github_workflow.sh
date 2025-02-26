#!/bin/bash
######################################################################
# This script has been extracted from the github workflow and can
# then be reused locally outside of a runner.
#
# Usage:
#   ./github_workflow.sh NAMESPACE
######################################################################

# print the logs if tests fail
_print_peer_logs() {
	peer=${1}
	if ls ${peer}-*.out 2>&1 >/dev/null
	then
		echo -e "\033[0;31m===> Test failed, printing the ${peer} node's output...\033[0m"
		cat ${peer}-*.out
	else
		echo -e "\033[0;31m===> Test failed without ${peer} output...\033[0m"
	fi
}

# check if test can be restarted
_check_retry() {
	local first_line_peer1
	echo -e "\033[0;32m===> Checking for retry\033[0m"

	# For debugging purposes, print the peer1 output if the tests failed
	if ls peer1-*.out 2>&1 >/dev/null
	then
		first_line_peer1=$(head -n 1 peer1-*.out)
	fi

	first_line_main=$(head -n 1 main.out)
	echo -e "\033[0;31m===> First line of peer1 node's output: $first_line_peer1\033[0m"
	echo -e "\033[0;31m===> First line of main node's output: $first_line_main\033[0m"

	# Check if it is a retryable error
	if [[ "$first_line_peer1" == "Protocol 'inet_tcp': register/listen error: "* ]]
	then
		echo "Retrying test because of inet_tcp error..."
		RETRYABLE=1
		sleep 1
	elif [[ "$first_line_peer1" == "Protocol 'inet_tcp': the name"* ]]
	then
		echo "Retrying test because of inet_tcp clash..."
		RETRYABLE=1
		sleep 1
	elif [[ "$first_line_main" == *"econnrefused"* ]]
	then
		echo "Retrying test because of econnrefused..."
		RETRYABLE=1
		sleep 1
	else
		_print_peer_logs peer1
		_print_peer_logs peer2
		_print_peer_logs peer3
		_print_peer_logs peer4
	fi
}

# set github environment
_set_github_env() {
	if test -z "${GITHUB_ENV}"
	then
		echo "GITHUB_ENV variable not set"
		return 1
	fi

	local exit_code=${1}
	# Set the exit_code output variable using Environment Files
	echo "exit_code=${exit_code}" >> ${GITHUB_ENV}
	return 0
}

######################################################################
# main script
######################################################################
MODE="${1}"
NAMESPACE_FLAG="${2}"
PWD=$(pwd)
EXIT_CODE=0
export PATH="${PWD}/_build/erts/bin:${PATH}"
export ERL_EPMD_ADDRESS="127.0.0.1"
export NAMESPACE="${NAMESPACE_FLAG}"
export ERL_PATH_ADD="$(echo ${PWD}/_build/test/lib/*/ebin)"
export ERL_PATH_TEST="${PWD}/_build/test/lib/arweave/test"
export ERL_PATH_CONF="${PWD}/config/sys.config"
export ERL_TEST_OPTS="-pa ${ERL_PATH_ADD} ${ERL_PATH_TEST} -config ${ERL_PATH_CONF}"

if test "${1}" = "e2e"
then
	export ERL_PATH_E2E_ADD="$(echo ${PWD}/_build/e2e/lib/*/ebin)"
	export ERL_PATH_E2E_TEST="${PWD}/_build/e2e/lib/arweave/e2e"
	export ERL_TEST_OPTS="-pa ${ERL_PATH_E2E_ADD} ${ERL_PATH_E2E_TEST} ${ERL_TEST_OPTS}"
fi

RETRYABLE=1
while [[ $RETRYABLE -eq 1 ]]
do
	RETRYABLE=0
	set +e
	set -x
	NODE_NAME="main-${NAMESPACE}@127.0.0.1"
	COOKIE=${NAMESPACE}
	erl +S 4:4 $ERL_TEST_OPTS \
		-noshell \
		-name "${NODE_NAME}" \
		-setcookie "${COOKIE}" \
		-run ar ${MODE} "${NAMESPACE}" \
		-s init stop 2>&1 | tee main.out
	EXIT_CODE=${PIPESTATUS[0]}
	set +x
	set -e

	if [[ ${EXIT_CODE} -ne 0 ]]
	then
		_check_retry
	fi
done

# exit with the exit code of the tests
_set_github_env ${EXIT_CODE}
exit ${EXIT_CODE}

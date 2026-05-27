#!/bin/bash
######################################################################
# Run eunit / e2e tests for one or more modules. Each module runs in
# its own fresh BEAM (clean isolation). When more than one module is
# passed, this script loops through them — useful for batched fast
# shards that share an artifact download but still get per-module
# BEAM separation.
#
# Usage:
#   github_workflow.sh MODE NAMESPACE [MODULE ...]
#
# - MODE: "tests" or "e2e".
# - NAMESPACE: identifier used in failure artifact filenames. Pass
#   the matrix slot name (e.g. "fast_shard_0") or, for back-compat
#   with the per-shard matrix, the single module name.
# - MODULE: one or more module names. Each module gets its own
#   erl invocation. If omitted, NAMESPACE is treated as the single
#   module to run.
######################################################################

_print_peer_logs() {
	local peer=${1}
	local module=${2}
	if ls "${peer}-${module}.out" 2>/dev/null
	then
		echo -e "\033[0;31m===> Test failed, printing the ${peer} node's output...\033[0m"
		cat "${peer}-${module}.out"
	else
		echo -e "\033[0;31m===> Test failed without ${peer} output...\033[0m"
	fi
}

# Check whether the just-failed erl run hit a known-retryable error.
# Sets RETRYABLE=1 if so. Looks at this module's specific output
# files so siblings in a batched shard don't contaminate the signal.
_check_retry() {
	local module=${1}
	local first_line_peer1=""
	local first_line_main=""

	if ls "peer1-${module}.out" 2>/dev/null
	then
		first_line_peer1=$(head -n 1 "peer1-${module}.out")
	fi
	if [ -f "main-${module}.out" ]
	then
		first_line_main=$(head -n 1 "main-${module}.out")
	fi

	echo -e "\033[0;32m===> Checking for retry (module=${module})\033[0m"
	echo -e "\033[0;31m===> First line of peer1 node's output: $first_line_peer1\033[0m"
	echo -e "\033[0;31m===> First line of main node's output: $first_line_main\033[0m"

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
		_print_peer_logs peer1 "${module}"
		_print_peer_logs peer2 "${module}"
		_print_peer_logs peer3 "${module}"
		_print_peer_logs peer4 "${module}"
	fi
}

_set_github_env() {
	if test -z "${GITHUB_ENV}"
	then
		echo "GITHUB_ENV variable not set"
		return 1
	fi

	local exit_code=${1}
	echo "exit_code=${exit_code}" >> "${GITHUB_ENV}"
	return 0
}

######################################################################
# Main
######################################################################
MODE="${1}"
NAMESPACE_FLAG="${2}"
shift 2
MODULES_TO_RUN=("$@")
if [ ${#MODULES_TO_RUN[@]} -eq 0 ]; then
	MODULES_TO_RUN=("${NAMESPACE_FLAG}")
fi

PWD=$(pwd)
OVERALL_EXIT_CODE=0
export PATH="${PWD}/_build/erts/bin:${PATH}"
export ERL_EPMD_ADDRESS="127.0.0.1"

if test "${MODE}" = "e2e"
then
	export ERL_PATH_ADD="$(echo ${PWD}/_build/e2e/lib/*/ebin)"
	export ERL_PATH_TEST="$(echo ${PWD}/_build/e2e/lib/*/e2e)"
else
	export ERL_PATH_ADD="$(echo ${PWD}/_build/test/lib/*/ebin)"
	# All apps' compiled eunit test modules, not just arweave's. The
	# pre-discovery matrix only listed modules from apps/arweave/test/,
	# so this single hard-coded path used to be sufficient; with
	# auto-discovery picking up apps/arweave_limiter/test/ (and any
	# future app's test dir), we need every app's test/ on the path.
	export ERL_PATH_TEST="$(echo ${PWD}/_build/test/lib/*/test)"
fi

export ERL_PATH_CONF="${PWD}/config/sys.config"
export ERL_TEST_OPTS="-pa ${ERL_PATH_ADD} ${ERL_PATH_TEST} -config ${ERL_PATH_CONF}"

for MODULE in "${MODULES_TO_RUN[@]}"; do
	echo "============================================================"
	echo "=== Running ${MODE} for module: ${MODULE} ==="
	echo "============================================================"

	# Each module's BEAM uses the module name as namespace so node
	# names, cookies, and *.out files don't collide with sibling
	# modules running in the same shard.
	export NAMESPACE="${MODULE}"
	NODE_NAME="main-${MODULE}@127.0.0.1"
	COOKIE="${MODULE}"

	RETRYABLE=1
	EXIT_CODE=0
	while [[ $RETRYABLE -eq 1 ]]
	do
		RETRYABLE=0
		set +e
		set -x
		erl +S 4:4 $ERL_TEST_OPTS \
			-noshell \
			-name "${NODE_NAME}" \
			-setcookie "${COOKIE}" \
			-run ar ${MODE} "${MODULE}" \
			-s init stop 2>&1 | tee "main-${MODULE}.out"
		EXIT_CODE=${PIPESTATUS[0]}
		set +x
		set -e

		if [[ ${EXIT_CODE} -ne 0 ]]
		then
			_check_retry "${MODULE}"
		fi
	done

	if [[ ${EXIT_CODE} -ne 0 ]]
	then
		echo "=== Module ${MODULE} FAILED with exit ${EXIT_CODE} ==="
		OVERALL_EXIT_CODE=${EXIT_CODE}
	else
		echo "=== Module ${MODULE} passed ==="
	fi
done

_set_github_env ${OVERALL_EXIT_CODE}
exit ${OVERALL_EXIT_CODE}

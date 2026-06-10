#!/usr/bin/env bash
set -euo pipefail

# Usage: list_e2e_cases.sh [FORMAT]
#   FORMAT: plain | json   (default: plain)
#
# Discovers every Common-Test e2e case by scanning `apps/*/e2e/*_SUITE.erl'
# and calling each suite's `all/0' callback. Output is one row per case.
#
# Discovery rule: any `*_SUITE.erl' file living under an `e2e/' directory
# at app root is an e2e suite, and every entry returned by its `all/0'
# callback is one matrix row. There is no manual list — add a testcase
# (and include it in `all/0') and CI picks it up on the next run.
#
# Requires the e2e profile to be compiled first (the `all/0' callback is
# read from the BEAM, not the source). Run `./ar-rebar3 e2e compile' if
# the suites haven't been built yet.
#
# Plain output (one row per line):
#   ar_repack_mine_SUITE test_replica_2_9_to_replica_2_9
#   ar_repack_mine_SUITE test_replica_2_9_to_spora_2_6
#   ...
#
# JSON output (for GitHub Actions matrix.include):
#   [{"suite":"ar_repack_mine_SUITE","case":"test_replica_2_9_to_replica_2_9"}, ...]

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd -P)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd -P)"
FORMAT="${1:-plain}"

# Every `_SUITE.erl' file under `apps/*/e2e/'.
SUITE_FILES="$(find "${REPO_ROOT}/apps" -path '*/e2e/*_SUITE.erl' -type f 2>/dev/null | sort)"
if [ -z "${SUITE_FILES}" ]; then
	case "${FORMAT}" in
		json) echo "[]" ;;
		*)    : ;;
	esac
	exit 0
fi

# Module names (basename minus .erl).
SUITE_NAMES="$(echo "${SUITE_FILES}" | xargs -n1 basename | sed 's/\.erl$//')"

# e2e BEAM paths so `Suite:all/0' resolves.
BUILD="${REPO_ROOT}/_build/e2e"
ERL_PATHS=""
for ebin in "${BUILD}"/lib/*/ebin; do
	[ -d "${ebin}" ] && ERL_PATHS="${ERL_PATHS} ${ebin}"
done
if [ -z "${ERL_PATHS}" ]; then
	echo "list_e2e_cases.sh: no compiled e2e artifacts under ${BUILD}; run './ar-rebar3 e2e compile' first" >&2
	exit 1
fi

# Build the Erlang `Suites' list literal as `[mod1, mod2, ...]'.
SUITES_ATOM_LIST="$(echo "${SUITE_NAMES}" | awk 'NR>1{printf ","} {printf "%s", $0} END{print ""}')"

# Emit one "Suite Case" pair per line. Each suite's `all/0' may return
# atoms (the common case), `{group, Name}' tuples (groups - ignored
# here), or other forms - we only emit atoms.
PAIRS="$(erl -noshell -pa ${ERL_PATHS} -eval "
	Suites = [${SUITES_ATOM_LIST}],
	lists:foreach(fun(S) ->
		case catch S:all() of
			Cases when is_list(Cases) ->
				[io:format(\"~s ~s~n\", [S, C]) || C <- Cases, is_atom(C)];
			_ ->
				ok
		end
	end, Suites),
	init:stop().")"

case "${FORMAT}" in
	json)
		# Convert "Suite Case" lines into a JSON array of objects.
		printf '['
		first=1
		while IFS=' ' read -r suite case_name; do
			[ -z "${suite}" ] && continue
			if [ "${first}" = "1" ]; then first=0; else printf ','; fi
			printf '{"suite":"%s","case":"%s"}' "${suite}" "${case_name}"
		done <<< "${PAIRS}"
		echo ']'
		;;
	plain|*)
		echo "${PAIRS}"
		;;
esac

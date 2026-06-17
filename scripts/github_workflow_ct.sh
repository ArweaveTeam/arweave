#!/usr/bin/env bash
######################################################################
# Run one Common Test e2e case via `bin/e2e'.
#
# Used by the per-case matrix in `.github/workflows/e2e-test.yml': each
# job invokes this script with the suite module name + the testcase
# name discovered by `scripts/list_e2e_cases.sh'. The script locates
# the SUITE file on disk, then defers to `bin/e2e' for the actual
# `ct:run_test/1' invocation.
#
# Usage:
#   github_workflow_ct.sh SUITE_NAME CASE_NAME
#
# Example:
#   github_workflow_ct.sh ar_repack_mine_SUITE test_replica_2_9_to_unpacked
######################################################################
set -e

SUITE_NAME="${1:?suite module name required}"
CASE_NAME="${2:?testcase name required}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd -P)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd -P)"

# Locate the SUITE file. The discovery script enumerates SUITEs from
# `apps/*/e2e/*_SUITE.erl' so any matching `_SUITE.erl' under that
# layout is a candidate.
SUITE_FILE="$(find "${REPO_ROOT}/apps" -path "*/e2e/${SUITE_NAME}.erl" -type f 2>/dev/null | head -1)"
if [ -z "${SUITE_FILE}" ]; then
	echo "github_workflow_ct.sh: SUITE file for ${SUITE_NAME} not found under apps/*/e2e/" >&2
	exit 2
fi

# Use the path relative to REPO_ROOT so `bin/e2e`'s `cd' into the
# repo root resolves it.
SUITE_REL="${SUITE_FILE#${REPO_ROOT}/}"

echo "============================================================"
echo "=== Running CT case: ${SUITE_NAME}:${CASE_NAME}"
echo "===   suite path: ${SUITE_REL}"
echo "============================================================"

# Run against the prebuilt artifact -- the build job already compiled the
# e2e profile + native libs, so skip the per-case recompile in bin/e2e
# (that recompile was the cc1plus CPU storm that severed the runner). Like
# eunit, we just launch the prebuilt suites.
export AR_E2E_SKIP_COMPILE=1
exec bash "${REPO_ROOT}/bin/e2e" --suite="${SUITE_REL}" --case="${CASE_NAME}"

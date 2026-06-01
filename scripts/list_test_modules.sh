#!/usr/bin/env bash
set -euo pipefail

# Usage: list_test_modules.sh [CATEGORY] [FORMAT]
#   CATEGORY: fast | slow | vdf | all  (default: all)
#   FORMAT:   plain | json              (default: plain)
#
# CATEGORY semantics — derived from a `-test_category([<cat>[, <cat>...]])`
# module attribute in the test source. Attributes look like:
#
#     -test_category([fast]).
#     -test_category([fast, vdf]).
#
# Categories comma-separated; keep the attribute on a single line.
# Absence of the attribute = default behavior:
#   - `fast`  = matches modules annotated with `fast`. These can run
#               in batched CI shards (each module still gets its own
#               BEAM, but multiple modules share one artifact download).
#   - `slow`  = matches modules WITHOUT `fast`. They run in the main
#               matrix, one shard per module (full parallelism).
#               Default if a module isn't annotated.
#   - `vdf`   = matches modules annotated with `vdf`. Used by the
#               macOS workflow for the VDF-relevant subset.
#   - `all`   = every module that has eunit tests.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd -P)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd -P)"

CATEGORY="${1:-all}"
FORMAT="${2:-plain}"

# We scan every app's src/ and test/ for *.erl. eunit tests live
# inline (src/) or in dedicated *_tests.erl files (test/). *_SUITE.erl
# files belong to Common Test (./bin/ct), not this matrix.
APPS_DIR="${REPO_ROOT}/apps"

# All .erl files under apps/*/src and apps/*/test, excluding CT suites.
candidate_files() {
    find "${APPS_DIR}" -type d \( -name src -o -name test \) -maxdepth 3 \
        -exec find {} -maxdepth 1 -name '*.erl' ! -name '*_SUITE.erl' \; \
        2>/dev/null \
        | sort -u
}

# Output: one module name per file containing a `_test/0` or
# `_test_/0` function head (the eunit conventions), sorted unique.
list_modules_with_eunit_tests() {
    candidate_files \
        | while read -r f; do
            if grep -qE '^[a-z_][a-z0-9_]*_test_?\(\)[[:space:]]*->' "$f"; then
                basename "$f" .erl
            fi
        done \
        | sort -u
}

# Output: module name per file that declares the given -test_category.
list_modules_with_category() {
    local target="$1"
    candidate_files \
        | while read -r f; do
            # awk extracts categories from the -test_category([...])
            # attribute: grab the contents between [ and ], split on
            # comma, trim whitespace, print one per line. Anchoring on
            # `^-test_category(' ignores commented-out lines.
            cats=$(awk '
                /^-test_category\(\[/ {
                    line = $0
                    sub(/^-test_category\(\[[[:space:]]*/, "", line)
                    sub(/[[:space:]]*\]\).*/, "", line)
                    n = split(line, parts, /[[:space:]]*,[[:space:]]*/)
                    for (i = 1; i <= n; i++) {
                        cat = parts[i]
                        sub(/[[:space:]]+$/, "", cat)
                        if (cat != "") print cat
                    }
                }
            ' "$f")
            if echo "$cats" | grep -qFx "$target"; then
                basename "$f" .erl
            fi
        done \
        | sort -u
}

case "${CATEGORY}" in
    fast)
        # Modules tagged `fast` that also have eunit tests.
        comm -12 \
            <(list_modules_with_eunit_tests) \
            <(list_modules_with_category fast)
        ;;
    slow)
        # Modules with eunit tests, but NOT tagged `fast` or `canary`.
        # `canary` is handled by x-test-canary.yml, not the main matrix.
        comm -23 \
            <(list_modules_with_eunit_tests) \
            <(list_modules_with_category fast \
              | sort -u -m - <(list_modules_with_category canary))
        ;;
    vdf)
        # Modules tagged `vdf` that also have eunit tests.
        comm -12 \
            <(list_modules_with_eunit_tests) \
            <(list_modules_with_category vdf)
        ;;
    canary)
        # Modules tagged `canary` (run by x-test-canary.yml).
        comm -12 \
            <(list_modules_with_eunit_tests) \
            <(list_modules_with_category canary)
        ;;
    all)
        list_modules_with_eunit_tests
        ;;
    *)
        echo "Unknown category: ${CATEGORY}. Use fast|slow|vdf|canary|all" >&2
        exit 1
        ;;
esac \
    | {
        case "${FORMAT}" in
            json)
                awk '
                    BEGIN { first = 1; printf("[") }
                    {
                        if (first == 0) printf(",")
                        printf("\"%s\"", $0)
                        first = 0
                    }
                    END { print "]" }
                '
                ;;
            plain)
                cat
                ;;
            *)
                echo "Unknown format: ${FORMAT}. Use plain|json" >&2
                exit 1
                ;;
        esac
    }

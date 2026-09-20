#!/usr/bin/env bash
set -euo pipefail

# Usage: list_ct_suites.sh [CATEGORY] [FORMAT]
#   CATEGORY: fast | slow | all   (default: all)
#   FORMAT:   plain | json        (default: plain)
#
# Or:    list_ct_suites.sh matrix BATCHES
#        list_ct_suites.sh matrix all
#
# Discovers the Common Test suites of the `test' profile: every
# `*_SUITE.erl' under `apps/*/test/'. Suites under `apps/*/e2e/' belong
# to the e2e profile (scripts/list_e2e_cases.sh), not this list.
#
# CATEGORY semantics -- the same `-test_category([...])' attribute,
# parser and convention as the eunit modules; see
# scripts/list_test_modules.sh and doc/agents/testing.md.
#   - `fast` = suites annotated `fast`. They are sliced into a few
#              shards that share one CT run each, so their per-job
#              overhead is amortised. Use only when the suite does not
#              share global state with siblings.
#   - `slow` = every other suite. Each runs in a CT shard of its own.
#              Default if a suite isn't annotated.
#   - `all`  = every suite.
#
# `matrix BATCHES' emits the shard list for the GitHub Actions matrix
# in x-common-test.yml: one shard per `slow' suite plus BATCHES shards
# of the `fast' suites (every Nth suite, so a new suite lands in a
# shard without any list to maintain). `matrix all' emits a single
# shard running every suite serially, which the on-demand coverage
# run uses so its report is not split across shards.
#
#   [{"name":"arweave_sync_sim_SUITE","suites":"apps/arweave_sync/test/arweave_sync_sim_SUITE.erl"},
#    {"name":"batch-0","suites":"apps/a/test/a_SUITE.erl,apps/b/test/b_SUITE.erl"},
#    ...]
#
# Suite paths are repo-relative and comma-separated: the form
# `rebar3 ct --suite' takes.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd -P)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd -P)"
APPS_DIR="${REPO_ROOT}/apps"

# Repo-relative paths of every test-profile suite, sorted.
suite_files() {
    find "${APPS_DIR}" -maxdepth 2 -type d -name test \
        -exec find {} -maxdepth 1 -name '*_SUITE.erl' \; \
        2>/dev/null \
        | sed "s|^${REPO_ROOT}/||" \
        | sort -u
}

# The categories a suite declares, one per line. Same awk as
# scripts/list_test_modules.sh: contents between `[' and `]', split on
# comma, trimmed; anchoring on `^-test_category(' ignores commented-out
# lines.
suite_categories() {
    awk '
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
    ' "${REPO_ROOT}/$1"
}

fast_suites() {
    suite_files | while read -r f; do
        if suite_categories "$f" | grep -qFx fast; then
            echo "$f"
        fi
    done
}

slow_suites() {
    comm -23 <(suite_files) <(fast_suites)
}

# Lines -> JSON array of strings.
json_array() {
    awk '
        BEGIN { first = 1; printf("[") }
        {
            if (first == 0) printf(",")
            printf("\"%s\"", $0)
            first = 0
        }
        END { print "]" }
    '
}

# One `{"name":...,"suites":...}' object per input line of
# `NAME<TAB>SUITES', wrapped in a JSON array.
json_shards() {
    awk -F'\t' '
        BEGIN { first = 1; printf("[") }
        {
            if (first == 0) printf(",")
            printf("{\"name\":\"%s\",\"suites\":\"%s\"}", $1, $2)
            first = 0
        }
        END { print "]" }
    '
}

# Emit `NAME<TAB>SUITES' shard rows: one per slow suite, then the fast
# suites sliced round-robin into BATCHES rows (empty rows are dropped,
# so more batches than suites is harmless).
shard_rows() {
    local batches="$1"
    slow_suites | while read -r f; do
        printf '%s\t%s\n' "$(basename "$f" .erl)" "$f"
    done
    fast_suites | awk -v t="${batches}" '
        { shard[(NR - 1) % t] = shard[(NR - 1) % t] (shard[(NR - 1) % t] == "" ? "" : ",") $0 }
        END {
            for (i = 0; i < t; i++) {
                if (shard[i] != "") printf("batch-%d\t%s\n", i, shard[i])
            }
        }
    '
}

MODE="${1:-all}"

if [ "${MODE}" = "matrix" ]; then
    BATCHES="${2:?matrix needs a batch count or 'all'}"
    case "${BATCHES}" in
        all)
            printf 'all\t%s\n' "$(suite_files | paste -sd, -)" | json_shards
            ;;
        ''|*[!0-9]*|0)
            echo "matrix: BATCHES must be a positive integer or 'all', got '${BATCHES}'" >&2
            exit 1
            ;;
        *)
            shard_rows "${BATCHES}" | json_shards
            ;;
    esac
    exit 0
fi

FORMAT="${2:-plain}"
case "${MODE}" in
    all) suite_files ;;
    fast) fast_suites ;;
    slow) slow_suites ;;
    *)
        echo "Unknown category: ${MODE}. Use fast|slow|all, or 'matrix'" >&2
        exit 1
        ;;
esac \
    | {
        case "${FORMAT}" in
            json) json_array ;;
            plain) cat ;;
            *)
                echo "Unknown format: ${FORMAT}. Use plain|json" >&2
                exit 1
                ;;
        esac
    }

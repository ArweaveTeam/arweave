#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd -P)"
MODULES_FILE="${SCRIPT_DIR}/full_test_modules.txt"
FORMAT="${1:-plain}"

case "${FORMAT}" in
  json)
    awk '
      BEGIN {
        first = 1
        printf("[")
      }

      /^[[:space:]]*#/ || /^[[:space:]]*$/ {
        next
      }

      {
        gsub(/^[[:space:]]+|[[:space:]]+$/, "", $0)
        if (first == 0) {
          printf(",")
        }
        printf("\"%s\"", $0)
        first = 0
      }

      END {
        print "]"
      }
    ' "${MODULES_FILE}"
    ;;
  plain)
    awk '
      /^[[:space:]]*#/ || /^[[:space:]]*$/ {
        next
      }

      {
        gsub(/^[[:space:]]+|[[:space:]]+$/, "", $0)
        print
      }
    ' "${MODULES_FILE}"
    ;;
  *)
    echo "Usage: $0 [plain|json]" >&2
    exit 1
    ;;
esac

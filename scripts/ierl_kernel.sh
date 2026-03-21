#!/usr/bin/env sh

set -eu

SCRIPT_DIR="$(dirname "$0")"
REPO_ROOT="$SCRIPT_DIR/.."
IERL_BIN="$REPO_ROOT/.venv/bin/ierl"

if [ -x "$IERL_BIN" ]; then
  exec "$IERL_BIN" "$@"
fi

exec ierl "$@"

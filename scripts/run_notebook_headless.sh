#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

NOTEBOOK_NAME="pricing_transition_localnet"
NOTEBOOK_PATH=""
NOTEBOOK_ARG=""

while [ "$#" -gt 0 ]; do
  NOTEBOOK_ARG="$1"
  shift 1
done

NODE_NAME_FULL="main-localnet@127.0.0.1"
NODE_COOKIE="localnet"
JOIN_TIMEOUT_SEC="${JOIN_TIMEOUT_SEC:-300}"
JOIN_POLL_SEC="${JOIN_POLL_SEC:-1}"
EXEC_TIMEOUT_SEC="${EXEC_TIMEOUT_SEC:-1200}"
KERNEL_NAME="${ERLANG_JUPYTER_KERNEL:-erlang}"
JUPYTER_DATA_DIR="${JUPYTER_DATA_DIR:-$REPO_ROOT/.tmp/jupyter}"
JUPYTER_CONFIG_DIR="${JUPYTER_CONFIG_DIR:-$REPO_ROOT/.jupyter}"
LOCALNET_HTTP_HOST="${LOCALNET_HTTP_HOST:-127.0.0.1}"
LOCALNET_HTTP_PORT="${LOCALNET_HTTP_PORT:-1984}"
LOCALNET_NETWORK_NAME="${LOCALNET_NETWORK_NAME:-arweave.localnet}"

STARTED_LOCALNET=0
LOCALNET_PID=""

resolve_notebook() {
  if [ -z "$NOTEBOOK_PATH" ]; then
    case "${NOTEBOOK_ARG:-$NOTEBOOK_NAME}" in
      *.ipynb|*/*)
        NOTEBOOK_PATH="${NOTEBOOK_ARG:-$NOTEBOOK_NAME}"
        ;;
      *)
        NOTEBOOK_PATH="notebooks/${NOTEBOOK_ARG:-$NOTEBOOK_NAME}.ipynb"
        ;;
    esac
  fi

  if [ ! -f "$NOTEBOOK_PATH" ]; then
    echo "Notebook not found: $NOTEBOOK_PATH"
    exit 1
  fi
}

start_localnet() {
  if [ "$(uname -s)" == "Darwin" ]; then
    RANDOMX_JIT="disable randomx_jit"
  else
    RANDOMX_JIT=
  fi

  export ERL_EPMD_ADDRESS=127.0.0.1

  ./ar-rebar3 localnet compile

  ERL_LOCALNET_OPTS="-pa $(./rebar3 as localnet path) $(./rebar3 as localnet path --base)/lib/arweave/test -config config/sys.config"

  erl $ERL_LOCALNET_OPTS -name "$NODE_NAME_FULL" -setcookie "$NODE_COOKIE" -noshell -s ar shell_localnet -eval "timer:sleep(infinity)." &
  LOCALNET_PID="$!"
  STARTED_LOCALNET=1
}

fetch_info() {
  curl -fsS --max-time 2 \
    -H "x-network: ${LOCALNET_NETWORK_NAME}" \
    "http://${LOCALNET_HTTP_HOST}:${LOCALNET_HTTP_PORT}/info" 2>/dev/null | tr -d '\n' || true
}

parse_info_network() {
  local info="$1"
  echo "$info" | sed -E -n 's/.*"network"[[:space:]]*:[[:space:]]*"([^"]*)".*/\1/p'
}

parse_info_height() {
  local info="$1"
  echo "$info" | sed -E -n 's/.*"height"[[:space:]]*:[[:space:]]*(-?[0-9]+).*/\1/p'
}

wait_for_info_height() {
  local start
  local info
  local network
  local height
  start="$(date +%s)"

  while true; do
    info="$(fetch_info)"
    if [ -n "$info" ]; then
      network="$(parse_info_network "$info")"
      if [ -z "$network" ]; then
        echo "Failed to parse network from /info: $info"
        return 1
      fi
      if [ "$network" != "$LOCALNET_NETWORK_NAME" ]; then
        echo "Found node at ${LOCALNET_HTTP_HOST}:${LOCALNET_HTTP_PORT} with network ${network}, expected ${LOCALNET_NETWORK_NAME}."
        return 1
      fi

      height="$(parse_info_height "$info")"
      if [ -z "$height" ]; then
        echo "Failed to parse height from /info: $info"
        return 1
      fi
      if [ "$height" != "-1" ]; then
        return 0
      fi
    fi

    if [ "$(( $(date +%s) - start ))" -ge "$JOIN_TIMEOUT_SEC" ]; then
      echo "Timed out waiting for localnet /info height."
      return 1
    fi

    sleep "$JOIN_POLL_SEC"
  done
}

cleanup() {
  if [ "$STARTED_LOCALNET" = "1" ] && [ -n "$LOCALNET_PID" ]; then
    kill "$LOCALNET_PID" >/dev/null 2>&1 || true
  fi
}

run_notebook() {
  local jupyter_cmd
  local tmp_dir
  local tmp_output
  local tmp_root
  jupyter_cmd=()
  export PATH="$REPO_ROOT/.venv/bin:$REPO_ROOT/scripts:$PATH"

  if command -v jupyter >/dev/null 2>&1; then
    jupyter_cmd=("jupyter")
  elif command -v uv >/dev/null 2>&1 && [ -d "$REPO_ROOT/.venv" ]; then
    jupyter_cmd=("uv" "run" "jupyter")
  else
    jupyter_cmd=("jupyter")
  fi

  tmp_root="${NOTEBOOK_TMP_DIR:-$REPO_ROOT/.tmp/nbconvert}"
  mkdir -p "$tmp_root"
  tmp_dir="$(mktemp -d "$tmp_root/notebook.XXXXXX")"
  tmp_output="$(basename "$NOTEBOOK_PATH")"

  JUPYTER_DATA_DIR="$JUPYTER_DATA_DIR" JUPYTER_CONFIG_DIR="$JUPYTER_CONFIG_DIR" "${jupyter_cmd[@]}" nbconvert \
    --to notebook \
    --execute \
    --output "$tmp_output" \
    --output-dir "$tmp_dir" \
    --ExecutePreprocessor.timeout="$EXEC_TIMEOUT_SEC" \
    --ExecutePreprocessor.kernel_name="$KERNEL_NAME" \
    "$NOTEBOOK_PATH"

  if [ "${NOTEBOOK_SAVE_OUTPUTS:-}" = "1" ]; then
    mv "$tmp_dir/$tmp_output" "$NOTEBOOK_PATH"
  fi

  rm -rf "$tmp_dir"
}

cd "$REPO_ROOT"

trap cleanup EXIT

resolve_notebook

if [ -z "$(fetch_info)" ]; then
  start_localnet
fi

wait_for_info_height
run_notebook

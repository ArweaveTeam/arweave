#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

NOTEBOOK_NAME="pricing_transition_localnet"
NOTEBOOK_PATH=""
NOTEBOOK_ARG=""
SNAPSHOT_DIR=""

while [ "$#" -gt 0 ]; do
  case "$1" in
    --snapshot-dir)
      SNAPSHOT_DIR="$2"
      shift 2
      ;;
    --snapshot-dir=*)
      SNAPSHOT_DIR="${1#*=}"
      shift 1
      ;;
    *)
      NOTEBOOK_ARG="$1"
      shift 1
      ;;
  esac
done

NODE_NAME="${LOCALNET_NODE_NAME:-main-localnet}"
NODE_COOKIE="${LOCALNET_COOKIE:-localnet}"
JOIN_TIMEOUT_SEC="${JOIN_TIMEOUT_SEC:-300}"
JOIN_POLL_SEC="${JOIN_POLL_SEC:-1}"
EXEC_TIMEOUT_SEC="${EXEC_TIMEOUT_SEC:-1200}"
KERNEL_NAME="${ERLANG_JUPYTER_KERNEL:-erlang}"
JUPYTER_DATA_DIR="${JUPYTER_DATA_DIR:-$REPO_ROOT/.tmp/jupyter}"
JUPYTER_CONFIG_DIR="${JUPYTER_CONFIG_DIR:-$REPO_ROOT/.jupyter}"

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

erl_eval() {
  local expr="$1"
  erl -noshell -eval "$expr" -eval "init:stop()."
}

node_ping() {
  erl_eval "net_kernel:start([checker, shortnames]), erlang:set_cookie(node(), '${NODE_COOKIE}'), io:format(\"~p\", [net_adm:ping('${NODE_NAME_FULL}')])"
}

node_joined() {
  erl_eval "net_kernel:start([checker, shortnames]), erlang:set_cookie(node(), '${NODE_COOKIE}'), io:format(\"~p\", [rpc:call('${NODE_NAME_FULL}', ar_node, is_joined, [])])"
}

start_localnet() {
  if [ "$(uname -s)" == "Darwin" ]; then
    RANDOMX_JIT="disable randomx_jit"
  else
    RANDOMX_JIT=
  fi

  export ERL_EPMD_ADDRESS=127.0.0.1
  if [ -n "$SNAPSHOT_DIR" ]; then
    export LOCALNET_SNAPSHOT_DIR="$SNAPSHOT_DIR"
  fi
  ERL_LOCALNET_OPTS="-pa $(./rebar3 as localnet path) $(./rebar3 as localnet path --base)/lib/arweave/test -config config/sys.config"

  ./ar-rebar3 localnet compile

  erl $ERL_LOCALNET_OPTS -sname "$NODE_NAME_START" -setcookie "$NODE_COOKIE" -noshell -s ar shell_localnet -eval "timer:sleep(infinity)." &
  LOCALNET_PID="$!"
  STARTED_LOCALNET=1
}

wait_for_joined() {
  local start
  start="$(date +%s)"

  while true; do
    if [ "$(node_ping)" = "pong" ]; then
      if [ "$(node_joined)" = "true" ]; then
        return 0
      fi
    fi

    if [ "$(( $(date +%s) - start ))" -ge "$JOIN_TIMEOUT_SEC" ]; then
      echo "Timed out waiting for localnet to join."
      return 1
    fi

    sleep "$JOIN_POLL_SEC"
  done
}

cleanup() {
  if [ "$STARTED_LOCALNET" = "1" ] && [ -n "$LOCALNET_PID" ]; then
    if [ "$(node_ping)" = "pong" ]; then
      erl_eval "net_kernel:start([checker, shortnames]), erlang:set_cookie(node(), '${NODE_COOKIE}'), rpc:cast('${NODE_NAME_FULL}', init, stop, [])"
      local stop_start
      stop_start="$(date +%s)"
      while [ "$(node_ping)" = "pong" ]; do
        if [ "$(( $(date +%s) - stop_start ))" -ge 15 ]; then
          break
        fi
        sleep 1
      done
    fi

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

NODE_HOST="$(hostname -s 2>/dev/null || hostname)"
NODE_NAME_SHORT="${NODE_NAME%@*}"
NODE_NAME_FULL="${NODE_NAME_SHORT}@${NODE_HOST}"
NODE_NAME_START="$NODE_NAME_SHORT"

cd "$REPO_ROOT"

trap cleanup EXIT

resolve_notebook

if [ "$(node_ping)" != "pong" ]; then
  start_localnet
fi

wait_for_joined
run_notebook

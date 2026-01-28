#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

ERLANG_KERNEL_NAME="${ERLANG_JUPYTER_KERNEL:-erlang}"
IERL_URL="${IERL_URL:-https://github.com/filmor/ierl/releases/latest/download/ierl}"
IERL_PATH="${IERL_PATH:-$REPO_ROOT/.tmp/ierl}"
JUPYTER_DATA_DIR="${JUPYTER_DATA_DIR:-$REPO_ROOT/.tmp/jupyter}"

cd "$REPO_ROOT"

mkdir -p "$REPO_ROOT/.tmp"
mkdir -p "$JUPYTER_DATA_DIR"

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is not installed or not on PATH."
  exit 1
fi

if [ -d "$REPO_ROOT/.venv" ]; then
  echo "Using existing virtual environment at: $REPO_ROOT/.venv"
else
  python3 -m venv "$REPO_ROOT/.venv"
fi
"$REPO_ROOT/.venv/bin/python" -m pip install --upgrade pip
"$REPO_ROOT/.venv/bin/python" -m pip install jupyter pandas

if ! command -v curl >/dev/null 2>&1; then
  echo "curl is not installed or not on PATH."
  exit 1
fi

if [ ! -x "$IERL_PATH" ]; then
  curl -L "$IERL_URL" -o "$IERL_PATH"
  chmod +x "$IERL_PATH"
fi

if [ -d "$REPO_ROOT/.venv/bin" ]; then
  install -m 0755 "$IERL_PATH" "$REPO_ROOT/.venv/bin/ierl"
fi

cat > "$REPO_ROOT/scripts/ierl_kernel.sh" <<'EOF'
#!/usr/bin/env sh

set -eu

SCRIPT_DIR="$(dirname "$0")"
REPO_ROOT="$SCRIPT_DIR/.."
IERL_BIN="$REPO_ROOT/.venv/bin/ierl"

if [ -x "$IERL_BIN" ]; then
  exec "$IERL_BIN" "$@"
fi

exec ierl "$@"
EOF

chmod +x "$REPO_ROOT/scripts/ierl_kernel.sh"

install_kernel() {
  local kernel_dir="$JUPYTER_DATA_DIR/kernels/$ERLANG_KERNEL_NAME"
  local kernel_json="$kernel_dir/kernel.json"
  local kernel_wrapper="$kernel_dir/ierl_kernel.sh"

  mkdir -p "$kernel_dir"

  cat > "$kernel_wrapper" <<'EOF'
#!/usr/bin/env sh

set -eu

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
IERL_BIN="$REPO_ROOT/.venv/bin/ierl"

if [ -x "$IERL_BIN" ]; then
  if [ "${NOTEBOOK_SKIP_COMPILE:-0}" = "0" ]; then
    if [ ! -f "$REPO_ROOT/_build/localnet/lib/arweave/ebin/ar_node.beam" ]; then
      (cd "$REPO_ROOT" && ./ar-rebar3 localnet compile)
    fi
  fi
  if [ -d "$REPO_ROOT/_build/localnet/lib" ]; then
    export ERL_LIBS="$REPO_ROOT/_build/localnet/lib"
  fi
  exec "$IERL_BIN" "$@"
fi

exec ierl "$@"
EOF

  chmod +x "$kernel_wrapper"

  cat > "$kernel_json" <<'EOF'
{
  "argv": [
    "{resource_dir}/ierl_kernel.sh",
    "kernel",
    "erlang",
    "-f",
    "{connection_file}"
  ],
  "display_name": "Erlang",
  "language": "erlang"
}
EOF
}

install_kernel

if ! PATH="$REPO_ROOT/.venv/bin:$PATH" JUPYTER_DATA_DIR="$JUPYTER_DATA_DIR" "$REPO_ROOT/.venv/bin/jupyter" kernelspec list 2>/dev/null | grep -q "[[:space:]]${ERLANG_KERNEL_NAME}[[:space:]]"; then
  echo "Kernel not found after install: ${ERLANG_KERNEL_NAME}"
  echo "Check kernelspec list: $REPO_ROOT/.venv/bin/jupyter kernelspec list"
  exit 1
fi

echo "Notebook environment ready."

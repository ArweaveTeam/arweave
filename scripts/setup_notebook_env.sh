#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

ERLANG_KERNEL_NAME="${ERLANG_JUPYTER_KERNEL:-erlang}"
IERL_URL="${IERL_URL:-https://github.com/filmor/ierl/releases/latest/download/ierl}"
IERL_PATH="${IERL_PATH:-$REPO_ROOT/.tmp/ierl}"
JUPYTER_DATA_DIR="${JUPYTER_DATA_DIR:-$REPO_ROOT/.tmp/jupyter}"

cd "$REPO_ROOT"

if ! command -v git-lfs >/dev/null 2>&1; then
  cat >&2 <<'EOF'
git-lfs is not installed or not on PATH.
The localnet notebooks need Git LFS to materialise localnet_snapshot/:

  sudo apt install git-lfs           # if git-lfs is not installed
  git lfs install
  git lfs pull
EOF
  exit 1
fi

LFS_POINTER_FILES="$(git lfs ls-files | sed -n 's/^[^ ]* - //p')"
if [ -n "$LFS_POINTER_FILES" ]; then
  cat >&2 <<'EOF'
The repository contains Git LFS pointer files instead of real content.
The localnet notebooks need Git LFS content to be materialised:

  git lfs pull
  git lfs checkout

Pointer files found:
EOF
  printf '%s\n' "$LFS_POINTER_FILES" | sed 's/^/  /' >&2
  exit 1
fi

mkdir -p "$REPO_ROOT/.tmp"
mkdir -p "$JUPYTER_DATA_DIR"

PYTHON_BIN=${PYTHON:-python3}
if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "$PYTHON_BIN is not installed or not on PATH." >&2
  exit 1
fi

# `python -m venv` needs `ensurepip` and `venv` in the interpreter's
# stdlib. On Debian/Ubuntu those modules are split into a separate
# package (e.g. python3.10-venv); without them venv creation aborts
# with "ensurepip is not available", leaving a half-built .venv that
# then errors as "No module named pip". Surface the install hint up
# front instead.
if ! "$PYTHON_BIN" -c 'import ensurepip, venv' >/dev/null 2>&1; then
  PY_VER=$("$PYTHON_BIN" -c 'import sys; print("%d.%d" % sys.version_info[:2])' 2>/dev/null || echo "X.Y")
  cat >&2 <<EOF
$PYTHON_BIN ($PY_VER) is missing the venv stdlib package.

On Debian/Ubuntu install it with:
  sudo apt install python${PY_VER}-venv

Or point \$PYTHON at a full Python install:
  PYTHON=/usr/bin/python3.11 scripts/setup_notebook_env.sh
EOF
  exit 1
fi

# A .venv without pip means an earlier `python -m venv` aborted before
# ensurepip ran; wipe and rebuild so the next `pip install` doesn't
# bail with the cryptic "No module named pip".
if [ -d "$REPO_ROOT/.venv" ] && [ -x "$REPO_ROOT/.venv/bin/pip" ]; then
  echo "Using existing virtual environment at: $REPO_ROOT/.venv"
else
  rm -rf "$REPO_ROOT/.venv"
  "$PYTHON_BIN" -m venv "$REPO_ROOT/.venv"
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

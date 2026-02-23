# Jupyter Erlang Notebooks

This directory contains Jupyter notebooks using the Erlang kernel for interactive Arweave tests.

## Prerequisites

- Python 3 with venv + pip
- Erlang kernel for Jupyter

Quick setup:

```
scripts/setup_notebook_env.sh
```

The setup script writes a local kernelspec under `.tmp/jupyter` and configures
it to call `ierl_kernel.sh` from the `scripts/` directory (added to PATH by
the runner scripts).

On kernel startup, the wrapper compiles the localnet profile (if needed) and
adds `_build/localnet/lib` via `ERL_LIBS`, so local modules are available in
the notebooks.

## Run headless

```
scripts/run_notebook_headless.sh pricing_transition_localnet
```

## Run interactive

```
scripts/run_notebook.sh
```

You can open specific notebooks after jupyter launches.

## Notebook outputs

When using the repo scripts, notebooks are configured to strip cell outputs on
save. To keep outputs in the file for a session, set `NOTEBOOK_SAVE_OUTPUTS=1`
before launching the notebook (interactive or headless).

## Environment variables

- `ERLANG_JUPYTER_KERNEL`: kernelspec name (default `erlang`)
- `IERL_URL`: download URL for the ierl escript
- `IERL_PATH`: local path for the ierl escript (default `.tmp/ierl`)
- `JUPYTER_DATA_DIR`: where kernelspecs live (default `.tmp/jupyter`)
- `JUPYTER_CONFIG_DIR`: where Jupyter config lives (default `.jupyter`)
- `JUPYTER_PORT`: interactive server port (default `8888`)
- `JUPYTER_OPEN_BROWSER`: set to `true` to open a browser (default `true`)
- `EXEC_TIMEOUT_SEC`: nbconvert execution timeout in seconds (default `1200`)
- `LOCALNET_NODE_NAME`: shortname only (default `main-localnet`). If you pass
  `name@host`, the host is used for RPC node selection, but shortnames are
  still used for local nodes.
- `NOTEBOOK_SKIP_COMPILE`: set to `1` to skip compile on kernel startup.

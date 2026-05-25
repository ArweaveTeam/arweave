# Arweave Development

## Building

Use `./ar-rebar3` instead of `./rebar3` for building:
```bash
./ar-rebar3 test compile
```

## Running Tests

```bash
# Run all tests in a module
./bin/test test_module

# Run a specific test
./bin/test test_module:test
```

For example:
```bash
./bin/test ar_unconfirmed_chunk_tests
./bin/test ar_unconfirmed_chunk_tests:get_unconfirmed_chunk_from_disk_pool_test_
```

## Killing BEAMs and EPMDs: never kill another workspace's

This rule covers `beam.smp` and `epmd` processes only — every other process
and all other guidance you have been given still applies as normal.

This host runs multiple workspaces side-by-side (`bin/dev/ws` — see
`doc/workspaces.md`). Each workspace exports its own `ARWEAVE_NAMESPACE`,
which is appended to every Erlang node name (e.g. `main-localtest-<ns>@host`,
`arweave-<ns>@127.0.0.1`), and runs its own private EPMD on `ERL_EPMD_PORT`.

`./bin/stop` and the targeted recipes below are the polite path, but when
`./bin/stop` does not work, or a cancelled test left several BEAMs behind,
`kill` / `pkill` against `beam.smp` and `epmd` are fine — be selective about
*which* ones you kill.

**The rule: a workspace must never kill another workspace's `beam.smp` or
`epmd`** (the peer-workspace agent's work disappears with it). Two cases:

1. **Outside a workspace** (`ARWEAVE_NAMESPACE` is unset): no
   workspace-isolation restrictions on which BEAMs/EPMDs you may kill —
   host-wide `pkill beam` is allowed for BEAM/EPMD cleanup.

2. **Inside a workspace** (`ARWEAVE_NAMESPACE` is set): of the BEAMs and
   EPMDs on the host, only kill
   - your own BEAMs — cmdline contains `-${ARWEAVE_NAMESPACE}[-@]`; or
   - namespace-less BEAMs — `-name <prefix>@host` with no `-<other-ns>`
     suffix at all (they belong to no workspace); and
   - your own EPMD (the one on `ERL_EPMD_PORT`); leave the default EPMD
     on 4369 alone — it is shared.

   Do **not** touch a BEAM whose `-name` argument carries a different
   `-<ns>` suffix. When unsure, `pgrep -af beam.smp` first and read the
   `-name` of each candidate before killing.

Targeted recipes from inside a workspace:

```bash
# Inspect first — never blind-fire pkill at beam.smp.
pgrep -af beam.smp

# Kill only this workspace's BEAMs.
pkill -f -- "-${ARWEAVE_NAMESPACE}[-@]"

# Stop only this workspace's EPMD.
epmd -port "$ERL_EPMD_PORT" -kill
```

Forbidden from inside a workspace (host-wide BEAM/EPMD killers — hit peers):
`pkill beam`, `pkill -f beam`, `killall beam.smp`, `pkill epmd`,
`killall epmd`, `epmd -kill` without `-port "$ERL_EPMD_PORT"`.

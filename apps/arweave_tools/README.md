# arweave_tools

Command-line doctor and benchmark tools, packaged with the release as a
load-only application. It has no supervisor and is not a dependency of the
running node.

## Public interface

`arweave_tools` is the only public module:

- `main/0` reads the launcher's plain arguments.
- `main/1` dispatches a command and stops the VM with its exit status.
- `run/1` dispatches the same command and returns its exit status on completion.
  Existing command-specific fatal-error handling can still stop the VM.

Commands are `["doctor", Command | Args]` and
`["benchmark", Benchmark | Args]`, where `Benchmark` is `"hash"`, `"packing"`
or `"vdf"`. The existing `bin/data-doctor` and `bin/benchmark-*` commands and
their arguments are unchanged. Doctor success returns status 0; benchmark
completion retains the legacy status 1.

All other modules are internal command implementations. Tools use the runtime
applications' code and start only the services needed by each command; loading
the tools application does not start an Arweave node.

## Runtime boundary

`ar_bench_hash` and `ar_bench_vdf` remain in `arweave`: normal node startup also
uses their calibration routines. Their CLI parsing lives here. The snapshot
engine `ar_snapshot` also remains in `arweave`, shared with localnet.

The console-only account-tree benchmark and timing helpers are not part of this
CLI extraction. Wallet creation and configuration conversion also retain their
existing entry points.

## Tests

`scripts/smoke_cli.sh` checks real benchmark dispatch and doctor CLI exit
statuses, including a snapshot command failure. Common Test here covers the
application boundary, exact argument forwarding, doctor dispatch and paths,
and the public API's unknown-command fallback. The snapshot suite also exports
an earlier chain height, rejoins from it, and resumes mining through a real,
isolated test node. It uses Arweave's test helpers without starting a node in
the BEAM running the other tools suites.

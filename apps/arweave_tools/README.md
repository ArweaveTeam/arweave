# arweave_tools

Command-line tools for offline maintenance (`doctor`) and benchmarks. It ships
in the release as a load-only application: it has no supervisor, the node
does not depend on it, and loading it does not start a node.

## Public interface

[`arweave_tools`](src/arweave_tools.erl) is the only public module:

- `main/0` reads the launcher's plain arguments.
- `main/1` runs a command and stops the VM with its exit status.
- `run/1` runs a command and returns its exit status.

The `bin/data-doctor` and `bin/benchmark-*` scripts call `bin/arweave doctor`
or `bin/arweave benchmark`, which start the VM with `-run arweave_tools main`.

## Commands

- **`doctor`** works on a stopped node's data directory: merge storage-module
  data, benchmark storage-module reads, dump blocks and transactions, inspect
  a module's chunk-state bitmap, and export a snapshot to join from.
- **`benchmark`** runs the hash, packing or VDF benchmark.

Doctor commands return 0 on success. Benchmarks and unknown commands return 1.

Each command starts only the services it needs, for example configuration,
the key-value store, or storage in standalone mode.

## External dependencies

`arweave_tools` calls the node's modules directly, without a deps module. The
main ones:

| Feature | Application | Description |
|---|---|---|
| Storage | [`arweave_storage`](../arweave_storage/src/arweave_storage.erl) | Reads and merges storage modules, in standalone mode. |
| Configuration | [`arweave_config`](../arweave_config/src/arweave_config.erl) | Loads the configuration a command needs. |
| Entropy | [`arweave_entropy`](../arweave_entropy/src/arweave_entropy.erl) | Maps entropy over footprints for the packing benchmark. |
| Key-value store | `arweave` ([`ar_kv`](../arweave/src/ar_kv.erl)) | Opens the node's databases for dumps and snapshots. |
| Benchmarks | `arweave` ([`ar_bench_hash`](../arweave/src/ar_bench_hash.erl), [`ar_bench_vdf`](../arweave/src/ar_bench_vdf.erl)) | The hash and VDF benchmark code, which the running node also uses. |
| Snapshots | `arweave` ([`ar_snapshot`](../arweave/src/ar_snapshot.erl)) | Exports a snapshot, with the same engine the local test network uses. |
| Helpers | [`arweave_lib`](../arweave_lib/README.md) | Protocol constants, interval sets and encoding helpers. |

## Tests

```sh
./bin/ct --dir apps/arweave_tools/test
```

The suites cover loading without a node, command dispatch, exit statuses, and
a snapshot exported, rejoined and mined on an isolated test node.
[`scripts/smoke_cli.sh`](../../scripts/smoke_cli.sh) checks the bin wrappers,
benchmarks and doctor exit statuses against a built release.

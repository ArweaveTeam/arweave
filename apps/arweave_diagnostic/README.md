# arweave_diagnostic

Reports on a running node and its BEAM for live debugging: CPU, memory, the
processes and ETS tables using the most memory, sockets and network, the
node's own processes, and DETS and RocksDB usage. Each report is written to
the log at info level and also returned.

## Public interface

[`arweave_diagnostic`](src/arweave_diagnostic.erl) is the only public module:

- `all/0` gathers every report.
- `select/1` gathers the named reports, for example `select([cpu, memory])`.
- `cpu/0`, `memory/0`, `processes/0`, `ets/0` and the other report functions
  each gather one report.

The node runs `all/0` when it receives `SIGUSR1`
([`arweave_config_signal_handler`](../arweave_config/src/arweave_config_signal_handler.erl)).

## External dependencies

| Feature | Application | Description |
|---|---|---|
| RocksDB handles | `arweave` ([`ar_kv`](../arweave/src/ar_kv.erl)) | The RocksDB report reads the open databases from the `ar_kv` ETS table, so it follows that table's record layout. |

The other reports read the BEAM directly.

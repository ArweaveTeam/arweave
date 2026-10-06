# arweave_storage

Everything a node keeps on disk about chunk data: storage-module metadata, the
chunk and entropy files, and the records of which byte ranges and footprints
each storage module holds. It is a service app.

## Public interface

- [`arweave_storage`](src/arweave_storage.erl) is the only public module.
- [`include/arweave_storage.hrl`](include/arweave_storage.hrl) defines
  `#store_info{}`, the metadata that `arweave_storage:store_info/1` returns for
  a storage module: its ID, label, ranges, packing and paths.
- Test builds add `internal_*` functions, guarded by `AR_TEST`.

## How it works

- **Storage-module metadata.** Describes each configured module, and finds the
  modules that cover an offset or range.
- **Chunk files.** Reads, writes and deletes chunks in each module's chunk
  files.
- **Entropy files.** Stores prepared replica.2.9 entropy and the cursors that
  track preparation. Entropy and chunk writes to the same bucket share a lock,
  so the two stay consistent.
- **Records.** Each module's records of the byte ranges and footprints it
  holds, persisted with a write-ahead log and snapshots.
- **Aggregate record.** The union of every module's records, and the sync
  records and sync buckets advertised to peers.
- **Persisted names.** Identifiers such as `ar_chunk_storage` in record IDs,
  RocksDB keys and directory names are data formats, not module names.
  Changing them needs a data migration.

### Querying records

A record query selects three things:

- **The index:** byte records (`{RecordID, byte}`) or footprint records
  (`{ar_data_sync, footprint}`). Byte records give exact coverage; footprint
  records give bucket presence. Each index has its own coordinates, and
  [`arweave_lib_footprint`](../arweave_lib/src/arweave_lib_footprint.erl)
  converts between them.
- **The packing:** a concrete packing, or `any_packing`.
- **The store:** a concrete module ID, or `any_store`. For interval queries,
  `any_store` means the union of the matching modules, so "unsynced" means
  absent from every module.

## Lifecycle

- **Start.** The application root owns the metadata cache and starts without a
  node, so offline tools and simulators can read metadata. Once its key-value
  store, events and packing services are running, the node adds
  `arweave_storage:child_spec/0` to its supervision tree. That starts the
  per-module record workers, the chunk and entropy storage workers, and the
  aggregate record.
- **Standalone.** Offline tools call `arweave_storage:activate(standalone)`,
  which starts the same workers without advertising to peers.
- **Shutdown.** Storage stops after its clients (entropy preparation, data
  sync and repacking) and before the key-value store.
- **Events.** Storage publishes per-packing data sizes on the `chunk_storage`
  event and record changes on the `sync_record` event.

## External dependencies

| Feature | Application | Description |
|---|---|---|
| Key-value store | `arweave` ([`ar_kv`](../arweave/src/ar_kv.erl)) | Persists the byte and footprint records. |
| Events | `arweave` ([`ar_events`](../arweave/src/ar_events.erl)) | Publishes storage events, and delivers record changes to the aggregate record. |
| Clock | `arweave` ([`ar_timer`](../arweave/src/ar_timer.erl)) | System time and timers. |
| Chunk cipher | `arweave` ([`ar_packing_server`](../arweave/src/ar_packing_server.erl)) | Enciphers replica.2.9 chunks with their entropy. |
| Sync buckets | `arweave` ([`ar_sync_buckets`](../arweave/src/ar_sync_buckets.erl)) | Builds the sync buckets advertised to peers. |
| Serialization | `arweave` ([`ar_serialize`](../arweave/src/ar_serialize.erl)) | Encodes packing names. |
| Console | `arweave` ([`ar`](../arweave/src/ar.erl)) | Operator-facing console output. |
| Configuration | [`arweave_config`](../arweave_config/src/arweave_config.erl) | The storage-module, repack and defragmentation settings. |
| Metrics | [`arweave_metrics`](../arweave_metrics/src/arweave_metrics.erl) | Publishes the storage metrics. |
| Constants and geometry | [`arweave_lib`](../arweave_lib/README.md) | Protocol constants, interval sets and chunk geometry. |

`arweave_storage` calls `arweave_lib` directly and reaches every other
application through [`arweave_storage_deps`](src/arweave_storage_deps.erl).
When a prepared module lacks the entropy for a chunk write, storage reports
it. The caller generates the entropy with
[`arweave_entropy`](../arweave_entropy/src/arweave_entropy.erl) and retries
with `{chunk_with_entropy, Chunk, Entropy}`.

## Tests

```sh
./bin/ct --dir apps/arweave_storage/test
```

The suites cover metadata without a node, the runtime and its restarts, chunk
and entropy files, record persistence and replay, and the aggregate record.
Integration with the node is tested with EUnit in `arweave`, for example in
`ar_sync_record_tests` and `ar_entropy_storage_tests`.

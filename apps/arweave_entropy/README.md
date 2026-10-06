# arweave_entropy

Generates, caches and prepares replica.2.9 entropy: the RandomX output that
replica.2.9 packing combines with chunk data. It is a service app.

## Public interface

- [`arweave_entropy`](src/arweave_entropy.erl) is the only public module. It
  generates entropy, single chunks' slices and whole footprints, computes
  entropy keys and offsets, and provides `child_spec/0` for the node's
  supervision tree.
- Test builds add `internal_*` functions, guarded by `AR_TEST`.

## How it works

- **Generation.** Entropy is computed on the node's packing workers, with their
  shared RandomX dataset
  ([`arweave_entropy_generation`](src/arweave_entropy_generation.erl)).
  Concurrent requests for the same entropy share one generation.
- **Cache.** Generated entropy is kept in a cache of
  `packing.entropy.cache_size` MiB, so the footprints being packed or synced
  can reuse it. [`arweave_entropy_cache`](src/arweave_entropy_cache.erl)
  defines eviction.
- **Slicing.** A caller gets a copy of just the slice it needs, so holding a
  slice does not keep the whole entropy in memory.
- **Preparation.** One worker per storage module prepares that module's
  entropy in the background
  ([`arweave_entropy_preparation`](src/arweave_entropy_preparation.erl)).

## Lifecycle

- **Start.** The application root owns the cache tables. Once packing, device
  locks and storage are running, the node adds `arweave_entropy:child_spec/0`
  to its supervision tree, which starts the preparation workers.
- **Shutdown.** The preparation workers stop before storage and packing.

## External dependencies

| Feature | Application | Description |
|---|---|---|
| Entropy files and records | [`arweave_storage`](../arweave_storage/src/arweave_storage.erl) | Stores prepared entropy and preparation cursors, and records which entropy is prepared. |
| Configuration | [`arweave_config`](../arweave_config/src/arweave_config.erl) | The entropy cache size, and the storage and repack modules. |
| Metrics | [`arweave_metrics`](../arweave_metrics/src/arweave_metrics.erl) | Publishes the entropy metrics. |
| Packing workers | `arweave` ([`ar_packing_server`](../arweave/src/ar_packing_server.erl)) | Run entropy generation with their RandomX state. |
| RandomX | `arweave` ([`ar_mine_randomx`](../arweave/src/ar_mine_randomx.erl)) | Computes the entropy. |
| Device locks | `arweave` ([`ar_device_lock`](../arweave/src/ar_device_lock.erl)) | Gives each device one mode at a time. A store prepares entropy only while its device is in prepare mode. |
| Footprint limit | `arweave` ([`ar_footprint_limit`](../arweave/src/ar_footprint_limit.erl)) | Each store's footprint limit, which bounds its preparation. |
| Serialization | `arweave` ([`ar_serialize`](../arweave/src/ar_serialize.erl)) | Encodes packing names. |
| Console | `arweave` ([`ar`](../arweave/src/ar.erl)) | Operator-facing console output. |
| Constants and geometry | [`arweave_lib`](../arweave_lib/README.md) | Protocol constants and the replica.2.9 entropy mapping. |

`arweave_entropy` calls `arweave_lib` directly and reaches every other
application through [`arweave_entropy_deps`](src/arweave_entropy_deps.erl).
When storage reports that a prepared module lacks the entropy for a chunk
write, [`ar_data_sync`](../arweave/src/ar_data_sync.erl) generates it here and
retries the write.

## Tests

```sh
./bin/ct --dir apps/arweave_entropy/test
```

The suites cover the cache, generation, slicing, shared generation and the
lifecycle. Integration with packing and chunk storage in a running node is
tested with EUnit in `arweave`, in `ar_entropy_storage_tests`.

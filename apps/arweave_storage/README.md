# Storage

`arweave_storage` is the public API. It is a thin facade: storage-module
metadata, chunk files, entropy files, persisted sync records, footprint
records and aggregate availability are implemented by app-internal modules.

## Public API

The facade remains a single module, with exports and delegations grouped by
lifecycle, storage module metadata, byte-range records, chunk I/O, entropy,
aggregate availability, footprint records, and geometry. Internal worker
names, file semaphores, intermediate write operations, and internal-only
overloads are not part of this API.

For related metadata, include `arweave_storage/include/arweave_storage.hrl`
and use `store_info/1`:

```erlang
#store_info{
    id = StoreID,
    label = Label,
    configured_range = {Start, End},
    effective_range = {Start, EndWithOverlap},
    padded_range = {PaddedStart, PaddedEnd},
    disk_dir_name = DirectoryName,
    path = ModulePath,
    chunk_storage_path = ChunkStoragePath,
    repack_in_place = IsRepackInPlace,
    packing = Packing,
    mining_address = MiningAddr,
    packing_difficulty = PackingDifficulty
} = arweave_storage:store_info(ModuleOrID).
```

The input can be a `{Start, End, Packing}` tuple or a configured module ID.
ID lookup includes in-place repacking modules and describes their **source**
packing. Missing IDs return `not_found`. The default module has range
`{0, infinity}` and unpacked packing. Metadata is computed on demand, so it
does not retain stale configuration and can be read before storage starts.

The metadata record replaces the individual ID, label, range, padded-range,
packing, directory-name, path and repacking-status getters. Look it up once and
match the fields a caller needs. `padded_range` is `{-1, -1}` for the default
module; a missing module returns `not_found`, not a metadata record.

Labels use the existing runtime metrics-label cache. Before the storage app
starts, `label` is `undefined` (except for the default module); the other
fields remain available to startup and offline-tool callers. Obtain a fresh
record after startup when the label is needed. Directory names preserve the
legacy bucket notation when using a legacy configuration.

Paths use the configured `[data_dir]`. For the default module, `path` is
`data_dir` itself; other modules use `data_dir/storage_modules/disk_dir_name`.
`chunk_storage_path` appends `chunk_storage` to that path. These replace
`storage_module_path/2` and `get_chunk_storage_path/2`; reading metadata does not
create directories. Offline tools must initialize configuration and set
`[data_dir]` before looking up metadata. For another node's paths, query
`store_info/1` on that node. Tuples also support not-yet-configured repack targets.

The function cannot be named `module_info/1`: Erlang reserves that function
for the module's automatically generated runtime introspection API.

### Configured coverage and store selection

`covers_offset(Offset, Packing, StoreID)` checks configured coverage, including
packing overlap. `covers_range(Start, End, Packing, StoreID)` checks full range
coverage **without** overlap, preserving the previous coverage rules. Neither
function checks whether any data has been synced. Use `any_packing` and
`any_store` to include all configured modules; the unbounded default store is
not a configured module and does not make arbitrary offsets covered.

`covering_stores(Offset, Packing)` and
`intersecting_stores(Start, End, Packing)` return module tuples in the existing
reverse configuration order. `any_packing` does not filter; a concrete packing
is a strict filter. `covering_store(Offset, PreferredPacking)` retains the old
selection behavior, where packing is a preference, not a filter.
`covering_ranges(Start, End, PreferredStore)` retains the existing coverage
planning behavior and preferred-module argument.

`chunk_filepath(Name, StoreID)` constructs a path within the chunk directory.
`list_chunk_files(StoreID)` uses the configured data directory and retains the
existing directory-creation behavior of file listing.

### Record and index selectors

Availability calls delegate directly to `arweave_storage_sync_record`, which
resolves index selectors and owns interval queries, mutations, and persistence.
Selectors are converted to the existing record IDs before accessing ETS or disk;
the persisted format is unchanged.

Record query APIs take both packing and store selectors:

```erlang
Record = {ar_data_sync, byte},
arweave_storage:is_recorded(Offset, Packing, Record, StoreID).
arweave_storage:sync_record_exists(Packing, Record, StoreID).
arweave_storage:get_sync_record(Packing, Record, StoreID).
arweave_storage:get_interval(Offset, Packing, Record, StoreID).
arweave_storage:get_intersection_size(End, Start, Packing, Record, StoreID).
arweave_storage:get_next_interval(
    synced, Offset, End, Packing, Record, StoreID).
arweave_storage:get_next_interval(
    unsynced, Offset, End, Packing, Record, StoreID).
arweave_storage:get_intervals(
    synced, Start, End, Packing, Record, StoreID).
```

The selector is `{RecordID, byte}` or `{ar_data_sync, footprint}`. Offsets and
returned intervals use the selected index's **native coordinates**, not an
implicit byte-to-footprint conversion. Use `get_footprint_offset/1` for a point
or `get_footprint_range(Partition, Footprint)` for a whole footprint. Footprint
entries describe bucket presence; byte records remain authoritative for exact
byte coverage. Unsupported index/record combinations are rejected.

Use `any_packing` to read the aggregate record (including untyped entries),
or a concrete packing to read only that packing's record. `any_store` lets
byte point lookups search the default store first, then configured modules covering
the byte; packing-specific point lookups retain the existing preference for
modules configured with that packing. `is_recorded_any/3` remains separate:
it searches only the caller's ordered list of candidate modules.
Footprint point lookups search registered footprint records instead of treating
the index offset as a physical byte when selecting stores.

Point lookup results retain their existing shapes: a concrete packing and
store return a boolean; `any_packing` may return `{true, Packing}` or `true`
for untyped data; `any_store` wraps a hit as `{Hit, StoreID}`. Misses are `false`.

For interval reads, `any_store` uses the union of matching registered records:
`unsynced` means absent from **every** store, not missing from just one.
Intervals are `{End, Start}` (right-inclusive, left-exclusive), or `not_found`.
The end is capped at the supplied bound, which may be `infinity`; a synced
interval's start can precede the cursor. Cross-store reads walk the relevant
intervals without copying entire records into memory.

`sync_record_exists/3` checks for a registered record, even if it is empty.
Whole-record, containing-interval, and intersection-size reads use the same
registered-record selection as next-interval reads. `any_store` merges their
coverage without double-counting overlaps. Only whole-record reads materialize
the entire union; containing-interval and size queries walk the relevant ranges.
Byte point lookup candidate selection remains unchanged and intentionally
differs from the registered-record selection used by interval queries.

`add_sync_record(End, Start, Packing, RecordID, StoreID)` requires a concrete
store. `any_packing` here means an **untyped add** to the aggregate record only;
it does not mark every packing as present. A concrete packing updates both
its packing-specific record and the aggregate record. On-disk keys and WAL
operations are unchanged.
Record selectors also apply to add/delete/cut and whole-record reads. A cut is
in native index order: cutting a byte range must not be translated into a single
footprint-index cut. Chunk-aware footprint mutations retain their geometry.

`get_intervals/6` replaces the separate footprint synced/unsynced collectors and
the byte-only unsynced collector. It clips all results to the supplied bounds.

Calls within storage may use its internal modules directly. Tests owned by
storage can inspect those internals without adding facade exports; production
callers in other applications continue to use only `arweave_storage`.

`get_serialized_buckets(byte | footprint)` exposes the existing cached bucket
formats. `await_entropy_writes(StoreID)` waits for preceding entropy writes
from the same caller to be processed; it does not indicate that entropy
preparation is complete or force an additional filesystem sync.

## Ownership and lifecycle

The application root owns the metadata cache and can start without a node.
After the host's KV, events and packing services are ready, the lifecycle child
returned by `arweave_storage:child_spec/0` activates the storage runtime under
the storage application:

- `arweave_storage_sync_record_sup` owns the record registry and starts the
  per-store persisted-record workers.
- `arweave_storage_chunk_storage_sup` owns the chunk-file index and starts
  chunk and entropy-storage workers.
- `arweave_storage_runtime_sup` owns entropy file semaphores and the cached
  aggregate availability, and supervises the above services plus the global
  sync-record worker.

The host lifecycle child starts before entropy generation, data-sync and
repacking, so storage stops **after** those clients and **before** host KV.
Stopping and restarting the host recreates the storage runtime, while the
metadata application can stay alive. Offline doctor tools use
`activate(standalone)`, without starting peer availability publication or
entropy generation.

Chunk reads still use caller-owned file handles. Sync-record reads still use
ETS directly; neither path goes through a new central coordinator.

## Boundaries

Configuration, metrics and generic interval/geometry helpers are direct app
dependencies. Calls to host-owned KV, events, protocol geometry, bucket
serialization and the pure chunk cipher are collected in
`arweave_storage_deps`. Shared protocol headers remain compile-time
dependencies. This extraction does not move block/transaction storage,
`ar_data_sync`, packing, device scheduling or the shared
chunk cache.

Entropy generation, caching and preparation belong to `arweave_entropy`;
storage has no dependency on that app. Storage retains the preparation cursor
files and atomic coordination of entropy/chunk writes. A missing entropy in an
already-prepared store is reported to the caller, which can generate it outside
storage and retry with `{chunk_with_entropy, Chunk, Entropy}`.

Storage publishes persisted per-packing data sizes through the `chunk_storage`
event stream. Mining statistics subscribe and read the current snapshot when
starting, so storage does not call mining processes.

Persisted record IDs, RocksDB keys, WAL/snapshot layouts, storage directories
and chunk-file layouts are unchanged. In particular, identifiers such as
`ar_chunk_storage` remain data-format keys, not module names to rename.

## Tests

`arweave_storage_SUITE` covers metadata without a node.
`arweave_storage_services_SUITE` exercises runtime ownership, host-style
restart, real chunk files, global record aggregation, legacy snapshots and WAL
replay using only the host KV/event helpers. Genesis-data seeding remains an
Arweave-node integration test in `ar_sync_record_tests`.
Storage-module, interval geometry, chunk I/O, entropy locking and record
persistence tests use Common Test suites under `test/`. Cross-app tests stay
in `arweave/test`, including prepared-entropy packing in
`ar_entropy_storage_tests`.

Cross-app corruption tests use `arweave_storage:internal_write_chunk/3` and
`internal_erase_chunk/2` to change raw chunk bytes while deliberately retaining
records. Their exports and implementations are guarded by `AR_TEST`; production
builds do not expose these operations.

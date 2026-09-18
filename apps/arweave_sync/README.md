# Network synchronization

`arweave_sync` owns peer discovery, store sweep cursors, chunk selection,
download scheduling, per-peer concurrency, footprint reservations, fetched-chunk
validation/unpacking orchestration, and network-task lifecycle. It does not own
persisted sync records, protocol validation algorithms, or chunk storage.

## Boundaries

Storage-module metadata comes directly from the public `arweave_storage` API.
The simulator uses the same metadata implementation and application-owned cache.

`arweave_sync.erl` and `arweave_sync.hrl` are the public interface. All other
modules and headers are internal to the app, without an `_internal` suffix.

`arweave_sync` is the host's entry point for registering writers, starting stores,
and updating sync bounds. Each store has a sync-owned ingestion
process for fetched chunks and network task references. Local-copy and disk-pool
producers call the host's storage interface directly, without entering sync
ingestion.
`ar_data_sync` receives storage requests with opaque reply references and shared
chunk cache references, and replies with
`{chunk_store_result, RequestRef, Result}`.
It does not interpret sync task references or manipulate sync counters.
Results distinguish `stored`, skipped,
and failed requests. Sync samples host completion counters for all producers,
so local-copy and disk-pool writes contribute to the store's drain-rate estimate.

`ar_chunk_cache`, owned by the host supervisor, admits chunks before reads or
fetches. A reservation follows each chunk through unpacking, packing and storage;
the allowance covers input, unpacked intermediate and packed output together.
Stages retain the same reservation while holding payloads, including queued
packing replies. The final holder releases capacity. Process death reclaims only
that holder, and coordinator restart reconstructs live ownership from the
supervisor-owned table. The coordinator never receives chunk payloads.

`packing.cache_size` (MiB) is the shared chunk cache for sync, local copying,
disk-pool storage and repacking.
Each chunk reserves three 256 KiB representations, so 768 MiB admits 1024 chunks.
Legacy `data_cache_size_limit` counts 256 KiB chunks and converts to MiB,
rounding up. The same payload budget now admits fewer chunks because it also
covers transformations. Legacy `packing_cache_size_limit` is ignored with a
warning; it does not create a separate packing cache or override this limit.
Entropy remains a distinct cache; automatic sizing accounts for entropy and peer
intervals, and configuration validation rejects combined caches exceeding
system/container RAM. These budgets do not bound total node RSS.

`disk_pool.max_buffer_size` remains separate: it limits pending chunks on disk
and contributes to the free-disk-space reserve, not the in-memory chunk cache.

`arweave_sync_deps` is the inventory of replaceable dependencies. It selects
modules such as `storage()`, `clock()` and `chunk_cache()`; callers use those
modules' existing APIs instead of adding a forwarding callback per operation.
The mainnet implementation only returns modules. Fetched chunks go directly
from the fetch worker to sync's internal ingestion module. Peer metadata requests
use the selected `http()` module's byte and footprint endpoints directly.

Missing records are queried through `arweave_storage:get_intervals/6`. The
sweeper applies blacklist and footprint-limit policy to those records. Ingestion
interprets raw storage-presence and disk-pool results; adapters do not make
those decisions.

`arweave_sync_deps_sim` can select itself for simulated APIs or select real
helpers directly. It keeps network and availability modeling together,
backed by `arweave_sim`, without requiring a separate module for every service.
Production always selects the mainnet adapter; dependency overrides exist only
in test builds and remain VM-wide.

OTP, `arweave_config`, `arweave_metrics`, `arweave_util` and `arweave_storage`
are direct dependencies. Pure storage geometry uses the public storage API
directly. The constants selector uses `arweave_constants` in production and
simulation; focused tests can substitute geometry without duplicating helpers
in the simulator. Generic interval operations live in `arweave_util`.

Persisted sync/footprint records and chunk/entropy storage belong to
`arweave_storage`. Sync reads records through the selected public storage API.
The bucket representation and serialization (`ar_sync_buckets`) remain in
`arweave`. Discovery accesses holdings and iterates advertised buckets through
the dependency adapter.

## Lifecycle

The OTP application starts its own root supervisor and tables before Arweave's
storage services. Network workers are activated separately when the host is ready:

- `arweave_sync_sup` owns all sync tables, the lifecycle coordinator, ingestion
  workers for each store, and the network pipeline supervisor.
- Each `ar_data_sync:init/1` registers its writer through `arweave_sync`.
  Restart registration reclaims only that store's lost network work. An
  ingestion restart cancels the old client's pending storage requests;
  late results cannot complete new requests. Shared reservations survive as
  long as another processing stage still holds the chunk.
- `arweave_sync_pipeline_sup` supervises discovery, scheduler and sweepers as a
  `one_for_all` group. Tables and ingestion survive pipeline restarts. Bounds and
  local-copy readiness are restored when sweepers restart.
- Arweave activates this pipeline after `ar_sup` starts, and quiesces sync before
  shutting down its storage services. A zero download rate pauses new fetches
  and sweeps while leaving the pipeline available for runtime resume. In-flight
  work may finish; host storage admission and accounting remain available.
- `ar_data_sync_sup` owns the local-copy service alongside the storage workers.

Boundary tests reject direct host calls from every module in the app.
They also inspect module-selector calls to verify that production and simulated
dependencies export the APIs used, and that the mainnet adapter only selects
external modules.
Arweave depends on `arweave_sync`, not vice versa: host services are reached only
after activation/registration. No persisted-data migration is required.

When reusing an older build directory, clean that build profile before
building this layout. Obsolete BEAM files under `arweave/ebin` can otherwise
clash with the same modules in their new applications.

## Tests

Run the app's native Common Test suites with:

```bash
./bin/ct --dir apps/arweave_sync/test
```

They cover peer control, footprint lifecycle, store capacity, chunk
selection, fetch-worker outcomes, download limits, reservation accounting,
and the app boundary.
They do not start the `arweave` application. Private test exports and internal
record headers preserve the focused assertions previously embedded in source;
they are not additions to the production API.

Cross-app integration tests use `arweave_sync:internal_*` operations compiled only
under `AR_TEST`. These enqueue real scheduler work, supply controlled fetch
results, and observe writer readiness and fetch activity without exposing task
records or ingestion messages. Fixture implementation stays in the app's test
directory; the public facade only delegates to it or internal services.

The sync simulator runner, dependency adapter and scenario suite live in this
app's test directory. They cover the performance scenarios, runtime download-rate
changes and cache-budget configuration; a separate adapter suite checks peer
metadata. The reusable clock and six world-model cases remain in `arweave_sim`.
The fixture activates the app-owned pipeline and controls the simulated clock.
Network, writes and device readiness are simulated.
The fixture replaces internal ingestion with modeled writes, without recording
per-chunk mock history, and restores ingestion after stopping the pipeline.
Real Arweave protocol geometry, bucket serialization and entropy-cache eviction
remain test-only dependencies, preserving their existing coverage without
starting a node or peer cluster. Scenarios remain sequential because the clock and
dependency overrides are VM-wide. Run just these suites with:

```bash
./bin/ct --suite apps/arweave_sync/test/arweave_sync_sim_SUITE.erl,apps/arweave_sync/test/arweave_sync_deps_sim_SUITE.erl
./bin/ct --suite apps/arweave_sync/test/arweave_sync_discovery_SUITE.erl
./bin/ct --suite apps/arweave_sync/test/arweave_sync_fetch_worker_SUITE.erl
./bin/ct --suite apps/arweave_sync/test/arweave_sync_scheduler_SUITE.erl,apps/arweave_sync/test/arweave_sync_store_sweeper_SUITE.erl,apps/arweave_sync/test/arweave_sync_chunk_cache_SUITE.erl
./bin/ct --dir apps/arweave_sim/test
```

The scheduler, sweeper, and sync-side cache lifecycle tests run here as standalone
Common Test suites. They use real sync code and test-only host helpers without
starting an Arweave node.

Host-owned and cross-app tests remain EUnit in `arweave`: local writer/cache
handoffs live in `ar_data_sync_storage_requests_tests`; HTTP metadata tests,
genesis-to-sync-record integration, and data-sync restart tests also remain
there. Record aggregation is covered in `arweave_storage_services_SUITE`, while
bucket coverage is embedded in `ar_sync_buckets`. For example:

```bash
ARWEAVE_NAMESPACE=sync-restart ./bin/test ar_data_sync_restart_tests
ARWEAVE_NAMESPACE=sync-buckets ./bin/test ar_sync_buckets
```

**This is an alpha update and may not be ready for production use. This software was prepared by the Digital History Association, in cooperation from the wider Arweave ecosystem.**

## NASA Integration

This release includes several updates required by the [Network Availability Stacking Alpha (NASA)](https://ao.arweave.net/#/blog/nasa). Please visit the [#nasa channel](https://discord.com/channels/829732720505520150/1494561608637091900) on the Arweave Miners discord for more information. If you are running a NASA node we recommend you upgrade to this release.

### `absolute_end_offset` included in GET /chunk and GET /chunk2 responses

Both `GET /chunk` and `GET /chunk2` now include a `arweave-absolute-end-offset` HTTP header in their response. `GET /chunk` also includes the `absolute_end_offset` JSON key in its HTTP body.

### `GET /unconfirmed_chunk/{TXID}/{offset}` endpoint

The new `GET /unconfirmed_chunk/{TXID}/{offset}` endpoint provides access to chunk data that has been recently received by the node, exists in its disk pool, but may not yet have been confirmed.

## Footprint-based replica 2.9 syncing

This release introduces footprint-based syncing for replica 2.9 data. Instead of syncing chunk-by-chunk (which caused redundant entropy generation), the node now groups chunks by their entropy footprint and syncs them together.

### How it works

Replica 2.9 chunks share entropy in groups called footprints. A full footprint is 1024 chunks spread evenly across a partition. In prior releases, syncing chunk-by-chunk meant the same entropy was regenerated many times. This release alternates between two syncing phases:

- **Normal phase**: syncs non-replica-2.9 data (unpacked, spora_2_6, etc.) in the traditional left-to-right manner. Also handles disk pool syncing.
- **Footprint phase**: syncs replica 2.9 data footprint-by-footprint, so all chunks sharing the same entropy are fetched together.

The node limits the number of concurrently active footprints based on the configured `replica_2_9_entropy_cache_size_mb` to avoid entropy cache overload.

### New endpoints

- `GET /footprint_buckets`: returns a compact representation of footprint availability, similar to sync buckets.
- `GET /footprints/{partition}/{footprint}`: returns the detailed footprint intervals for a given partition and footprint number. Peers use this for footprint-based data discovery. Note that footprint records are maintained for all packing types (not just replica 2.9), making it convenient for any client to query data availability using the footprint-based layout.

Both endpoints require release 89 or later. Footprint syncing gracefully falls back when communicating with older peers.

### Sync record to footprint record migration

On first startup after upgrade, the node automatically migrates its existing sync records into the new footprint record format. This is a one-time, non-destructive migration.

#### Migration detailsab

- The migration runs in the background, processing 200 chunks per batch with a 1-second delay between batches.
- Progress is checkpointed, so if the node restarts, migration resumes from where it left off.
- **Footprint-based syncing is disabled until the migration completes.** Normal syncing proceeds as usual during the migration.

#### Implications for operators

- No new configuration is required. The migration starts automatically.
- Depending on the amount of synced data, the migration may take some time. Progress is logged (`initializing_footprint_record` / `footprint_record_initialized` events).
- Once migration is complete, nodes begin participating in footprint-based syncing.

## Data root syncing

This release introduces a background data root syncing process. Each storage module now runs a process that periodically scans its range (every 10 minutes) and ensures data roots are recorded for all blocks in that range. Missing data roots are fetched from peers via `GET /data_roots/{offset}`. You can disable this background process by setting `enable_data_roots_syncing false` (it is enabled by default).

### `POST /data_roots/{offset}`

A new endpoint, `POST /data_roots/{offset}`, allows pushing data roots to a node. The endpoint:

1. Validates the offset is within the disk pool threshold and maps to a known block.
2. Checks whether data roots are already synced for that block (returns 200 immediately if so).
3. Validates the submitted data roots: verifies the block size matches, recomputes the TX root from the entries, and verifies all Merkle paths.
4. Stores the validated data roots.

The `POST /chunk` endpoint requires the receiving node to have the data root for the chunk being submitted. A node that has not synced the data root index for a given block will reject chunks from that block. By pushing data roots via `POST /data_roots/{offset}`, you can ensure the receiving node is ready to accept chunks from arbitrary historical blocks. This may be useful e.g. for pushing historical chunks via `POST /chunk`.

## Packing benchmark rewrite

The `benchmark packing` command (formerly `benchmark 2_9`) has been rewritten to provide realistic performance measurements that account for disk I/O, not just CPU.

### What changed

The old benchmark only measured CPU-bound entropy generation and reported inflated rates (often >1 GiB/s) because it ignored disk I/O entirely. The new benchmark has two phases:

- **Phase 1 (Entropy Preparation)**: measures entropy generation rate and disk write rate separately, identifies which is the bottleneck, and extrapolates the preparation time for a full partition.
- **Phase 2 (Packing)**: measures the complete packing cycle — unpack entropy generation, decipher (XOR unpack), disk read (pack entropy from disk), encipher (XOR pack), and disk write (packed chunk). Identifies the bottleneck among all 5 operations.

### Realistic disk I/O simulation

- Uses the same file layout as `ar_chunk_storage` (spread-out writes matching the production entropy storage pattern).
- Simulates background disk contention with configurable read load threads performing random 4-64KB reads (typical RocksDB access pattern).
- Filters out cached writes using the `rated_speed` parameter — samples faster than the rated disk speed (with a 10% margin) are excluded.
- Syncs files and drops page cache between phases to ensure reads hit disk.

### Usage

```
benchmark packing [options]
```

Options:
- `threads` — number of threads (default: CPU cores)
- `samples` — number of samples to average (default: 20)
- `large_pages` — use large pages for RandomX, 0=off, 1=on (default: 1)
- `rated_speed` — expected disk write speed in MB/s, used to exclude cached samples (default: 250)
- `read_load` — background read threads simulating other disk activity (default: 2)
- `read_file_gb` — size of read load file in GB (default: 4)
- `dir` — directory to write to; if omitted, runs CPU-only benchmark

Examples:
```
benchmark packing threads 8 dir /mnt/storage1
benchmark packing rated_speed 246 dir /tmp/bench
```

## Mining cache bug fixes

This release fixes a mining cache management bug where entries could become orphaned when chunks fail to read from disk.

### The problem

When a chunk read failed during mining (e.g., the chunk was missing from the recall range tail), the cache reservation for that chunk was not released. Over time, orphaned entries accumulated and consumed all reserved cache space, preventing new `compute_h0` tasks from making progress. This inefficiency was typically reset by new VDF sessions.

### The fix

- When a chunk read or store operation fails, the cache reservation for that sub-chunk is now properly released.
- If both chunks have failed for a given nonce, the entry is dropped from the cache entirely — preventing orphaned entries from filling the cache.

## Rate-limiting reworked

A new rate limiting model has been introduce that allows finer tuning of allowed load. It combines a sliding window and a leaky bucket token limiter. It also allows limiting concurrent requests per limiter groups. 

Precedence of rate limiting:
- Concurrency
- Sliding Window
- Leaky bucket token

If configured concurrency limit is breached for a limiter group, requests are rejected. 
If configured Sliding Window rate limit is breached, it falls back to Leaky bucket token limiter. If the group burned all the leaky tokens, the request is rejected.

The current version keeps default config values to preserve previous default behaviour. Further documentation is available on the website.

Please see:
https://docs.arweave.org/developers/mining/operations/rate-limiting

## P3 Removed

All Permaweb Payment Protocol (P3) functionality has been removed. Future support for decentralized payments will make use of [HyperBEAM](https://github.com/permaweb/HyperBEAM) integrations.

## EPMD leak improvement

### Symptoms
- When restarting, or stopping Arweave nodes `epmd` might have had terminated `arweave` Erlang node still registered, and stuck

### Solution

Provided code change for graceful shutdown of Arweave to help `epmd` deregister Erlang node.

## Config validation

On startup, config validation now won't fail if peers (member of any configured peer list - trusted_peer, local_peer, peer) can't be validated. A warning message will be emmitted and the peer will be ignored.

## arweave-denomination header

All `price` endpoints now include an `arweave-denomination` HTTP header that states the denomination of the price value. To date and for the foreseeable future the Arweave denomination is 1. This is unlikely to change for many years or perhaps decades, if ever. For more information please see [the Denomination guide](https://docs.arweave.org/developers/development/overview/denomination).

## This release introduces various stability and validation enhancements.

Several input validation steps could crash on invalid values, in some cases halting the arweave node.
The patch includes graceful validation of certain inputs and defensive deserialization of local binaries.

## Community involvement

A huge thank you to all the Mining community members who contributed to this release by identifying and investigating bugs, sharing debug logs and node metrics, and providing guidance on performance tuning!

Discord users (alphabetical order):
- BerryCZ
- Butcher_
- doesn't stay up late
- EvM
- Evalcast
- JF
- JamJunJ
- lawso2517
- Niya
- Qwinn
- T777
- timothynode
- Vidiot
- zip
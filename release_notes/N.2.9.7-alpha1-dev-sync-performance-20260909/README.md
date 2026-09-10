# Arweave 2.9.7-alpha1-dev-sync-performance-20260909 Release Notes

**This is a development prerelease containing changes under active
development. It may not work correctly, and some changes may not be included
in a regular release.**

Based on Arweave 2.9.7-alpha1, this release introduces adaptive per-peer sync
concurrency, better balancing across storage modules, localized backpressure
for slow peers and disks, and lower sync-record snapshot memory use.

## Configuration changes

- `sync_jobs` and `[sync, jobs]` are removed. To limit your sync rate, use
  `[sync, max_download_rate]` or `sync_max_download_rate` in bytes per second,
  or `infinity` (the default). A value of `0`
  disables syncing.
- `[sync, max_concurrent_peer_scans]` and the legacy
  `data_discovery_max_concurrent_peer_scans` option have been removed without
  replacement. Discovery concurrency is automatic.
- `[sync, cache_size_limit]` is now `[sync, cache_size]`, measured in MiB.
- `[network, client, http, connections_per_peer]` is new and defaults to `8`.
  Most operators should leave it unchanged.

Other option changes are below. Legacy flat names remain supported and unchanged.

| Old option | New option |
| --- | --- |
| `[disk_pool, jobs]` | `[disk_pool, workers]` |
| `[gossip, header_sync_jobs]` | `[gossip, header, workers]` |
| `[gossip, header_cache_size]` | `[gossip, header, cache_size]` |
| `[network, server, tcp, ...]` socket settings | `[network, server, socket, ...]` |
| `[network, client, tcp, ...]` | `[network, client, socket, ...]` |
| Server listener, connection, and idle settings | `[network, server, http, ...]` |
| Server shutdown and socket-backend settings | Nested `shutdown` and `socket` paths |

## Replica 2.9 guidance

Preparing entropy first remains recommended: start with
`[sync, max_download_rate]` set to `0`, wait for preparation, then restart with
a positive rate or `infinity`. 

## Metrics and compatibility

Custom dashboards may need updating. Key replacements are
`chunk_write_rate_bytes_per_second` → `store_drain_rate_bytes_per_second`,
`peer_interval_cache_*` → `chunk_interval_cache_*`, and `data_discovery` →
`sync_discovery_peers`. New `sync_peer_*`, `sync_tasks_by_*`, `sync_claimed_*`,
and `sync_sweep_offset` metrics describe scheduler behavior.

If downgrading to 2.9.7-alpha1 after running this
release, stop the node and remove the `peers` file from `data_dir`; the older
release cannot read the new peer-performance cache format, and the cache will
be rebuilt automatically.

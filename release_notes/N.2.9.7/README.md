This release introduces various bug fixes, stability improvements and significantly improved replica 2.9 syncing. The new approach stabilizes syncing and increases its throughput by maximizing healthy utilization of various hardware setups and network environments.

**Configuration note:** the vdf-server-5.arweave.xyz VDF server is no longer operational. Please remove vdf-server-5.arweave.xyz from your settings.

This release includes changes from all of these alpha releases:

- [2.9.6-alpha1](https://github.com/ArweaveTeam/arweave/releases/tag/N.2.9.6-alpha1)
- [2.9.6-alpha2](https://github.com/ArweaveTeam/arweave/releases/tag/N.2.9.6-alpha2)
- [2.9.7-alpha1](https://github.com/ArweaveTeam/arweave/releases/tag/N.2.9.7-alpha1)
- [2.9.7-alpha2](https://github.com/ArweaveTeam/arweave/releases/tag/N.2.9.7-alpha2)

**Highlights:**

- Fixed a mining cache leak when a chunk fails to read from disk, which could stall hashing until the next VDF session.
- The account tree processing during startup and block application has been improved.
- `GET /metrics` is served from a cache and stays fast under load.
- `GET /sync_buckets` is now faster.
- `GET /chunk` and `GET /chunk2` include an `arweave-absolute-end-offset` header, and `GET /chunk` also includes `absolute_end_offset` in its body.
- New `GET /unconfirmed_chunk/{TXID}/{offset}` serves chunks from the disk pool that may not be confirmed yet.
- New `GET /footprint_buckets` and `GET /footprints/{partition}/{footprint}` describe footprint availability.
- The miner now switches between VDF sessions more efficiently.
- Upgraded dependencies, including Erlang 27, which is now the minimum required version.
- Added support for HTTP/2, turned on by default.
- Improved peer discovery and connection management.
- Tuned rate-limiting and throttling.
- Mempool processing is faster now.
- The data range webhook no longer fails.
- Fixed footprint batching at partition boundaries.

**The configuration has been updated substantially.** See [the documentation](https://docs.arweave.org/developers/mining/readme/configuration) for more details.

A storage module can now be defined by an explicit `range_start` and `range_end` instead of the bucket size and index notation, which allows arbitrary byte ranges. A module is defined either by a `partition` or by `range_start` and `range_end`. The old notation is still accepted through the legacy configuration format.

**Some notable configuration updates:**

- `./bin/arweave convert_config yaml config.json config.yaml` (or `json`) converts an existing configuration file to the new format.
- On startup, a configured peer that cannot be validated is now skipped with a warning instead of failing the startup.
- `data_dir` now defaults to `./data` instead of the current directory.
- A number of configuration options were renamed:
  | Old name | New name |
  |---|---|
  | `disk_pool.jobs` | `disk_pool.workers` |
  | `gossip.header_sync_jobs` (legacy `header_sync_jobs`)  | `gossip.header.workers` |
  | `gossip.header_cache_size` | `gossip.header.cache_size` |
  | `network.server.transport.idle_timeout` | `network.server.http.idle_timeout` |
  | `network.server.shutdown_connection_timeout` | `network.server.shutdown.connection_timeout` |
  | `network.server.shutdown_mode` | `network.server.shutdown.mode` |
  | `network.server.socket_backend` | `network.server.socket.backend` |
  | `network.server.tcp.{listener_shutdown, max_connections, num_acceptors}` | `network.server.http.*` |
  | `sync.jobs` (legacy `sync_jobs`) | Deprecated. To turn syncing off, set `sync.max_download_rate` to 0. |
  | `sync.cache_size_limit` | `packing.cache_size`, now in MiB instead of chunks |
  | `data_cache_size_limit` | `packing.cache_size`. The legacy name is still read in chunks and converted to MiB. |
  | `network.client.tcp.{delay_send, keepalive, linger, linger_timeout, nodelay, send_timeout, send_timeout_close}` | `network.client.socket.*` |
  | `network.server.tcp.{backlog, delay_send, keepalive, linger, linger_timeout, nodelay, send_timeout, send_timeout_close}` | `network.server.socket.*` |
  | `packing.cache_size` (legacy `packing_cache_size_limit`) | Same name, but now in MiB instead of chunks and shared across syncing, copying and packing. Existing values are not converted, the legacy name is ignored. |

**Downgrading to 2.9.6 is supported:**

- 2.9.6 only reads the legacy `config.json` format and refuses unknown options, so you may want to keep your 2.9.6 configuration.
- 2.9.6 does not support storage modules defined by `range_start` and `range_end`.
- If you then upgrade to 2.9.7 again, some chunks may be downloaded twice while the footprint records catch up.

**This is an alpha update and may not be ready for production use. This software was prepared by the Digital History Association, in cooperation from the wider Arweave ecosystem.**

## Configuration

Node configuration can now be inspected and changed directly from the
`arweave` command, on top of the `arweave_config` system.

- `arweave config get [option]` — print the current value of an option (or the
  whole config). Values are returned as flattened strings, and the output of
  `config get` can be fed straight back into `config set`.
- `arweave config set <option> <value>` — set an option, including list options
  such as `peers.local`. Arbitrary quoting in values is handled correctly, and
  IPs are formatted in dotted notation.
- `arweave config help [group]` — show help for a configuration group. `config
  help` runs even while the node is running.

To support this feature, configuration parameters have been renamed to allow
grouping, and reflect their hierarchy.

New ways of supplying configuration to arweave have been implemented.

- New, hiearchic JSON and YAML config files
- `--config_param` Command line flags with double-dash allow access to the renamed
  configuration parameters when calling start scripts.
- `AR_CONFIG_PARAM=VALUE` Every configuration option can be set through an environment variable.

Both new and legacy configuration options are available to allow users time to adapt.

Please, see the following link to learn more about configuring your arweave node:
https://docs.arweave.org/developers/mining/readme/configuration

### Configuration migration tool

A CLI tool has been added to arweave to convert old style configuration files
to new JSON or YAML configuration files.

`./bin/arweave convert_config yaml config.json config.yaml`

or

`./bin/arweave convert_config json config.json config-new.json`

Please, see the following link for detailed information on migrating your configuration:
https://docs.arweave.org/developers/mining/readme/configuration/migrating-config

## Storage modules defined by byte range

Storage modules are now defined by an explicit `range_start` / `range_end` byte
range rather than the older `bucket_size` / `index` (bucket) notation. Any range
with `range_end > range_start` is valid, giving operators arbitrary byte-range
storage modules. A module is defined by either a `partition` or an explicit
`range_start` + `range_end` (mutually exclusive). The pre-2.9.6 bucket notation
is still accepted through the legacy config parser for backward compatibility.

https://docs.arweave.org/developers/mining/readme/configuration/migrating-config

## Rate Limiter and Throttling

The rate limiter is still the sliding window limiter integrated with leaky bucket
tokens limiter. However several improvements and fixes have been applied.

Please, see the following link for detailed information on rate-limiting and configuring
your node's rate-limiting: https://docs.arweave.org/developers/mining/operations/rate-limiting

### Headers

It is using new naming convention for the different limiting groups in the HTTP headers.
There is an additional header that is not part of the Polli IETF draft.
https://www.ietf.org/archive/id/draft-polli-ratelimit-headers-02.html

### Throttling

The client throttling mechanism has been replace with a fully-adaptive throttling
logic. Throttling doesn't need configuration parameters, and it will
acquire the remote peer's settings after the first request sent to a compatible
host. Traffic to incompatible peers is not throttled.

The new throttling solution relies on appropriately named limiting pools that
have been introduced in this release.

Peer configured as `local_peers` are not throttled.

## Account tree rewritten (ETS-based)

The account (wallet) tree has a new ETS-based implementation with a shared
Patricia-tree core, alongside the retained legacy implementation for reference
and data compatibility.
- **Streamed initialization** of the account tree, with garbage collection and
  hibernation after initialization to reduce memory pressure.
- More resilient persistence: transient RocksDB errors during node persistence
  and batch account storage are retried.
- A bug was fixed where a stale `ar_account_tree` could prevent a node from
  restarting after a crash.
- Extensive new metrics for observability, including
  `account_tree_call_duration_milliseconds`, `account_tree_sink_move_hops`,
  `account_tree_rehashed_nodes`, `ar_wallets_bytes_total`,
  `account_tree_ets_bytes`, and `ar_storage_queue_len`.
- New `logging.patricia` dynamic option to track account-tree hashing progress.

## Faster metrics endpoint

Prometheus metrics were extracted into a dedicated `arweave_metrics`
application, and `GET /metrics` now serves from a cache. The endpoint stays fast
regardless of node load or the number of metrics.

## Stability and validation fixes

Several input validation steps could crash on invalid values, in some cases halting the arweave node.
The patch includes graceful validation of certain inputs and defensive deserialization of local binaries.

## Community involvement

A huge thank you to all the Mining community members who contributed to this release by identifying and investigating bugs, sharing debug logs and node metrics, and providing guidance on performance tuning!

Discord users (alphabetical order):

**This is an alpha update and may not be ready for production use. This software was prepared by the Digital History Association, in cooperation from the wider Arweave ecosystem.**

**This Arweave node implementation proposes hard fork 2.9.6 that activates at height 2,000,000, approximately 2026-09-13 12:00 UTC. This software was prepared by the Digital History Association, in cooperation from the wider Arweave ecosystem.**

Format 1 transactions have been deprecated since 2020 and now represent less than 0.7% of all Arweave L1 transactions. As of the hard fork activation height, Format 1 transactions will no longer be accepted by the Arweave network.

## Configuration

Arweave node configuration has been completely rewritten. For details please
see the update docs: [https://docs.arweave.org/developers/mining/readme/configuration](https://docs.arweave.org/developers/mining/readme/configuration)

### New Option Names

All configuration options have been renamed to allow grouping, and reflect their hierarchy.
Legacy options are still supported.

The new option names and their descriptions can be seen through an updated `arweave config help`.

### New Option Formats

New ways of supplying configuration to arweave have been implemented.

- New, hierachic JSON and YAML config files
- `--config_param` Command line flags with double-dash allow access to the renamed
configuration parameters.
- `AR_CONFIG_PARAM=VALUE` Every configuration option can be set through an environment variable.
- Legacy command line flags and config.json format are still supported.



### Dynamic Set/Get

All options can now be queried directly from a running node, and many options can be changed
without needing to restart the node.

- `arweave config get [option]` — print the current value of an option (or the
whole config). Values are returned as flattened strings, and the output of
`config get` can be fed straight back into `config set`.
- `arweave config set <option> <value>` — set an option, including list options
such as `peers.local`.



### Configuration migration tool

A CLI tool has been added to arweave to convert old style configuration files
to new JSON or YAML configuration files.

`./bin/arweave convert_config yaml config.json config.yaml`

or

`./bin/arweave convert_config json config.json config-new.json`

Please, see the following link for detailed information on migrating your configuration:
[https://docs.arweave.org/developers/mining/readme/configuration/migrating-config](https://docs.arweave.org/developers/mining/readme/configuration/migrating-config)

## Storage modules defined by byte range

Storage modules are now defined by an explicit `range_start` / `range_end` byte
range rather than the older `bucket_size` / `index` (bucket) notation. Any range
with `range_end > range_start` is valid, giving operators arbitrary byte-range
storage modules. A module is defined by either a `partition` or an explicit
`range_start` + `range_end` (mutually exclusive). The pre-2.9.6 bucket notation
is still accepted through the legacy config parser for backward compatibility.

[https://docs.arweave.org/developers/mining/readme/configuration/migrating-config](https://docs.arweave.org/developers/mining/readme/configuration/migrating-config)

## Rate Limiter and Throttling

The rate limiter is still the sliding window limiter integrated with leaky bucket
tokens limiter. However several improvements and fixes have been applied.

Please, see the following link for detailed information on rate-limiting and configuring
your node's rate-limiting: [https://docs.arweave.org/developers/mining/operations/rate-limiting](https://docs.arweave.org/developers/mining/operations/rate-limiting)

### Headers

It is using new naming convention for the different limiting groups in the HTTP headers.
There is an additional header that is not part of the Polli IETF draft.
[https://www.ietf.org/archive/id/draft-polli-ratelimit-headers-02.html](https://www.ietf.org/archive/id/draft-polli-ratelimit-headers-02.html)

### Throttling

The client throttling mechanism has been replace with a fully-adaptive throttling
logic. Throttling doesn't need configuration parameters, and it will
acquire the remote peer's settings after the first request sent to a compatible
host. Traffic to incompatible peers is not throttled.

The new throttling solution relies on appropriately named limiting pools that
have been introduced in this release.

Peer configured as `local_peers` are not throttled.

## Account Tree Performance Improvements

The account (wallet) tree has been re-implemented to improve tree initialization and
update times by an order of magnitude. These improvements are visible now via slightly
faster node launch and block processing times, and ensure efficient scalability as
the number of accounts grow in the future.

## Faster metrics endpoint

`GET /metrics` now serves from a cache. The endpoint stays fast
regardless of node load or the number of metrics.

## Additional Fixes

- Several input validation steps could crash on invalid values, in some cases halting the arweave node.
The patch includes graceful validation of certain inputs and defensive deserialization of local binaries.
- Previously repack-in-place from `replica.2.9` to `unpacked` would occasionally stall with only a few
GB left in the storage module. This has been fixed.
- In order to simplify repack-in-place we've removed the `repack_cache_size_mb` option. Repack-in-place
can now be tuned almost entirely via the `[packing, entropy, cache_size]` option
(legacy: `replica_2_9_entropy_cache_size_mb`). Allocating more memory to the entropy cache should speed up
repack-in-place performance. 
- A collection of sync performance improvements have been implemented (including a fix for a stall that
could occur with `take_one_timeout` warnings).
- Performance improvements for `GET /sync_buckets`



## Community involvement

A huge thank you to all the Mining community members who contributed to this release by identifying and investigating bugs, sharing debug logs and node metrics, and providing guidance on performance tuning!

Discord users (alphabetical order):

- Butcher_
- Evalcast
- JF
- lawso2517
- smash
- timothynode

And a further huge thank you to the following researchers who identified and helped to patch issues addressed in this release!

- bbl4de ([https://github.com/bbl4de](https://github.com/bbl4de))


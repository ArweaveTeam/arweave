# arweave_lib

Protocol constants and pure helpers shared by every Arweave application. It is
a library: it starts no processes, holds no state and does no IO, so any
application calls it directly. Every other dependency goes through the
caller's `<app>_deps` module (see
[application boundaries](../../doc/agents/application-boundaries.md)).

## Public interface

| Module or header | Purpose |
|---|---|
| [`arweave_lib_constants`](src/arweave_lib_constants.erl) | Runtime geometry (partition size, replica.2.9 entropy and footprint sizes, recall ranges, chunk padding and chunk buckets) and fork heights. |
| [`arweave_lib_replica_2_9`](src/arweave_lib_replica_2_9.erl) | The replica.2.9 entropy mapping: each chunk's and sub-chunk's entropy partition, index, key and slice. |
| [`arweave_lib_footprint`](src/arweave_lib_footprint.erl) | Replica.2.9 footprint geometry, and conversion between footprint and byte intervals. |
| [`arweave_lib_intervals`](src/arweave_lib_intervals.erl) | Immutable sets of non-overlapping intervals. |
| [`arweave_lib_ets_intervals`](src/arweave_lib_ets_intervals.erl) | Interval sets in ETS tables the caller owns, for shared access. |
| [`arweave_lib_util`](src/arweave_lib_util.erl) | General collection, encoding, formatting and parsing helpers. |
| [`include/arweave_lib_constants.hrl`](include/arweave_lib_constants.hrl) | Compile-time protocol constants, such as units, block and transaction limits, and packing, VDF and pricing values. `arweave/include/ar.hrl` includes it. |

Other modules and headers are internal. Include `arweave_lib_constants.hrl`
rather than one of the topic headers it pulls in.

## What belongs here

A module belongs in `arweave_lib` only if its functions depend on nothing but
their arguments and build constants.
[`arweave_lib_SUITE`](test/arweave_lib_SUITE.erl) enforces this: the app
depends only on OTP, `jiffy` and `b64fast`, includes no header from another
Arweave application, and calls no file, OS, network, timer, process-registry,
logging or `gen_*` functions. Helpers with side effects belong to the node
([`ar_util`](../arweave/src/ar_util.erl)) or to the service that owns the side
effect.

[`arweave_lib_boundaries_SUITE`](test/arweave_lib_boundaries_SUITE.erl) checks
the other half of the rule: each application reaches every application other
than `arweave_lib` through its deps module.

## Test and build values

- Test builds (`AR_TEST`) use smaller values for constants that would make
  tests slow, such as the partition and entropy sizes.
- Build profiles can override selected values with `{d, ...}` defines in
  `rebar.config`, for example `FORKS_RESET` to activate every fork at height 0.
- Test builds can also override the partition size and the replica.2.9 entropy
  size and count at runtime, so simulations can use mainnet-sized footprints.

## Tests

```sh
./bin/ct --dir apps/arweave_lib/test
```

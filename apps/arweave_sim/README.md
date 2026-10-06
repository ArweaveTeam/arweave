# arweave_sim

A deterministic simulated clock and world model, for running Arweave
applications in a simulated network of peers, links and disks. Setups that
would take hours to reproduce on live nodes run in minutes. It is test-only:
no production application depends on it, and it is not in the release.

So far the only suite built on it is
[`arweave_sync`](../arweave_sync/README.md)'s, and the world model's peers
answer the requests sync makes. The simulator is meant to serve other
applications too: the clock works for any code that uses `ar_timer`, and any
application that reaches peers and disks through its deps module can run
against the world model. The rest of this README describes the sync suite.

## Why it exists

Performance depends on how peers, links, disks and caches behave together,
and those conditions are slow and expensive to reproduce on live nodes. Sync
shows the problem well: changes to it often help one setup while hurting
another. A fix for a slow peer starves fast ones, or a fix for many stores
breaks a single-store node. Each simulated scenario states a performance
contract, so a change that improves one setup and makes another worse fails
the suite.

## Public interface

- [`arweave_sim`](src/arweave_sim.erl) is the only public module: the clock,
  the world model and its measurements.
- [`include/arweave_sim.hrl`](include/arweave_sim.hrl) defines the records
  that describe a world, such as `#sim_world{}` and `#sim_peer{}`.

## How it works

### Real logic, simulated boundaries

The sync code runs unmodified, along with the real entropy cache,
`arweave_lib` geometry and configuration. The simulation replaces only what
lies outside sync:

- **Dependencies.** The sync driver installs
  [`arweave_sync_deps_sim`](../arweave_sync/test/arweave_sync_deps_sim.erl)
  as `arweave_sync`'s deps module. `arweave_sync_deps_sim` answers sync's HTTP
  requests, peer lists, storage queries, chunk cache counters, device locks
  and events from the world model.
- **Time.** Starting the simulated clock redirects `ar_timer` in test builds,
  so timers and sleeps in real code use simulated time.
- **Chunk writes.** The sync driver replaces validating, unpacking and writing
  a fetched chunk with a modeled write.

To add a boundary, add it to the deps module. Do not mock sync's internal
functions.

### The world model

A scenario describes its world in a `#sim_world{}` record:

- **Peers:** serving rate, latency, rate limiting (429s), timeouts and client
  errors, connection limits, metadata latency, and the data they advertise.
- **Network:** a shared downlink capacity, and a read limit for each peer's
  copy of each store.
- **Local node:** storage modules, the rate at which each store writes,
  entropy generation time, and node configuration such as cache sizes and the
  download limit.

The model also tracks the chunk cache, stored chunks, and per-peer and
per-store counters, which scenarios read as measurements.

### The clock

Simulated time advances only when the driver calls `arweave_sim:advance/1`.
The sync driver advances in substeps of `SIM_SUBSTEP_MS`, and waits for every
sync worker to go idle before the next one, so each substep is processed
completely. Peer latencies must align with the substep.

Time is deterministic, but the order in which processes run within a substep
is not. Aggregate results are stable from run to run; how work splits between
individual peers can vary.

## Writing a scenario

Scenarios live in
[`arweave_sync_sim_SUITE`](../arweave_sync/test/arweave_sync_sim_SUITE.erl).
Each one:

1. Builds a `#sim_world{}` and starts it with the sync driver
   ([`arweave_sync_sim`](../arweave_sync/test/arweave_sync_sim.erl)).
2. Runs for a warm-up period, then measures over a window.
3. Asserts contracts relative to the modeled capacity: for example, that
   settled throughput uses at least 95% of the limiting capacity, or that a
   degraded peer doesn't drag down healthy ones.

Assert outcomes, not exact schedules or controller state, so scenarios survive
legitimate changes to the algorithms. Follow the existing comments: each
scenario states its setup, timeline and contracts, and explains how its
durations and thresholds were chosen.

## Tests

```sh
# The clock and world model
./bin/ct --dir apps/arweave_sim/test

# The sync scenarios and the deps module
./bin/ct --suite apps/arweave_sync/test/arweave_sync_sim_SUITE.erl,apps/arweave_sync/test/arweave_sync_deps_sim_SUITE.erl
```

The scenario suite takes several minutes, and CI runs it in its own shard.
Scenarios run one at a time, because the clock and dependency selection are
global to the VM.

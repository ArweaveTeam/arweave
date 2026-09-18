# arweave_sim

Test-only deterministic clock and network, disk and packing models.
No production application depends on this app, and it is not in the release.

`arweave_sim.erl` and `arweave_sim.hrl` are the public interface.
Drivers supply their cache budget, tick duration and query-range size; this app
does not depend on sync internals or sync test helpers.

The world uses its internal `arweave_sim_clock` directly. External drivers use
the clock functions on `arweave_sim`. Starting the clock also redirects `ar_timer`
in test builds, so real code under simulation observes the same clock. Simulated
operations require the clock to be started; their sleeps complete when the driver
advances time.

The sync driver (`arweave_sync_sim`), dependency adapter
(`arweave_sync_deps_sim`) and sync scenario suite live in
`apps/arweave_sync/test`. They drive the real sync pipeline using this app's
simulated environment. Protocol and cache helpers remain real test dependencies;
the host node is not started. Scenarios run sequentially because test-clock and
dependency overrides are VM-global.

Run the clock and world-model tests with:

```sh
./bin/ct --dir apps/arweave_sim/test
```

Run the sync scenarios and adapter tests with:

```sh
./bin/ct --suite apps/arweave_sync/test/arweave_sync_sim_SUITE.erl,apps/arweave_sync/test/arweave_sync_deps_sim_SUITE.erl
```

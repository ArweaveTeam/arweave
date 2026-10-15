**This is an alpha update and may not be ready for production use. This software was prepared by the Digital History Association, in cooperation from the wider Arweave ecosystem.**

This release introduces various bug fixes and stability improvements.

Configuration note: the vdf-server-5.arweave.xyz VDF server is no longer operational. Please remove vdf-server-5.arweave.xyz from your settings.

This release contains the updated revision of the improved replica 2.9 syncing shipped in 2.9.7-alpha1-dev-sync-performance-20260909. The new approach stabilizes syncing and increases its throughput by maximizing healthy utilization of various hardware setups and network environments.

Other highlights:

- The miner now switches between VDF sessions more efficiently.
- Upgraded dependencies including Erlang 27, which is a required minimal version now.
- Added support for HTTP/2, turned on by default.
- Improved peer discovery and connection management.
- Tuned rate-limiting and throttling.
- Mempool processing is faster now.
- The data range webhook no longer fails.
- Fixed footprint batching at partition boundaries.


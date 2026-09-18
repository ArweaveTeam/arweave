%%% Select the sync pipeline's production dependencies.
-module(arweave_sync_deps_mainnet).
-behaviour(arweave_sync_deps).

-export([
    clock/0,
    peers/0,
    throttling/0,
    http/0,
    chunk_cache/0,
    data_sync/0,
    storage/0,
    blacklist/0,
    footprint_limit/0,
    node/0,
    device_lock/0,
    disk_pool/0,
    events/0,
    sync_buckets/0,
    replica/0,
    packing/0,
    constants/0
]).

clock() -> ar_timer.
peers() -> ar_peers.
throttling() -> arweave_throttling.
http() -> ar_http_iface_client.
chunk_cache() -> ar_chunk_cache.
data_sync() -> ar_data_sync.
storage() -> arweave_storage.
blacklist() -> ar_tx_blacklist.
footprint_limit() -> ar_footprint_limit.
node() -> ar_node.
device_lock() -> ar_device_lock.
disk_pool() -> ar_disk_pool.
events() -> ar_events.
sync_buckets() -> ar_sync_buckets.
replica() -> ar_replica_2_9.
packing() -> ar_packing_server.
constants() -> arweave_constants.

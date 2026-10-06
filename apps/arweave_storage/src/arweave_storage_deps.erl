%%% @doc Selects the host modules that storage calls, one function per
%%% dependency. Call sites use ?DEP(Name) from include/arweave_storage_deps.hrl.
-module(arweave_storage_deps).

-export([
    kv/0,
    events/0,
    clock/0,
    serialize/0,
    packing/0,
    sync_buckets/0,
    console/0,
    config/0,
    metrics/0
]).

kv() -> ar_kv.
events() -> ar_events.
clock() -> ar_timer.
serialize() -> ar_serialize.
packing() -> ar_packing_server.
sync_buckets() -> ar_sync_buckets.
console() -> ar.
config() -> arweave_config.
metrics() -> arweave_metrics.

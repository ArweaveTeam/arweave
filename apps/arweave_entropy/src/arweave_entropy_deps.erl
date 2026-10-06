%%% @doc Selects the host modules that entropy calls, one function per
%%% dependency. Call sites use ?DEP(Name) from include/arweave_entropy_deps.hrl.
-module(arweave_entropy_deps).

-export([
    packing/0,
    device_lock/0,
    footprint_limit/0,
    serialize/0,
    randomx/0,
    console/0,
    storage/0,
    config/0,
    metrics/0
]).

packing() -> ar_packing_server.
device_lock() -> ar_device_lock.
footprint_limit() -> ar_footprint_limit.
serialize() -> ar_serialize.
randomx() -> ar_mine_randomx.
console() -> ar.
storage() -> arweave_storage.
config() -> arweave_config.
metrics() -> arweave_metrics.

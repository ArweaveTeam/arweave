%%% Select the sync pipeline's external modules.
%%% Pure storage geometry and shared utilities use their public APIs directly.
-module(arweave_sync_deps).

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

-callback clock() -> module().
-callback peers() -> module().
-callback throttling() -> module().
-callback http() -> module().
-callback chunk_cache() -> module().
-callback data_sync() -> module().
-callback storage() -> module().
-callback blacklist() -> module().
-callback footprint_limit() -> module().
-callback node() -> module().
-callback device_lock() -> module().
-callback disk_pool() -> module().
-callback events() -> module().
-callback sync_buckets() -> module().
-callback replica() -> module().
-callback packing() -> module().
-callback constants() -> module().

-ifdef(AR_TEST).
-export([override_module/1, reset_all_overrides/0]).
-endif.

clock() -> (implementation()):clock().
peers() -> (implementation()):peers().
throttling() -> (implementation()):throttling().
http() -> (implementation()):http().
chunk_cache() -> (implementation()):chunk_cache().
data_sync() -> (implementation()):data_sync().
storage() -> (implementation()):storage().
blacklist() -> (implementation()):blacklist().
footprint_limit() -> (implementation()):footprint_limit().
node() -> (implementation()):node().
device_lock() -> (implementation()):device_lock().
disk_pool() -> (implementation()):disk_pool().
events() -> (implementation()):events().
sync_buckets() -> (implementation()):sync_buckets().
replica() -> (implementation()):replica().
packing() -> (implementation()):packing().
constants() -> (implementation()):constants().

-ifdef(AR_TEST).

implementation() ->
    persistent_term:get({?MODULE, module}, arweave_sync_deps_mainnet).

%% @doc Install node-global test dependencies until reset_all_overrides/0.
override_module(Module) ->
    persistent_term:put({?MODULE, module}, Module).

reset_all_overrides() ->
    persistent_term:erase({?MODULE, module}),
    ok.

-else.

implementation() ->
    arweave_sync_deps_mainnet.

-endif.

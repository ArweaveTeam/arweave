%%% @doc Selects the host modules that sync calls. Each function
%%% returns the module for one dependency; for example, sync reads the clock
%%% as (arweave_sync_deps:clock()):monotonic_ms().
%%%
%%% Builds without AR_TEST always use arweave_sync_deps_mainnet. Test builds
%%% read the implementation from a persistent_term: override_module/1
%%% replaces it for the whole node until reset_all_overrides/0 is called.
%%% arweave_sync_sim installs arweave_sync_deps_sim, and
%%% arweave_sync_test_deps installs itself.
%%%
%%% Sync calls arweave_lib directly; every other application is reached
%%% through these selectors.
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
    packing/0,
    config/0,
    metrics/0
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
-callback packing() -> module().
-callback config() -> module().
-callback metrics() -> module().

-ifdef(AR_TEST).
-export([override_module/1, reset_all_overrides/0]).
-endif.

%%%===================================================================
%%% Public interface.
%%%===================================================================

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
packing() -> (implementation()):packing().
config() -> (implementation()):config().
metrics() -> (implementation()):metrics().

%%%===================================================================
%%% Implementation selection.
%%%===================================================================

-ifdef(AR_TEST).

implementation() ->
    persistent_term:get({?MODULE, module}, arweave_sync_deps_mainnet).

override_module(Module) ->
    persistent_term:put({?MODULE, module}, Module).

reset_all_overrides() ->
    persistent_term:erase({?MODULE, module}),
    ok.

-else.

implementation() ->
    arweave_sync_deps_mainnet.

-endif.

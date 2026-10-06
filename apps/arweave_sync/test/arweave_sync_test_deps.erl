%%% @doc The arweave_sync_deps implementation for tests that do not start the
%%% host application. It stubs the clock, chunk cache, data sync,
%%% node, peers and events with fixed answers, and names the production module
%%% for every other dependency.
-module(arweave_sync_test_deps).
-behaviour(arweave_sync_deps).

-export([
    clock/0,
    chunk_cache/0,
    data_sync/0,
    node/0,
    peers/0,
    events/0,
    throttling/0,
    http/0,
    storage/0,
    blacklist/0,
    footprint_limit/0,
    device_lock/0,
    disk_pool/0,
    sync_buckets/0,
    packing/0,
    config/0,
    metrics/0,
    setup/0,
    cleanup/0,
    monotonic_ms/0,
    send_after/3,
    limit/0,
    cached_size/0, cached_size/1,
    is_disk_space_sufficient/1,
    is_joined/0,
    get_peers/1,
    get_peer_release/1,
    subscribe/1,
    completed/1
]).

setup() ->
    ok = arweave_config:start(),
    arweave_sync_deps:override_module(?MODULE),
    ok.

cleanup() ->
    arweave_sync_deps:reset_all_overrides(),
    arweave_config:stop().

monotonic_ms() -> erlang:monotonic_time(millisecond).
send_after(DelayMs, Destination, Message) ->
    {ok, erlang:send_after(DelayMs, Destination, Message)}.
limit() -> 2000.
cached_size() -> 0.
cached_size(_StoreID) -> 0.
is_disk_space_sufficient(_StoreID) -> true.
is_joined() -> false.
get_peers(_Type) -> [].
get_peer_release(_Peer) -> 0.
subscribe(Events) -> [ok || _ <- Events].

completed(_StoreID) -> undefined.

clock() -> ?MODULE.
chunk_cache() -> ?MODULE.
data_sync() -> ?MODULE.
node() -> ?MODULE.
peers() -> ?MODULE.
events() -> ?MODULE.
throttling() -> arweave_sync_deps_mainnet:throttling().
http() -> arweave_sync_deps_mainnet:http().
storage() -> arweave_sync_deps_mainnet:storage().
blacklist() -> arweave_sync_deps_mainnet:blacklist().
footprint_limit() -> arweave_sync_deps_mainnet:footprint_limit().
device_lock() -> arweave_sync_deps_mainnet:device_lock().
disk_pool() -> arweave_sync_deps_mainnet:disk_pool().
sync_buckets() -> arweave_sync_deps_mainnet:sync_buckets().
packing() -> arweave_sync_deps_mainnet:packing().
config() -> arweave_sync_deps_mainnet:config().
metrics() -> arweave_sync_deps_mainnet:metrics().

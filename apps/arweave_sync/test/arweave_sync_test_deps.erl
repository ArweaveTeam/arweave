%%% Dependency fixture for tests that do not start the host application.
-module(arweave_sync_test_deps).

-export([
    clock/0,
    chunk_cache/0,
    data_sync/0,
    constants/0,
    node/0,
    peers/0,
    events/0,
    setup/0,
    cleanup/0,
    monotonic_ms/0,
    send_after/3,
    limit/0,
    cached_size/0, cached_size/1,
    is_disk_space_sufficient/1,
    get_replica_2_9_footprint_size/0,
    get_replica_2_9_footprints_per_partition/0,
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
get_replica_2_9_footprint_size() -> 100 * 262144.
get_replica_2_9_footprints_per_partition() -> 1000.
is_joined() -> false.
get_peers(_Type) -> [].
get_peer_release(_Peer) -> 0.
subscribe(Events) -> [ok || _ <- Events].

completed(_StoreID) -> undefined.

clock() -> ?MODULE.
chunk_cache() -> ?MODULE.
data_sync() -> ?MODULE.
constants() -> ?MODULE.
node() -> ?MODULE.
peers() -> ?MODULE.
events() -> ?MODULE.

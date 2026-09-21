%%% Public interface to deterministic simulations.
-module(arweave_sim).

-export([start_clock/0, stop_clock/0, advance/1, sleeping/0, pending_timers/0]).
-export([monotonic_ms/0, sleep/1, send_after/3, apply_after/4]).
-export([
    create_world/0,
    delete_world/0,
    reset_world/4,
    update_world/1,
    start_scenario/0,
    tick_index/0,
    world_value/1,
    snapshot/0
]).
-export([
    storage_modules/0, storage_modules/1, default_storage_modules/0,
    store_ranges/0,
    store_ids/0,
    weave_size/0,
    use_mainnet_replica_2_9_sizes/0
]).
-export([
    get_chunk_binary/3,
    wait_for_entropy/2,
    wait_for_chunk_interval_response/1,
    peer_sync_kind_enabled/2,
    peer_sync_intervals/1
]).
-export([
    is_chunk_cache_full/0,
    chunk_cache_size/0, chunk_cache_size/1,
    increment_chunk_cache_size/1,
    admit_store_write/3,
    write_completed/2,
    unsynced_intervals/3,
    unsynced_footprint_intervals/3,
    get_next_synced_interval/3
]).

start_clock() -> arweave_sim_clock:start().
stop_clock() -> arweave_sim_clock:stop().
advance(Milliseconds) -> arweave_sim_clock:advance(Milliseconds).
sleeping() -> arweave_sim_clock:sleeping().
pending_timers() -> arweave_sim_clock:pending().

monotonic_ms() -> arweave_sim_clock:monotonic_ms().
sleep(Milliseconds) -> arweave_sim_clock:sleep(Milliseconds).

send_after(Milliseconds, Destination, Message) ->
    arweave_sim_clock:send_after(Milliseconds, Destination, Message).

apply_after(Milliseconds, Module, Function, Arguments) ->
    arweave_sim_clock:apply_after(Milliseconds, Module, Function, Arguments).

create_world() -> arweave_sim_world:create().
delete_world() -> arweave_sim_world:delete().

%% @doc Reset the world with the driver's cache budget, tick and query sizes.
reset_world(World, CacheLimit, TickIntervalMS, QueryRangeBytes) ->
    arweave_sim_world:reset(World, CacheLimit, TickIntervalMS, QueryRangeBytes).

update_world(World) -> arweave_sim_world:update_world(World).
start_scenario() -> arweave_sim_world:start_scenario().
tick_index() -> arweave_sim_world:tick_index().
world_value(Key) -> arweave_sim_world:get(Key).
snapshot() -> arweave_sim_world:snapshot().

storage_modules() -> arweave_sim_world:storage_modules().
storage_modules(World) -> arweave_sim_world:storage_modules(World).
default_storage_modules() -> arweave_sim_world:default_storage_modules().
store_ranges() -> arweave_sim_world:store_ranges().
store_ids() -> arweave_sim_world:store_ids().
weave_size() -> arweave_sim_world:weave_size().

use_mainnet_replica_2_9_sizes() ->
    arweave_sim_world:use_mainnet_replica_2_9_sizes().

get_chunk_binary(Peer, Offset, TimeoutMS) ->
    arweave_sim_world:get_chunk_binary(Peer, Offset, TimeoutMS).

wait_for_entropy(Peer, Byte) ->
    arweave_sim_world:wait_for_entropy(Peer, Byte).

wait_for_chunk_interval_response(Peer) ->
    arweave_sim_world:wait_for_chunk_interval_response(Peer).

peer_sync_kind_enabled(Peer, Kind) ->
    arweave_sim_world:peer_sync_kind_enabled(Peer, Kind).

peer_sync_intervals(Peer) ->
    arweave_sim_world:peer_sync_intervals(Peer).

is_chunk_cache_full() -> arweave_sim_world:is_chunk_cache_full().
chunk_cache_size() -> arweave_sim_world:chunk_cache_size().
chunk_cache_size(StoreID) -> arweave_sim_world:chunk_cache_size(StoreID).

increment_chunk_cache_size(StoreID) ->
    arweave_sim_world:increment_chunk_cache_size(StoreID).

admit_store_write(StoreID, Tick, Second) ->
    arweave_sim_world:admit_store_write(StoreID, Tick, Second).

write_completed(StoreID, Byte) ->
    arweave_sim_world:write_completed(StoreID, Byte).

unsynced_intervals(Start, End, StoreID) ->
    arweave_sim_world:unsynced_intervals(Start, End, StoreID).

unsynced_footprint_intervals(Partition, Footprint, StoreID) ->
    arweave_sim_world:unsynced_footprint_intervals(Partition, Footprint, StoreID).

get_next_synced_interval(StoreID, Byte, End) ->
    arweave_sim_world:get_next_synced_interval(StoreID, Byte, End).

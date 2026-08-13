%%% @doc Public integration surface for the chunk-sync subsystem.
-module(ar_sync).

-export([create_ets/0, child_specs/0, enabled/0, start_store/1, set_weave_size/2,
        task_write_completed/1,
        get_peers_for_offset/1]).

-include_lib("arweave/include/ar_sup.hrl").
-include_lib("arweave/include/ar_sync.hrl").

%% @doc Create sync-owned tables before any process that reads them starts.
create_ets() ->
    ar_sync_peer:create_ets(),
    %% Byte and footprint coarse bucket rows share this table under
    %% {Mode, Bucket, Peer} keys. Mode leads because the two modes use
    %% different bucket index spaces.
    ets:new(?SYNC_BUCKET_CACHE_TABLE,
        [ordered_set, public, named_table, {read_concurrency, true}]),
    %% Chunk interval rows use the same {Mode, Location, Peer} key shape.
    ets:new(?CHUNK_INTERVAL_CACHE_TABLE,
        [ordered_set, public, named_table, {read_concurrency, true}]),
    ok.

%% @doc Child specs for the network-sync subsystem.
child_specs() ->
    case enabled() of
        false -> [];
        true -> [?CHILD_SUP(ar_sync_sup, supervisor)]
    end.

%% @doc Return whether network syncing is enabled.
enabled() ->
    ar_sync_download_limit:enabled().

%% @doc Start one store's work-discovery loop.
start_store(StoreID) ->
    ar_sync_store_sweeper:start(StoreID).

%% @doc Update the live weave bound of one store's sweeper.
set_weave_size(StoreID, WeaveSize) ->
    ar_sync_store_sweeper:set_weave_size(StoreID, WeaveSize).

%% @doc Notify the scheduler that one fetched chunk reached a terminal write result.
task_write_completed(TaskRef) ->
    ar_sync_scheduler:task_write_completed(TaskRef).

%% @doc Return peers whose coarse discovery data includes Offset.
get_peers_for_offset(Offset) ->
    ar_sync_discovery:get_peers_for_offset(Offset).

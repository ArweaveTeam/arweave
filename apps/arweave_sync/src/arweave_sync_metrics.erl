%%% @doc Publishes the sync metrics from values each caller already has. Store
%%% series carry the store's label (store_label/1), never the store ID, which
%%% contains the packing address. The publishers remove a peer's series once
%%% that peer has no work.
-module(arweave_sync_metrics).

-export([
    %% Scheduler.
    publish_stores/4, publish_peers/4, publish_footprints/2,
    %% Peer limits.
    publish_peer/4, publish_peer_caps/2,
    %% Discovery.
    publish_discovery/4, count_chunk_interval_evictions/2,
    %% Sweep and chunk writer.
    publish_sweep_offset/3, count_chunk_skipped/1
]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_storage/include/arweave_storage.hrl").
-include_lib("arweave_sync/include/arweave_sync.hrl").
-include_lib("arweave_sync/include/arweave_sync_deps.hrl").

%%%===================================================================
%%% Scheduler.
%%%===================================================================

%% @doc Publish each store's task, claim and pipeline gauges. Every store
%% reports, so a starved store shows zeros rather than a missing series.
%%
%% Tasks: TaskRef => #task{} for the started tasks.
%%
%% QueuedTasks: the tasks in the peer queues.
publish_stores(StoreIDs, Stores, Tasks, QueuedTasks) ->
    TaskCounts = count_tasks(fun(#task{ store_id = StoreID }) -> StoreID end,
        Tasks),
    PeersByStore = peers_by_store(maps:values(Tasks) ++ QueuedTasks),
    FairShare = arweave_sync_store:fair_share(Stores),
    lists:foreach(
        fun(StoreID) ->
            Label = store_label(StoreID),
            ?DEP(metrics):gauge_set(sync_tasks_by_store, [queued, Label],
                arweave_sync_store:queued_task_count(StoreID, Stores)),
            ?DEP(metrics):gauge_set(sync_tasks_by_store, [fetching, Label],
                maps:get({fetching, StoreID}, TaskCounts, 0)),
            ?DEP(metrics):gauge_set(sync_tasks_by_store, [writing, Label],
                maps:get({writing, StoreID}, TaskCounts, 0)),
            ?DEP(metrics):gauge_set(sync_claimed_bytes_by_store, [Label],
                arweave_sync_store:claimed_chunks(StoreID, Stores)
                    * ?DATA_CHUNK_SIZE),
            ?DEP(metrics):gauge_set(sync_claimed_peers_by_store, [Label],
                sets:size(maps:get(StoreID, PeersByStore, sets:new()))),
            ?DEP(metrics):gauge_set(sync_store_pipeline_limit_chunks,
                [Label],
                arweave_sync_store:pipeline_limit(StoreID, FairShare, Stores))
        end,
        StoreIDs).

%% @doc Publish each active peer's task gauges and the totals, and remove the
%% task series of peers that are no longer active.
%%
%% QueueLengths: Peer => tasks in the peer's queue.
publish_peers(Peers, Tasks, QueueLengths, InflightCount) ->
    TaskCounts = count_tasks(fun(#task{ peer = Peer }) -> Peer end, Tasks),
    Labels = lists:flatmap(
        fun(Peer) ->
            Label = arweave_lib_util:format_peer(Peer),
            ?DEP(metrics):gauge_set(sync_tasks_by_peer, [queued, Label],
                maps:get(Peer, QueueLengths, 0)),
            ?DEP(metrics):gauge_set(sync_tasks_by_peer, [fetching, Label],
                maps:get({fetching, Peer}, TaskCounts, 0)),
            ?DEP(metrics):gauge_set(sync_tasks_by_peer, [writing, Label],
                maps:get({writing, Peer}, TaskCounts, 0)),
            [[queued, Label], [fetching, Label], [writing, Label]]
        end,
        Peers),
    ?DEP(metrics):gauge_set(sync_active_peers, length(Peers)),
    ?DEP(metrics):gauge_set(sync_total_inflight, InflightCount),
    prune(sync_tasks_by_peer, Labels).

%% @doc Publish the entropy slots that footprints hold and the total slots.
publish_footprints(BoundCount, MaxActive) ->
    ?DEP(metrics):gauge_set(sync_active_footprints, BoundCount),
    ?DEP(metrics):gauge_set(sync_max_active_footprints, MaxActive).

count_tasks(Key, Tasks) ->
    maps:fold(
        fun(_TaskRef, #task{ state = Stage } = Task, Acc)
                    when Stage =:= fetching; Stage =:= writing ->
                arweave_lib_util:increment_map_value({Stage, Key(Task)}, Acc);
           (_TaskRef, _Task, Acc) ->
                Acc
        end,
        #{},
        Tasks).

peers_by_store(Tasks) ->
    lists:foldl(
        fun(#task{ peer = Peer, store_id = StoreID }, Acc) ->
            maps:update_with(StoreID,
                fun(Peers) -> sets:add_element(Peer, Peers) end,
                sets:from_list([Peer]), Acc)
        end,
        #{},
        Tasks).

%%%===================================================================
%%% Peer limits.
%%%===================================================================

%% @doc Publish one peer's cap, goodput and failure pressure after a tick.
%%
%% Goodput: chunk bytes fetched from the peer per millisecond.
publish_peer(Peer, Cap, Goodput, FailurePressure) ->
    Label = arweave_lib_util:format_peer(Peer),
    ?DEP(metrics):gauge_set(sync_peer_concurrency_cap, [Label], Cap),
    ?DEP(metrics):gauge_set(sync_peer_goodput_bytes_per_second, [Label],
        Goodput * 1000),
    ?DEP(metrics):gauge_set(sync_peer_failure_pressure, [Label],
        FailurePressure).

%% @doc Publish the sum of the active peers' caps, and remove the per-peer
%% series of the other peers.
publish_peer_caps(Peers, TotalCap) ->
    ?DEP(metrics):gauge_set(sync_total_concurrency_cap, TotalCap),
    Labels = [[arweave_lib_util:format_peer(Peer)] || Peer <- Peers],
    lists:foreach(fun(Name) -> prune(Name, Labels) end,
        [sync_peer_concurrency_cap, sync_peer_goodput_bytes_per_second,
            sync_peer_failure_pressure]).

%%%===================================================================
%%% Discovery.
%%%===================================================================

%% @doc Publish the tracked peer count, the chunk interval cache size and the
%% number of peers advertising data in each store's range.
%%
%% StorePeers: [{Mode, StoreID, Peers}], the set of peers advertising data in
%% each store's range, by mode.
publish_discovery(TrackedPeerCount, CacheRows, CacheBytes, StorePeers) ->
    ?DEP(metrics):gauge_set(discovery_peers_scanned, TrackedPeerCount),
    ?DEP(metrics):gauge_set(chunk_interval_cache_size, [rows], CacheRows),
    ?DEP(metrics):gauge_set(chunk_interval_cache_size, [bytes], CacheBytes),
    lists:foreach(
        fun({Mode, StoreID, Peers}) ->
            ?DEP(metrics):gauge_set(sync_discovery_peers,
                [Mode, store_label(StoreID)], sets:size(Peers))
        end,
        StorePeers),
    ModePeers = lists:foldl(
        fun({Mode, _StoreID, Peers}, Acc) ->
            maps:update_with(Mode, fun(Set) -> sets:union(Set, Peers) end,
                Peers, Acc)
        end,
        maps:from_keys([byte, footprint], sets:new()),
        StorePeers),
    maps:foreach(
        fun(Mode, Peers) ->
            ?DEP(metrics):gauge_set(sync_discovery_peers, [Mode, "all"],
                sets:size(Peers))
        end,
        ModePeers),
    ?DEP(metrics):gauge_set(sync_discovery_peers, [union, "all"],
        sets:size(sets:union(maps:values(ModePeers)))).

%% @doc Count the rows discovery evicts from the chunk interval cache.
count_chunk_interval_evictions(_Reason, 0) ->
    ok;
count_chunk_interval_evictions(Reason, Count) ->
    ?DEP(metrics):counter_inc(chunk_interval_cache_evictions, [Reason],
        Count).

%%%===================================================================
%%% Sweep and chunk writer.
%%%===================================================================

%% @doc Publish how far each mode's sweep has advanced into the store's range,
%% in bytes.
publish_sweep_offset(StoreID, ByteSwept, FootprintSwept) ->
    Label = store_label(StoreID),
    ?DEP(metrics):gauge_set(sync_sweep_offset, [Label, byte], ByteSwept),
    ?DEP(metrics):gauge_set(sync_sweep_offset, [Label, footprint],
        FootprintSwept).

%% @doc Count a fetched chunk that the chunk writer dropped.
count_chunk_skipped(Reason) ->
    ?DEP(metrics):counter_inc(sync_chunks_skipped, [Reason]).

%%%===================================================================
%%% Private functions.
%%%===================================================================

store_label(StoreID) ->
    (?DEP(storage):store_info(StoreID))#store_info.label.

%% @doc Remove the series of Name whose label values are not in Labels.
prune(Name, Labels) ->
    Existing = [[Value || {_LabelName, Value} <- LabelPairs]
        || {LabelPairs, _MetricValue} <- ?DEP(metrics):gauge_values(Name)],
    lists:foreach(fun(Stale) -> ?DEP(metrics):gauge_remove(Name, Stale) end,
        Existing -- Labels).

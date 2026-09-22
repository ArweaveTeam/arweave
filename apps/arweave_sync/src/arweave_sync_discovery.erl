%%% @doc Maintains a cache of the data ranges served by each peer.
%%%
%%% Sync bucket jobs cache coarse peer availability. Chunk interval jobs retrieve
%%% detailed availability from /data_sync_record and /footprints and cache it in
%%% ETS.
%%%
%%% arweave_sync_store_sweeper reads this cache and gives arweave_sync_chunk_picker an
%%% immutable snapshot when deciding which chunks can be fetched from which peers.
-module(arweave_sync_discovery).

-ifdef(AR_TEST).
%% Focused tests live outside the production module.
-export([
    add_peer/2,
    chunk_interval_key/3,
    chunk_interval_lookup/3,
    compute_job_load/1,
    delete_expired_sync_buckets/0,
    do_remove_peer/3,
    enqueue_job/2,
    enqueue_sync_bucket_jobs/1,
    fetch_chunk_intervals/3,
    finish_job/2,
    get_chunk_intervals/5,
    inflight_peer_store_mode_counts/1,
    interval_location/2,
    job_exists/2,
    mark_chunk_intervals_stale_on_share_change/4,
    refresh_chunk_intervals/1,
    remove_jobs/2,
    start_jobs/1,
    store_row/5,
    sync_bucket/2,
    sync_bucket_job/1,
    sync_bucket_key/3,
    take_next_job/4
]).
-endif.

-behaviour(gen_server).

-export([
    start_link/0,
    get_peers_for_offset/1,
    warm_peer_ranges/3,
    cached_peer_ranges/5
]).

-ifdef(AR_TEST).
-export([collect_peers/0, reset_all_caches/0, inflight_count/0]).
-endif.

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_storage/include/arweave_storage.hrl").
-include_lib("arweave_sync/include/arweave_sync.hrl").

-include("arweave_sync_discovery.hrl").

%%%===================================================================
%%% State and data structures.
%%%===================================================================

%% Per-peer cache of chunk intervals reported by each peer
%% (via /data_sync_record for byte mode and /footprints for
%% footprint mode). Populated by the chunk interval jobs in this module;
%% read by arweave_sync_store_sweeper and passed to arweave_sync_chunk_picker to compute
%% fetchable intervals.
%%
%% Rows are `{Key, Intervals, MonotonicMs}', keyed by
%% `{Mode, Peer, Location}' so a peer's rows for one coarse sync bucket are
%% contiguous. The timestamp is used for demand-side freshness
%% checks and oldest-first cache trimming.
%% The host supplies a separate byte budget for peer intervals and includes
%% it in joint cache sizing. When the cap fires we drop the
%% oldest 10% of rows in a single pass; still-needed rows are re-warmed on
%% demand.

%%%===================================================================
%%% Coarse peer availability.
%%%===================================================================

%% @doc Return peers advertising the byte or replica.2.9 sync bucket that
%% contains Offset. Callers fetch chunks through /chunk2 either way; the
%% protocol-specific proof that a peer has data is hidden in this module.
get_peers_for_offset(Offset) ->
    Peers = lists:foldl(
        fun(Mode, Acc) -> sets:union(Acc, get_peers_for_offset(Mode, Offset)) end,
        sets:new(),
        [byte, footprint]
    ),
    sets:to_list(Peers).

get_peers_for_offset(Mode, Offset) ->
    case sync_bucket(Mode, Offset) of
        SyncBucket when is_integer(SyncBucket) ->
            get_peers_for_sync_bucket(Mode, SyncBucket);
        _ ->
            sets:new()
    end.

peer_has_sync_bucket(Mode, Peer, Offset) ->
    case sync_bucket(Mode, Offset) of
        SyncBucket when is_integer(SyncBucket) ->
            ets:member(
                ?SYNC_BUCKET_CACHE_TABLE,
                sync_bucket_key(Mode, SyncBucket, Peer)
            );
        _ ->
            false
    end.

sync_bucket(byte, Offset) ->
    Offset div ?NETWORK_DATA_BUCKET_SIZE;
sync_bucket(footprint, Offset) ->
    arweave_storage:get_footprint_bucket(Offset + ?DATA_CHUNK_SIZE).

get_peers_for_sync_bucket(Mode, SyncBucket) ->
    get_peers_for_sync_bucket(Mode, SyncBucket, sets:new()).

%% @doc Return Peers plus peers advertising SyncBucket for Mode. Rows for both
%% modes share the ?SYNC_BUCKET_CACHE_TABLE ordered_set under
%% {Mode, SyncBucket, Peer} keys
%% (byte and footprint bucket numbers are different index spaces, so Mode
%% leads); no_peer sorts before any peer tuple, so this is a prefix walk.
get_peers_for_sync_bucket(Mode, SyncBucket, Peers) ->
    get_peers_for_sync_bucket(
        Mode,
        SyncBucket,
        Peers,
        sync_bucket_key(Mode, SyncBucket, no_peer)
    ).

get_peers_for_sync_bucket(Mode, SyncBucket, Peers, Cursor) ->
    case ets:next(?SYNC_BUCKET_CACHE_TABLE, Cursor) of
        {Mode, SyncBucket, Peer} = Key ->
            get_peers_for_sync_bucket(
                Mode, SyncBucket, sets:add_element(Peer, Peers), Key
            );
        _ ->
            Peers
    end.

get_peers_for_sync_bucket_range(
    _Mode, StartSyncBucket, EndSyncBucket, Peers
) when
    StartSyncBucket > EndSyncBucket
->
    Peers;
get_peers_for_sync_bucket_range(
    Mode, StartSyncBucket, EndSyncBucket, Peers
) ->
    collect_sync_bucket_peers(
        Mode,
        EndSyncBucket,
        Peers,
        sync_bucket_key(Mode, StartSyncBucket, no_peer)
    ).

%% One prefix walk over the whole bucket range. Rows are keyed
%% {Mode, SyncBucket, Peer} in an ordered_set, so ets:next lands on the next
%% populated row and empty buckets cost nothing. Visiting each bucket in turn
%% instead costs a lookup per bucket, and a store's range spans far more
%% buckets than the peers serving it ever populate.
collect_sync_bucket_peers(Mode, EndSyncBucket, Peers, Cursor) ->
    case ets:next(?SYNC_BUCKET_CACHE_TABLE, Cursor) of
        {Mode, SyncBucket, Peer} = Key when SyncBucket =< EndSyncBucket ->
            collect_sync_bucket_peers(
                Mode, EndSyncBucket, sets:add_element(Peer, Peers), Key
            );
        _ ->
            Peers
    end.

%%%===================================================================
%%% Detailed peer availability.
%%%===================================================================

%% @doc Warm detailed metadata for the supplied peers at Offset. Cache misses
%% and stale rows enqueue exact chunk interval jobs; fresh rows and peers that
%% do not advertise the relevant coarse bucket require no work.
warm_peer_ranges(StoreID, Peers, Offset) ->
    gen_server:call(
        ?MODULE, {warm_peer_ranges, StoreID, Peers, Offset}, infinity
    ).

warm_peer_ranges(StoreID, Peers, Offset, State) ->
    lists:foldl(
        fun(Peer, Acc) ->
            Acc2 = warm_peer_range(byte, StoreID, Peer, Offset, Acc),
            warm_peer_range(footprint, StoreID, Peer, Offset, Acc2)
        end,
        State,
        Peers
    ).

warm_peer_range(Mode, StoreID, Peer, Offset, State) ->
    maybe
        true ?= peer_supports(Mode, Peer),
        true ?= peer_has_sync_bucket(Mode, Peer, Offset),
        true ?= chunk_interval_refresh_needed(Mode, Peer, Offset),
        PeerRange = #peer_range{
            store_id = StoreID,
            offset = Offset,
            peer = Peer
        },
        enqueue_job(chunk_interval_job(Mode, PeerRange), State)
    else
        false ->
            State
    end.

%% @doc Refresh needed if the row is missing or past TTL.
%% Reads only the timestamp so we do not copy the row's intervals out of ETS.
chunk_interval_refresh_needed(Mode, Peer, Offset) ->
    Key = chunk_interval_key(Mode, Peer, Offset),
    case ets:lookup_element(?CHUNK_INTERVAL_CACHE_TABLE, Key, 3, undefined) of
        undefined -> true;
        MonotonicMs ->
            MonotonicMs < expiration_cutoff(?CHUNK_INTERVAL_CACHE_TTL_MS)
    end.

%% @doc Return non-empty cached peer ranges without scheduling discovery work.
%% `ok' means every relevant peer/mode has a cached row; successful empty and
%% stale rows count as cached, while a missing row returns `cache_miss'.
cached_peer_ranges(StoreID, Peers, Offset, RangeStart, RangeEnd) ->
    {PeerRanges, AnyCacheMiss} = lists:foldl(
        fun(Peer, Acc) ->
            cached_peer_ranges_for_peer(
                StoreID,
                Peer,
                Offset,
                RangeStart,
                RangeEnd,
                Acc
            )
        end,
        {[], false},
        Peers
    ),
    CacheStatus =
        case AnyCacheMiss of
            true -> cache_miss;
            false -> ok
        end,
    {lists:reverse(PeerRanges), CacheStatus}.

cached_peer_ranges_for_peer(
    StoreID,
    Peer,
    Offset,
    RangeStart,
    RangeEnd,
    Acc
) ->
    BasePeerRange = #peer_range{
        store_id = StoreID,
        offset = Offset,
        peer = Peer,
        intervals = ar_intervals:new()
    },
    Acc2 = cached_peer_range(byte, BasePeerRange, RangeStart, RangeEnd, Acc),
    cached_peer_range(
        footprint, BasePeerRange, RangeStart, RangeEnd, Acc2
    ).

cached_peer_range(
    Mode,
    BasePeerRange,
    RangeStart,
    RangeEnd,
    {PeerRanges, AnyCacheMiss}
) ->
    #peer_range{peer = Peer, offset = Offset, store_id = StoreID} =
        BasePeerRange,
    maybe
        true ?= peer_supports(Mode, Peer),
        true ?= peer_has_sync_bucket(Mode, Peer, Offset),
        {_Freshness, Intervals} ?=
            get_chunk_intervals(
                Mode, Peer, Offset, RangeStart, RangeEnd
            ),
        true ?= not ar_intervals:is_empty(Intervals),
        Location = interval_location(Mode, Offset),
        PeerRange = BasePeerRange#peer_range{
            intervals = Intervals,
            footprint = footprint_key(Mode, StoreID, Location)
        },
        {[PeerRange | PeerRanges], AnyCacheMiss}
    else
        false ->
            {PeerRanges, AnyCacheMiss};
        cache_miss ->
            {PeerRanges, true}
    end.

%% Which footprint a peer range belongs to. The peer is not part of it: the
%% scheduler binds one peer per footprint and carries it alongside
%% (#peer_range.peer, #task.peer), so naming it here would only let the
%% same footprint appear under two keys.
footprint_key(byte, _StoreID, _Location) ->
    none;
footprint_key(footprint, StoreID, {Partition, Footprint}) ->
    #footprint{
        store_id = StoreID,
        partition = Partition,
        footprint = Footprint
    }.

%% @doc Return a peer's cached availability as byte intervals. Byte metadata
%% is clipped to the requested range; footprint metadata is converted from
%% footprint-record space.
get_chunk_intervals(_Mode, _Peer, _Offset, RangeStart, RangeEnd) when
    RangeStart >= RangeEnd
->
    {ok, ar_intervals:new()};
get_chunk_intervals(Mode, Peer, Offset, RangeStart, RangeEnd) ->
    case chunk_interval_lookup(Mode, Peer, Offset) of
        miss ->
            cache_miss;
        {Freshness, Intervals} ->
            ByteIntervals = chunk_intervals_to_byte_intervals(
                Mode, Intervals, RangeStart, RangeEnd
            ),
            case Freshness of
                hit -> {ok, ByteIntervals};
                stale -> {stale, ByteIntervals}
            end
    end.

chunk_intervals_to_byte_intervals(byte, Intervals, RangeStart, RangeEnd) ->
    ByteRange = ar_intervals:from_list([{RangeEnd, RangeStart}]),
    ar_intervals:intersection(Intervals, ByteRange);
chunk_intervals_to_byte_intervals(footprint, Intervals, _RangeStart, _RangeEnd) ->
    arweave_storage:footprint_intervals_to_byte_intervals(Intervals).

interval_location(byte, Offset) ->
    Step = arweave_sync_cursor:query_range_step_size(),
    (Offset div Step) * Step;
interval_location(footprint, Offset) ->
    arweave_storage:get_footprint_location(Offset + ?DATA_CHUNK_SIZE).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    %% Discovery tables are owned by the sync application root so the
    %% gen_server crashing or restarting doesn't wipe the cache.
    (arweave_sync_deps:clock()):send_after(
        ?DATA_DISCOVERY_COLLECT_PEERS_FREQUENCY_MS, self(), collect_peers
    ),
    schedule_sync_bucket_refresh(),
    %% Periodic maintenance updates cache state and emits discovery metrics.
    %% It fires inside the gen_server so it has direct access to State.
    (arweave_sync_deps:clock()):send_after(?MAINTENANCE_INTERVAL_MS, self(), maintenance),
    [ok, ok] = (arweave_sync_deps:events()):subscribe([peer, node_state]),
    {ok, #state{}}.

handle_call(inflight_count, _From, State) ->
    %% Expose asynchronous job count so a caller can determine quiescence.
    #state{jobs = JobsByKind} = State,
    NumInflight = maps:fold(
        fun(_Kind, Jobs, Acc) ->
            Acc + inflight_count(Jobs)
        end,
        0,
        JobsByKind
    ),
    {reply, NumInflight, State};
handle_call({warm_peer_ranges, StoreID, Peers, Offset}, _From, State) ->
    State2 = warm_peer_ranges(StoreID, Peers, Offset, State),
    {reply, ok, start_jobs(chunk_interval, State2)};
handle_call(Request, _From, State) ->
    ?LOG_WARNING([{event, unhandled_call}, {request, Request}]),
    {reply, ok, State}.

handle_cast({add_peers, Peers}, State) ->
    State2 = lists:foldl(fun add_peer/2, State, Peers),
    {noreply, start_jobs(sync_bucket, State2)};
handle_cast({job_result, Peer, Result}, State) ->
    State2 =
        case is_peer_tracked(Peer, State) of
            true -> record_job_result(Peer, Result, State);
            false -> State
        end,
    {noreply, State2};
handle_cast({remove_peer, Peer, Reason}, State) ->
    do_remove_peer(Peer, Reason, State);
handle_cast(Cast, State) ->
    ?LOG_WARNING([{event, unhandled_cast}, {cast, Cast}]),
    {noreply, State}.

handle_info({'DOWN', _, process, Pid, _Reason}, State) ->
    {noreply, finish_job(Pid, State)};
handle_info({event, peer, {removed, Peer}}, State) ->
    gen_server:cast(?MODULE, {remove_peer, Peer, peer_event_removed}),
    {noreply, State};
handle_info({event, peer, _}, State) ->
    {noreply, State};
handle_info({event, node_state, {initialized, _B}}, State) ->
    %% Join completed. Kick peer collection immediately instead of waiting up to
    %% ?DATA_DISCOVERY_COLLECT_PEERS_FREQUENCY_MS for the next periodic tick:
    %% collect_peers/0 gates on ar_node:is_joined/0, so every pre-join tick is a
    %% no-op and the first productive sync bucket job would otherwise lag join
    %% by minutes.
    collect_peers(),
    {noreply, State};
handle_info({event, node_state, _}, State) ->
    {noreply, State};
handle_info(collect_peers, State) ->
    (arweave_sync_deps:clock()):send_after(
        ?DATA_DISCOVERY_COLLECT_PEERS_FREQUENCY_MS, self(), collect_peers
    ),
    collect_peers(),
    {noreply, State};
handle_info(refresh_sync_buckets, State) ->
    schedule_sync_bucket_refresh(),
    State2 = enqueue_sync_bucket_jobs(State),
    {noreply, start_jobs(sync_bucket, State2)};
handle_info(maintenance, State0) ->
    (arweave_sync_deps:clock()):send_after(?MAINTENANCE_INTERVAL_MS, self(), maintenance),
    State = start_jobs(chunk_interval, run_maintenance(State0)),
    {noreply, start_jobs(sync_bucket, State)};
handle_info(Message, State) ->
    ?LOG_WARNING([{event, unhandled_info}, {message, Message}]),
    {noreply, State}.

terminate(Reason, State) ->
    #state{jobs = JobsByKind} = State,
    terminate_jobs(JobsByKind),
    ?LOG_INFO([{pid, self()}, {callback, terminate}, {reason, Reason}]),
    ok.

%%%===================================================================
%%% Maintenance.
%%%===================================================================

run_maintenance(State) ->
    delete_expired_sync_buckets(),
    trim_chunk_interval_cache(),
    emit_metrics(State),
    State.

%%%===================================================================
%%% Peer lifecycle.
%%%===================================================================

collect_peers() ->
    %% Wait until join. This prevents all the data_discovery traffic from
    %% slowing down the join process.
    case (arweave_sync_deps:node()):is_joined() of
        false ->
            ok;
        true ->
            LocalOnly = arweave_config:get([sync, local_peers_only]),
            Peers =
                case LocalOnly of
                    true ->
                        arweave_config:get([peers, local]);
                    false ->
                        %% rank peers by current rating since we care about their
                        %% recent throughput performance
                        (arweave_sync_deps:peers()):get_peers(current)
                end,
            gen_server:cast(
                ?MODULE,
                {add_peers, lists:sublist(Peers, ?MAX_DISCOVERY_PEERS)}
            )
    end.

do_remove_peer(Peer, Reason, State) ->
    #state{
        tracked_peers = TrackedPeers,
        jobs = JobsByKind
    } = State,
    State2 = State#state{
        tracked_peers = sets:del_element(Peer, TrackedPeers),
        jobs = maps:map(
            fun(_Kind, Jobs) -> remove_jobs(Peer, Jobs) end,
            JobsByKind
        )
    },
    NumSyncBuckets = delete_rows(?SYNC_BUCKET_CACHE_TABLE, Peer),
    NumChunkIntervals = delete_rows(?CHUNK_INTERVAL_CACHE_TABLE, Peer),
    record_chunk_interval_evictions(peer_removed, NumChunkIntervals),
    ?LOG_DEBUG([
        {event, peer_removed_from_discovery},
        {peer, arweave_util:format_peer(Peer)},
        {reason, Reason},
        {had_sync_buckets, NumSyncBuckets > 0},
        {remaining_buckets_rows, ets:info(?SYNC_BUCKET_CACHE_TABLE, size)}
    ]),
    State3 = start_jobs(chunk_interval, State2),
    {noreply, start_jobs(sync_bucket, State3)}.

is_peer_tracked(Peer, State) ->
    #state{tracked_peers = TrackedPeers} = State,
    sets:is_element(Peer, TrackedPeers).

peer_supports(byte, _Peer) ->
    true;
peer_supports(footprint, Peer) ->
    (arweave_sync_deps:peers()):get_peer_release(Peer) >= ?GET_FOOTPRINT_SUPPORT_RELEASE.

%%%===================================================================
%%% Generic job functions.
%%%===================================================================

inflight_count(#discovery_jobs{inflight = Inflight}) ->
    map_size(Inflight).

job_exists(Key, State) ->
    Jobs = jobs_for_key(Key, State),
    #discovery_jobs{
        pending = Pending,
        inflight = Inflight
    } = Jobs,
    PendingMatch =
        case maps:get(Key, Pending, undefined) of
            #discovery_job{key = Key} -> true;
            _ -> false
        end,
    PendingMatch orelse maps:is_key(Key, Inflight).

jobs_for_key(Key, #state{jobs = JobsByKind}) ->
    maps:get(element(1, Key), JobsByKind).

%% @doc Identify the peer, store, and mode shared by chunk interval locations.
peer_store_mode_key({chunk_interval, Peer, StoreID, Mode, _Start}) ->
    {Peer, StoreID, Mode};
peer_store_mode_key(Key) ->
    Key.

%% @doc Add Job when its peer is tracked and its exact location is not active.
%% The global pending limit is opportunistic: a store below its small guaranteed
%% minimum may exceed it, so a late store can make progress without moving or
%% evicting work that was already queued.
enqueue_job(Job, State) ->
    #discovery_job{peer = Peer} = Job,
    case is_peer_tracked(Peer, State) of
        true -> enqueue_tracked_job(Job, State);
        false -> State
    end.

enqueue_tracked_job(#discovery_job{key = Key, kind = Kind} = Job, State) ->
    #state{jobs = JobsByKind} = State,
    Jobs = maps:get(Kind, JobsByKind),
    Jobs2 =
        case job_exists(Key, State) of
            true ->
                refresh_pending_job(Job, Jobs);
            false ->
                enqueue_new_job(Job, Jobs)
        end,
    State#state{jobs = JobsByKind#{Kind := Jobs2}}.

enqueue_new_job(Job, Jobs) ->
    #discovery_job{key = Key} = Job,
    #discovery_jobs{pending = Pending} = Jobs,
    case has_pending_capacity(Job, Jobs) of
        true ->
            Jobs#discovery_jobs{
                pending = maps:put(Key, Job, Pending)
            };
        false ->
            Jobs
    end.

refresh_pending_job(
    #discovery_job{key = Key} = Job,
    #discovery_jobs{pending = Pending} = Jobs
) ->
    case maps:is_key(Key, Pending) of
        true ->
            RefreshedJob = Job#discovery_job{
                requested_at = (arweave_sync_deps:clock()):monotonic_ms()
            },
            Jobs#discovery_jobs{
                pending = maps:put(Key, RefreshedJob, Pending)
            };
        false ->
            Jobs
    end.

has_pending_capacity(
    #discovery_job{kind = chunk_interval, store_id = StoreID},
    #discovery_jobs{
        pending = Pending,
        max_pending = MaxPending
    }
) ->
    map_size(Pending) < MaxPending orelse
        pending_job_count(StoreID, Pending) <
            ?MIN_PENDING_CHUNK_INTERVAL_JOBS_PER_STORE;
has_pending_capacity(_Job, #discovery_jobs{
    pending = Pending,
    max_pending = MaxPending
}) ->
    map_size(Pending) < MaxPending.

pending_job_count(StoreID, Pending) ->
    maps:fold(
        fun
            (_Key, #discovery_job{store_id = PendingStoreID}, Count) when
                PendingStoreID =:= StoreID
            ->
                Count + 1;
            (_Key, _Job, Count) ->
                Count
        end,
        0,
        Pending
    ).

job_counts_by_store(Jobs) ->
    maps:fold(
        fun(_Key, #discovery_job{store_id = StoreID}, Counts) ->
            arweave_util:increment_map_value(StoreID, Counts)
        end,
        #{},
        Jobs
    ).

%% @doc Spawn Job and track its process PID on the inflight job.
start_job(Job, Jobs) ->
    #discovery_job{key = Key} = Job,
    #discovery_jobs{inflight = Inflight} = Jobs,
    {Pid, _Ref} = spawn_monitor(fun() -> run_job(Job) end),
    Jobs#discovery_jobs{
        inflight = maps:put(Key, Job#discovery_job{pid = Pid}, Inflight)
    }.

run_job(#discovery_job{kind = sync_bucket, peer = Peer}) ->
    fetch_sync_buckets(Peer);
run_job(#discovery_job{kind = chunk_interval} = Job) ->
    refresh_chunk_intervals(Job).

pid_to_job(Pid, Inflight) ->
    case
        lists:search(
            fun({_Key, #discovery_job{pid = JobPid}}) ->
                JobPid =:= Pid
            end,
            maps:to_list(Inflight)
        )
    of
        {value, {Key, _Job}} -> {ok, Key};
        false -> error
    end.

finish_job(Pid, State) ->
    #state{jobs = JobsByKind} = State,
    Result = maps:fold(
        fun
            (_Kind, _Jobs, {ok, _, _} = Acc) ->
                Acc;
            (Kind, Jobs, error) ->
                #discovery_jobs{inflight = Inflight} = Jobs,
                case pid_to_job(Pid, Inflight) of
                    {ok, Key} ->
                        Jobs2 = Jobs#discovery_jobs{
                            inflight = maps:remove(Key, Inflight)
                        },
                        {ok, Kind, Jobs2};
                    error ->
                        error
                end
        end,
        error,
        JobsByKind
    ),
    case Result of
        {ok, Kind, Jobs2} ->
            Jobs3 = start_jobs(Jobs2),
            State#state{jobs = JobsByKind#{Kind := Jobs3}};
        error ->
            State
    end.

%% @doc Remove all work for Peer and stop its inflight job processes.
remove_jobs(Peer, Jobs) ->
    #discovery_jobs{
        pending = Pending,
        inflight = Inflight
    } = Jobs,
    Jobs#discovery_jobs{
        pending = maps:filter(
            fun(_Key, #discovery_job{peer = JobPeer}) ->
                JobPeer =/= Peer
            end,
            Pending
        ),
        inflight = maps:filter(
            fun
                (
                    _Key,
                    #discovery_job{
                        peer = JobPeer,
                        pid = JobPID
                    }
                ) when JobPeer =:= Peer ->
                    terminate_job(JobPID),
                    false;
                (_Key, _Job) ->
                    true
            end,
            Inflight
        )
    }.

%% @doc Start pending jobs while preferring idle peers and stores.
start_jobs(Kind, State) ->
    #state{jobs = JobsByKind} = State,
    Jobs = maps:get(Kind, JobsByKind),
    Jobs2 = start_jobs(Jobs),
    State#state{jobs = JobsByKind#{Kind := Jobs2}}.

start_jobs(Jobs) ->
    case has_capacity(Jobs) of
        false ->
            %% Do not inspect pending jobs at the concurrency limit.
            Jobs;
        true ->
            {PeerLoad, StoreLoad} = compute_job_load(Jobs),
            PeerStoreModeCounts = inflight_peer_store_mode_counts(Jobs),
            start_pending_jobs(
                Jobs, PeerLoad, StoreLoad, PeerStoreModeCounts
            )
    end.

start_pending_jobs(Jobs, PeerLoad, StoreLoad, PeerStoreModeCounts) ->
    case has_capacity(Jobs) of
        true ->
            case
                take_next_job(
                    Jobs, PeerLoad, StoreLoad, PeerStoreModeCounts
                )
            of
                {ok,
                    #discovery_job{
                        peer = Peer,
                        store_id = StoreID
                    } = Job,
                    Jobs2} ->
                    Jobs3 = start_job(Job, Jobs2),
                    PeerStoreMode = peer_store_mode_key(
                        Job#discovery_job.key
                    ),
                    start_pending_jobs(
                        Jobs3,
                        arweave_util:increment_map_value(Peer, PeerLoad),
                        arweave_util:increment_map_value(StoreID, StoreLoad),
                        arweave_util:increment_map_value(
                            PeerStoreMode, PeerStoreModeCounts
                        )
                    );
                none ->
                    Jobs
            end;
        false ->
            Jobs
    end.

inflight_peer_store_mode_counts(#discovery_jobs{inflight = Inflight}) ->
    maps:fold(
        fun(Key, _Job, Acc) ->
            arweave_util:increment_map_value(peer_store_mode_key(Key), Acc)
        end,
        #{},
        Inflight
    ).

has_capacity(#discovery_jobs{max_inflight = infinity}) ->
    true;
has_capacity(#discovery_jobs{max_inflight = MaxInflight} = Jobs) ->
    inflight_count(Jobs) < MaxInflight.

compute_job_load(#discovery_jobs{inflight = Inflight}) ->
    maps:fold(
        fun(_Key, Job, {PeerLoad, StoreLoad}) ->
            #discovery_job{
                peer = Peer,
                store_id = StoreID
            } = Job,
            {
                arweave_util:increment_map_value(Peer, PeerLoad),
                arweave_util:increment_map_value(StoreID, StoreLoad)
            }
        end,
        {#{}, #{}},
        Inflight
    ).

take_next_job(Jobs, PeerLoad, StoreLoad, PeerStoreModeCounts) ->
    #discovery_jobs{pending = Pending} = Jobs,
    RunnableByPeerStoreMode = maps:fold(
        fun(PendingKey, Job, Acc) ->
            PeerStoreMode = peer_store_mode_key(PendingKey),
            case
                maps:get(PeerStoreMode, PeerStoreModeCounts, 0) <
                    max_jobs_per_peer_store_mode(PeerStoreMode)
            of
                true ->
                    Candidate = {PendingKey, Job},
                    maps:update_with(
                        PeerStoreMode,
                        fun(Existing) ->
                            preferred_pending_job(Candidate, Existing)
                        end,
                        Candidate,
                        Acc
                    );
                false ->
                    Acc
            end
        end,
        #{},
        Pending
    ),
    case maps:values(RunnableByPeerStoreMode) of
        [] ->
            none;
        PendingJobs ->
            %% Tuple ordering prefers less-loaded peers, then less-loaded stores.
            %% Job keys provide deterministic ordering when both loads are equal.
            RankedJobs = lists:map(
                fun({Key, Job}) ->
                    {job_load(Job, PeerLoad, StoreLoad), Key, Job}
                end,
                PendingJobs
            ),
            {_Load, SelectedKey, Selected} = lists:min(RankedJobs),
            {ok, Selected, Jobs#discovery_jobs{
                pending = maps:remove(SelectedKey, Pending)
            }}
    end.

max_jobs_per_peer_store_mode({_Peer, _StoreID, footprint}) ->
    ?MAX_FOOTPRINT_JOBS_PER_PEER_STORE;
max_jobs_per_peer_store_mode(_PeerStoreMode) ->
    ?MAX_BYTE_JOBS_PER_PEER_STORE.

preferred_pending_job(
    {Key1, Job1} = Candidate1,
    {Key2, Job2} = Candidate2
) ->
    case
        pending_job_priority(Job1, Key1) >=
            pending_job_priority(Job2, Key2)
    of
        true -> Candidate1;
        false -> Candidate2
    end.

pending_job_priority(
    #discovery_job{
        kind = chunk_interval,
        requested_at = undefined,
        start = Start
    },
    _Key
) ->
    {0, 0, -Start};
pending_job_priority(
    #discovery_job{
        kind = chunk_interval,
        requested_at = RequestedAt,
        start = Start
    },
    _Key
) ->
    {1, RequestedAt, -Start};
pending_job_priority(_Job, Key) ->
    {0, 0, Key}.

job_load(Job, PeerLoad, StoreLoad) ->
    #discovery_job{peer = Peer, store_id = StoreID} = Job,
    {maps:get(Peer, PeerLoad, 0), maps:get(StoreID, StoreLoad, 0)}.

terminate_jobs(JobsByKind) ->
    maps:foreach(
        fun(_Kind, #discovery_jobs{inflight = Inflight}) ->
            maps:foreach(
                fun(_Key, #discovery_job{pid = JobPID}) ->
                    terminate_job(JobPID)
                end,
                Inflight
            )
        end,
        JobsByKind
    ).

terminate_job(JobPID) ->
    MonitorRef = erlang:monitor(process, JobPID),
    exit(JobPID, kill),
    receive
        {'DOWN', MonitorRef, process, JobPID, _Reason} -> ok
    end.

%%%===================================================================
%%% Sync bucket job functions.
%%%===================================================================

%% Track a new peer and request its first sync bucket job immediately. The
%% caller drives start_jobs/2 after adding the current peers.
%% Peer lifecycle (add/remove) is driven entirely by ar_peers reachability
%% events; ar_peers only drops a peer once its rolling average_success falls
%% below ?MINIMUM_SUCCESS, which is the grace against transient hiccups.
add_peer(Peer, State) ->
    #state{tracked_peers = TrackedPeers} = State,
    case sets:is_element(Peer, TrackedPeers) of
        true ->
            State;
        false ->
            State2 = State#state{
                tracked_peers = sets:add_element(Peer, TrackedPeers)
            },
            enqueue_job(sync_bucket_job(Peer), State2)
    end.

sync_bucket_job(Peer) ->
    #discovery_job{
        key = {sync_bucket, Peer},
        kind = sync_bucket,
        peer = Peer
    }.

%% @doc Queue one sync bucket refresh for every tracked peer. Existing pending
%% or inflight work is retained without duplication.
enqueue_sync_bucket_jobs(#state{tracked_peers = TrackedPeers} = State) ->
    sets:fold(
        fun(Peer, Acc) -> enqueue_job(sync_bucket_job(Peer), Acc) end,
        State,
        TrackedPeers
    ).

%% @doc Schedule the next node-wide refresh with +/-25% jitter so nodes that
%% start together do not repeatedly query peers at the same time.
schedule_sync_bucket_refresh() ->
    {ok, _} = (arweave_sync_deps:clock()):send_after(
        sync_bucket_refresh_delay_ms(), self(), refresh_sync_buckets
    ),
    ok.

sync_bucket_refresh_delay_ms() ->
    Spread = ?SYNC_BUCKET_JOB_INTERVAL_MS div 4,
    ?SYNC_BUCKET_JOB_INTERVAL_MS - Spread + rand:uniform(2 * Spread + 1) - 1.

%% Fetch the peer's coarse availability for each mode
%% sequentially so one peer occupies at most one metadata connection at a time.
fetch_sync_buckets(Peer) ->
    fetch_sync_buckets(byte, Peer),
    fetch_sync_buckets(footprint, Peer).

fetch_sync_buckets(Mode, Peer) ->
    try
        maybe
            true ?= peer_supports(Mode, Peer),
            {ok, SyncBuckets} ?= get_sync_buckets(Mode, Peer),
            gen_server:cast(
                ?MODULE,
                {job_result, Peer, {sync_buckets, Mode, SyncBuckets}}
            )
        else
            false ->
                ok;
            {error, request_type_not_found} ->
                ?LOG_DEBUG([
                    {event, sync_buckets_request_type_not_found},
                    {peer, arweave_util:format_peer(Peer)},
                    {mode, Mode}
                ]),
                error;
            {error, RequestError} ->
                (arweave_sync_deps:http()):log_failed_request(
                    RequestError,
                    [
                        {event, failed_to_fetch_sync_buckets},
                        {peer, arweave_util:format_peer(Peer)},
                        {mode, Mode},
                        {reason, io_lib:format("~p", [RequestError])}
                    ]
                ),
                error;
            Other ->
                ?LOG_DEBUG([
                    {event, failed_to_fetch_sync_buckets},
                    {peer, arweave_util:format_peer(Peer)},
                    {mode, Mode},
                    {reason, io_lib:format("~p", [Other])}
                ]),
                error
        end
    catch
        Class:Reason:Stacktrace ->
            ?LOG_WARNING([
                {event, refresh_sync_buckets_failed},
                {peer, arweave_util:format_peer(Peer)},
                {mode, Mode},
                {class, Class},
                {reason, io_lib:format("~p", [Reason])},
                {stacktrace, Stacktrace}
            ])
    end,
    ok.

%%%===================================================================
%%% Cache manipulation.
%%%===================================================================

record_job_result(Peer, {sync_buckets, Mode, SyncBuckets}, State) ->
    store_sync_buckets(Mode, Peer, SyncBuckets),
    ?LOG_DEBUG([
        {event, processed_sync_buckets},
        {peer, arweave_util:format_peer(Peer)},
        {mode, Mode}
    ]),
    State;
record_job_result(
    Peer,
    {chunk_intervals, _StoreID, Offset, Mode, {ok, Intervals}},
    State
) ->
    store_row(chunk_interval, Mode, Offset, Peer, Intervals),
    State;
record_job_result(
    Peer,
    {chunk_intervals, _StoreID, Offset, Mode, _Error},
    State
) ->
    %% Do not continue serving stale metadata when its replacement cannot be
    %% fetched. A later sweep will request it again.
    delete_row(chunk_interval, Mode, Offset, Peer),
    State.

%% @doc Peer comes last so one bucket's rows across peers are contiguous: this
%% table is walked per bucket to find the peers advertising it.
sync_bucket_key(Mode, SyncBucket, Peer) ->
    {Mode, SyncBucket, Peer}.

%% @doc The key of the peer's chunk-interval row covering Offset. Peer
%% precedes the location so one peer's rows for a bucket are contiguous: this
%% table is walked per peer (chunk_interval_keys_in_bucket/3).
chunk_interval_key(Mode, Peer, Offset) ->
    {Mode, Peer, interval_location(Mode, Offset)}.

store_row(sync_bucket, Mode, SyncBucket, Peer, Share) ->
    Key = sync_bucket_key(Mode, SyncBucket, Peer),
    insert_row(?SYNC_BUCKET_CACHE_TABLE, Key, Share);
store_row(chunk_interval, Mode, Offset, Peer, Intervals) ->
    Key = chunk_interval_key(Mode, Peer, Offset),
    insert_row(?CHUNK_INTERVAL_CACHE_TABLE, Key, Intervals).

insert_row(Table, Key, Value) ->
    Now = (arweave_sync_deps:clock()):monotonic_ms(),
    ets:insert(Table, {Key, Value, Now}),
    ok.

delete_row(sync_bucket, Mode, SyncBucket, Peer) ->
    Key = sync_bucket_key(Mode, SyncBucket, Peer),
    ets:delete(?SYNC_BUCKET_CACHE_TABLE, Key);
delete_row(chunk_interval, Mode, Offset, Peer) ->
    Key = chunk_interval_key(Mode, Peer, Offset),
    ets:delete(?CHUNK_INTERVAL_CACHE_TABLE, Key).

%% @doc Delete all cache rows for Peer from a discovery table.
delete_rows(?SYNC_BUCKET_CACHE_TABLE = Table, Peer) ->
    ets:select_delete(
        Table,
        [{{{'_', '_', Peer}, '_', '_'}, [], [true]}]
    );
delete_rows(?CHUNK_INTERVAL_CACHE_TABLE = Table, Peer) ->
    ets:select_delete(
        Table,
        [{{{'_', Peer, '_'}, '_', '_'}, [], [true]}]
    ).

expiration_cutoff(CacheTTLMs) ->
    (arweave_sync_deps:clock()):monotonic_ms() - CacheTTLMs.

%%%===================================================================
%%% Sync bucket cache.
%%%===================================================================

store_sync_buckets(Mode, Peer, SyncBuckets) ->
    (arweave_sync_deps:sync_buckets()):foreach(
        fun(SyncBucket, Share) ->
            mark_chunk_intervals_stale_on_share_change(
                Mode, Peer, SyncBucket, Share
            ),
            store_row(sync_bucket, Mode, SyncBucket, Peer, Share)
        end,
        sync_bucket_size(Mode),
        sync_bucket_range_end(Mode, (arweave_sync_deps:node()):get_weave_size()),
        SyncBuckets
    ),
    ok.

sync_bucket_size(byte) ->
    (arweave_sync_deps:sync_buckets()):get_network_data_bucket_size();
sync_bucket_size(footprint) ->
    (arweave_sync_deps:sync_buckets()):get_network_footprint_bucket_size().

sync_bucket_range_end(byte, WeaveSize) ->
    WeaveSize;
sync_bucket_range_end(footprint, WeaveSize) ->
    arweave_storage:max_footprint_offset(WeaveSize).

%% @doc Mark cached chunk intervals stale when a refreshed sync bucket's share
%% differs from the cached share.
mark_chunk_intervals_stale_on_share_change(
    Mode, Peer, SyncBucket, NewShare
) ->
    Key = sync_bucket_key(Mode, SyncBucket, Peer),
    case ets:lookup(?SYNC_BUCKET_CACHE_TABLE, Key) of
        [{_, NewShare, _}] ->
            ok;
        [] ->
            ok;
        [{_, _ChangedShare, _}] ->
            mark_chunk_intervals_stale(Mode, Peer, SyncBucket)
    end.

%% @doc The inclusive chunk-interval location range a coarse bucket covers. Byte
%% rows are located by byte offset; footprint rows by {Partition, Footprint},
%% whose lexicographic order matches the partition-major, footprint-major
%% linear layout (arweave_storage:get_footprint_offset/1), so a peer's rows for
%% a bucket are contiguous in both modes. Footprint buckets don't align exactly
%% to footprint boundaries (the +1 linear-offset convention lets an edge
%% footprint straddle), so the range is a one-footprint superset at each
%% end. Extra rows are refreshed or removed with the bucket.
chunk_interval_location_range(byte, SyncBucket) ->
    Lo = SyncBucket * ?NETWORK_DATA_BUCKET_SIZE,
    {Lo, Lo + ?NETWORK_DATA_BUCKET_SIZE - 1};
chunk_interval_location_range(footprint, SyncBucket) ->
    FootprintSize = (arweave_sync_deps:constants()):get_sub_chunks_per_replica_2_9_entropy(),
    FootprintsPerPartition = (arweave_sync_deps:constants()):get_replica_2_9_footprints_per_partition(),
    LoGlobal = (SyncBucket * ?NETWORK_FOOTPRINT_BUCKET_SIZE) div FootprintSize,
    HiGlobal = ((SyncBucket + 1) * ?NETWORK_FOOTPRINT_BUCKET_SIZE) div FootprintSize,
    {{LoGlobal div FootprintsPerPartition, LoGlobal rem FootprintsPerPartition}, {
        HiGlobal div FootprintsPerPartition, HiGlobal rem FootprintsPerPartition
    }}.

%% @doc Retire sync bucket rows whose stamp was not refreshed within
%% ?SYNC_BUCKET_CACHE_TTL_MS (withdrawn advertisements and silently dead
%% peers), dropping its cached chunk intervals with it.
%% Runs from periodic maintenance.
delete_expired_sync_buckets() ->
    Cutoff = expiration_cutoff(?SYNC_BUCKET_CACHE_TTL_MS),
    Expired = ets:select(
        ?SYNC_BUCKET_CACHE_TABLE,
        [{{'$1', '_', '$2'}, [{'<', '$2', Cutoff}], ['$1']}]
    ),
    lists:foreach(
        fun({Mode, SyncBucket, Peer}) ->
            delete_row(sync_bucket, Mode, SyncBucket, Peer),
            delete_chunk_intervals(Mode, Peer, SyncBucket)
        end,
        Expired
    ),
    case Expired of
        [] ->
            ok;
        _ ->
            ?LOG_DEBUG([
                {event, deleted_expired_sync_buckets},
                {count, length(Expired)}
            ])
    end.

%%%===================================================================
%%% Chunk interval job functions.
%%%===================================================================

chunk_interval_job(Mode, PeerRange) ->
    #peer_range{
        peer = Peer,
        store_id = StoreID,
        offset = Offset
    } = PeerRange,
    Start =
        case Mode of
            byte -> interval_location(byte, Offset);
            footprint -> Offset
        end,
    #discovery_job{
        key = {chunk_interval, Peer, StoreID, Mode, Start},
        kind = chunk_interval,
        peer = Peer,
        store_id = StoreID,
        mode = Mode,
        start = Start
    }.

%% @doc Refresh one exact chunk interval location. The sweeper owns bounded
%% readahead and submits each future location as a separate discovery job.
refresh_chunk_intervals(ChunkIntervalJob) ->
    #discovery_job{
        kind = chunk_interval,
        peer = Peer,
        store_id = StoreID,
        mode = Mode,
        start = Start
    } = ChunkIntervalJob,
    try
        maybe
            true ?= peer_has_sync_bucket(Mode, Peer, Start),
            true ?= chunk_interval_refresh_needed(Mode, Peer, Start),
            Location = interval_location(Mode, Start),
            Result = fetch_chunk_intervals(Mode, Peer, Location),
            gen_server:cast(
                ?MODULE,
                {job_result, Peer, {chunk_intervals, StoreID, Start, Mode, Result}}
            )
        else
            false ->
                ok
        end
    catch
        Class:Reason:Stacktrace ->
            ?LOG_WARNING([
                {event, chunk_interval_job_crashed},
                {class, Class},
                {peer, arweave_util:format_peer(Peer)},
                {mode, Mode},
                {reason, io_lib:format("~p", [Reason])},
                {stacktrace, Stacktrace}
            ])
    end.

%%%===================================================================
%%% Chunk interval cache.
%%%===================================================================

mark_chunk_intervals_stale(Mode, Peer, SyncBucket) ->
    StaleTimestamp =
        expiration_cutoff(?CHUNK_INTERVAL_CACHE_TTL_MS) - 1,
    lists:foreach(
        fun(Key) ->
            ets:update_element(
                ?CHUNK_INTERVAL_CACHE_TABLE, Key, {3, StaleTimestamp}
            )
        end,
        chunk_interval_keys_in_bucket(Mode, Peer, SyncBucket)
    ).

delete_chunk_intervals(Mode, Peer, SyncBucket) ->
    Keys = chunk_interval_keys_in_bucket(Mode, Peer, SyncBucket),
    lists:foreach(
        fun(Key) -> ets:delete(?CHUNK_INTERVAL_CACHE_TABLE, Key) end, Keys
    ),
    record_chunk_interval_evictions(withdrawn, length(Keys)).

%% @doc Return the keys of the peer's cached chunk-interval rows inside the
%% bucket. Keyed {Mode, Peer, Location}, they form one contiguous range: walk
%% just that. A select with the location unbound traverses every row of the
%% mode instead, tens of thousands per bucket, which stalled this server for
%% minutes whenever a peer's hourly bucket refresh changed thousands of
%% shares.
chunk_interval_keys_in_bucket(Mode, Peer, SyncBucket) ->
    {Lo, Hi} = chunk_interval_location_range(Mode, SyncBucket),
    First = {Mode, Peer, Lo},
    Start =
        case ets:member(?CHUNK_INTERVAL_CACHE_TABLE, First) of
            true -> First;
            false -> ets:next(?CHUNK_INTERVAL_CACHE_TABLE, First)
        end,
    collect_chunk_interval_keys(Mode, Peer, Hi, Start, []).

collect_chunk_interval_keys(
    Mode, Peer, Hi, {Mode, Peer, Location} = Key, Acc
) when Location =< Hi ->
    Next = ets:next(?CHUNK_INTERVAL_CACHE_TABLE, Key),
    collect_chunk_interval_keys(Mode, Peer, Hi, Next, [Key | Acc]);
collect_chunk_interval_keys(_Mode, _Peer, _Hi, _PastBucket, Acc) ->
    lists:reverse(Acc).

%% @doc Look up a cached interval row, age-gated by the shared safety-floor
%% TTL. A row past it comes back as {stale, ...}: callers still use the
%% intervals (an aged row beats stalling the pipeline on a miss) but treat the
%% row as needing a chunk interval job. refresh_chunk_intervals/1 uses this
%% same freshness result for both modes, so a stale row is replaced rather than
%% reused. Primary freshness is share-diffing; this TTL is the safety floor.
chunk_interval_lookup(Mode, Peer, Offset) ->
    Key = chunk_interval_key(Mode, Peer, Offset),
    case ets:lookup(?CHUNK_INTERVAL_CACHE_TABLE, Key) of
        [{_, Intervals, MonotonicMs}] ->
            Cutoff = expiration_cutoff(?CHUNK_INTERVAL_CACHE_TTL_MS),
            case MonotonicMs < Cutoff of
                true -> {stale, Intervals};
                false -> {hit, Intervals}
            end;
        [] ->
            miss
    end.

%% Trim runs from periodic maintenance rather than after every stored result.
%% A burst of cache-result messages could otherwise trigger repeated 10%
%% deletions while ETS memory accounting still reflects rows already removed.
%% The maintenance cadence gives ETS accounting time to settle between checks.
trim_chunk_interval_cache() ->
    Bytes =
        ets:info(?CHUNK_INTERVAL_CACHE_TABLE, memory) *
            erlang:system_info(wordsize),
    MaxBytes = (arweave_sync_deps:chunk_cache()):interval_limit(),
    case Bytes > MaxBytes of
        false ->
            ok;
        true ->
            Size = ets:info(?CHUNK_INTERVAL_CACHE_TABLE, size),
            ToDelete = max(1, Size div ?CHUNK_INTERVAL_CACHE_TRIM_DIVISOR),
            Timestamps = ets:select(
                ?CHUNK_INTERVAL_CACHE_TABLE,
                [{{'_', '_', '$1'}, [], ['$1']}]
            ),
            Sorted = lists:sort(Timestamps),
            Threshold = lists:nth(min(ToDelete, length(Sorted)), Sorted),
            Deleted = ets:select_delete(
                ?CHUNK_INTERVAL_CACHE_TABLE,
                [{{'_', '_', '$1'}, [{'=<', '$1', Threshold}], [true]}]
            ),
            record_chunk_interval_evictions(trim, Deleted),
            MbAfter =
                ets:info(?CHUNK_INTERVAL_CACHE_TABLE, memory) *
                    erlang:system_info(wordsize) div ?MiB,
            ?LOG_DEBUG([
                {event, chunk_interval_cache_trimmed},
                {rows_before, Size},
                {rows_deleted, Deleted},
                {rows_after, Size - Deleted},
                {mb_before, Bytes div ?MiB},
                {mb_after, MbAfter},
                {cap_mb, MaxBytes div ?MiB}
            ])
    end.

%%%===================================================================
%%% Chunk interval HTTP requests.
%%%===================================================================

get_sync_buckets(byte, Peer) ->
    (arweave_sync_deps:http()):get_sync_buckets(Peer);
get_sync_buckets(footprint, Peer) ->
    (arweave_sync_deps:http()):get_footprint_buckets(Peer).

fetch_chunk_intervals(byte, Peer, Left) ->
    Right = Left + arweave_sync_cursor:query_range_step_size(),
    %% Older peers ignore the right bound and return an open-ended range, which
    %% is cut to Right after pagination.
    Bound =
        case
            (arweave_sync_deps:peers()):get_peer_release(Peer) >=
                ?GET_SYNC_RECORD_RIGHT_BOUND_SUPPORT_RELEASE
        of
            true -> Right;
            false -> none
        end,
    fetch_chunk_interval_pages(
        Peer,
        Bound,
        Left,
        Right,
        ?MAX_CHUNK_INTERVAL_PAGES,
        ar_intervals:new()
    );
fetch_chunk_intervals(footprint, Peer, {Partition, Footprint}) ->
    case
        (arweave_sync_deps:http()):get_footprints(
            Peer, Partition, Footprint
        )
    of
        not_found ->
            {ok, ar_intervals:new()};
        Reply ->
            Reply
    end.

fetch_chunk_interval_pages(
    _Peer, _Bound, _Left, _Right, 0, _Intervals
) ->
    {error, interval_page_limit};
fetch_chunk_interval_pages(
    Peer, Bound, Left, Right, PagesLeft, Intervals
) ->
    case
        fetch_sync_record(
            Peer, Left + 1, Bound, ?QUERY_SYNC_INTERVALS_COUNT_LIMIT
        )
    of
        {ok, Page} ->
            case collect_chunk_interval_page(Left, Right, Intervals, Page) of
                {continue, PageEnd, Intervals2} ->
                    fetch_chunk_interval_pages(
                        Peer,
                        Bound,
                        PageEnd,
                        Right,
                        PagesLeft - 1,
                        Intervals2
                    );
                Result ->
                    Result
            end;
        Error ->
            Error
    end.

fetch_sync_record(Peer, Start, none, Limit) ->
    (arweave_sync_deps:http()):get_sync_record(Peer, Start, Limit);
fetch_sync_record(Peer, Start, Right, Limit) ->
    (arweave_sync_deps:http()):get_sync_record(Peer, Start, Right, Limit).

%% The peer has nothing more to give once it answers with a page shorter than
%% the one it was asked for, or one reaching Right. ar_http_iface_client rejects
%% pages that begin before the requested cursor.
collect_chunk_interval_page(_Left, Right, Intervals, Page) ->
    case ar_intervals:count(Page) of
        0 ->
            {ok, ar_intervals:cut(Intervals, Right)};
        Count ->
            {PageEnd, _PageStart} = ar_intervals:largest(Page),
            Intervals2 = ar_intervals:union(Intervals, Page),
            case
                PageEnd >= Right orelse
                    Count < ?QUERY_SYNC_INTERVALS_COUNT_LIMIT
            of
                true ->
                    {ok, ar_intervals:cut(Intervals2, Right)};
                false ->
                    {continue, PageEnd, Intervals2}
            end
    end.

%%%===================================================================
%%% Metrics.
%%%===================================================================

emit_metrics(State) ->
    #state{
        tracked_peers = TrackedPeers,
        jobs = JobsByKind
    } = State,
    arweave_metrics:gauge_set(
        discovery_peers_scanned,
        sets:size(TrackedPeers)
    ),
    arweave_metrics:gauge_set(
        chunk_interval_cache_size,
        [rows],
        ets:info(?CHUNK_INTERVAL_CACHE_TABLE, size)
    ),
    arweave_metrics:gauge_set(
        chunk_interval_cache_size,
        [bytes],
        ets:info(?CHUNK_INTERVAL_CACHE_TABLE, memory) *
            erlang:system_info(wordsize)
    ),
    maps:foreach(
        fun(Kind, Jobs) ->
            #discovery_jobs{
                pending = Pending,
                inflight = Inflight,
                max_inflight = MaxInflight
            } = Jobs,
            arweave_metrics:gauge_set(
                sync_discovery_jobs, [Kind, pending], map_size(Pending)
            ),
            arweave_metrics:gauge_set(
                sync_discovery_jobs, [Kind, inflight], map_size(Inflight)
            ),
            case MaxInflight of
                infinity ->
                    ok;
                _ ->
                    arweave_metrics:gauge_set(
                        sync_discovery_jobs, [Kind, max_inflight], MaxInflight
                    )
            end
        end,
        JobsByKind
    ),
    emit_chunk_interval_job_metrics(JobsByKind),
    emit_sync_discovery_peers().

emit_chunk_interval_job_metrics(JobsByKind) ->
    #discovery_jobs{pending = Pending, inflight = Inflight} =
        maps:get(chunk_interval, JobsByKind),
    PendingCounts = job_counts_by_store(Pending),
    InflightCounts = job_counts_by_store(Inflight),
    lists:foreach(
        fun(StoreID) ->
            #store_info{label = Label} = arweave_storage:store_info(StoreID),
            arweave_metrics:gauge_set(
                sync_chunk_interval_jobs_by_store,
                [pending, Label],
                maps:get(StoreID, PendingCounts, 0)
            ),
            arweave_metrics:gauge_set(
                sync_chunk_interval_jobs_by_store,
                [inflight, Label],
                maps:get(StoreID, InflightCounts, 0)
            )
        end,
        arweave_sync_store_sweeper:store_ids()
    ).

record_chunk_interval_evictions(_Reason, 0) ->
    ok;
record_chunk_interval_evictions(Reason, Count) ->
    arweave_metrics:counter_inc(chunk_interval_cache_evictions, [Reason], Count).

emit_sync_discovery_peers() ->
    Modes = [byte, footprint],
    StorePeerSets = lists:flatmap(
        fun(Module) -> get_peers_for_store(Module, Modes) end,
        arweave_config:storage_modules()
    ),
    lists:foreach(
        fun({Mode, StoreID, Peers}) ->
            arweave_metrics:gauge_set(
                sync_discovery_peers,
                [Mode, (arweave_storage:store_info(StoreID))#store_info.label],
                sets:size(Peers)
            )
        end,
        StorePeerSets
    ),
    ModePeerSets = lists:map(
        fun(Mode) ->
            {Mode, get_peers_for_mode(Mode, StorePeerSets)}
        end,
        Modes
    ),
    lists:foreach(
        fun({Mode, Peers}) ->
            arweave_metrics:gauge_set(
                sync_discovery_peers, [Mode, "all"], sets:size(Peers)
            )
        end,
        ModePeerSets
    ),
    AllPeers = lists:foldl(
        fun({_Mode, Peers}, Acc) -> sets:union(Acc, Peers) end,
        sets:new(),
        ModePeerSets
    ),
    arweave_metrics:gauge_set(
        sync_discovery_peers, [union, "all"], sets:size(AllPeers)
    ).

get_peers_for_mode(Mode, StorePeerSets) ->
    lists:foldl(
        fun({PeerMode, _StoreID, Peers}, Acc) ->
            case PeerMode =:= Mode of
                true -> sets:union(Acc, Peers);
                false -> Acc
            end
        end,
        sets:new(),
        StorePeerSets
    ).

get_peers_for_store(Module, Modes) ->
    #store_info{
        id = StoreID, effective_range = {RangeStart, RangeEnd}
    } = arweave_storage:store_info(Module),
    [
        {Mode, StoreID, get_peers_for_range(Mode, RangeStart, RangeEnd)}
     || Mode <- Modes
    ].

get_peers_for_range(Mode, RangeStart, RangeEnd) ->
    {StartSyncBucket, EndSyncBucket} =
        sync_bucket_range(Mode, RangeStart, RangeEnd),
    get_peers_for_sync_bucket_range(
        Mode, StartSyncBucket, EndSyncBucket, sets:new()
    ).

sync_bucket_range(byte, RangeStart, RangeEnd) ->
    {sync_bucket(byte, RangeStart), sync_bucket(byte, RangeEnd - 1)};
sync_bucket_range(footprint, RangeStart, RangeEnd) ->
    {sync_bucket(footprint, RangeStart), sync_bucket(footprint, RangeEnd - ?DATA_CHUNK_SIZE)}.

-ifdef(AR_TEST).

%%%===================================================================
%%% Test support.
%%%===================================================================

%% @doc Sim-driver support: how many discovery jobs are currently inflight.
%% Their processes sleep on the simulated clock, so the sim's settle invariant
%% must account for them.
inflight_count() ->
    gen_server:call(?MODULE, inflight_count).

%% @doc Drop all coarse bucket and chunk interval availability cached by discovery.
%% Intended for test setup so reused peer identifiers cannot inherit metadata
%% from a previous test case.
reset_all_caches() ->
    ensure_discovery_test_tables(),
    ets:delete_all_objects(?SYNC_BUCKET_CACHE_TABLE),
    ets:delete_all_objects(?CHUNK_INTERVAL_CACHE_TABLE),
    ok.

ensure_discovery_test_tables() ->
    ensure_named_table(?SYNC_BUCKET_CACHE_TABLE, ordered_set),
    ensure_named_table(?CHUNK_INTERVAL_CACHE_TABLE, ordered_set).

ensure_named_table(Name, Type) ->
    case ets:whereis(Name) of
        undefined ->
            ets:new(Name, [Type, public, named_table]),
            ok;
        _ ->
            ok
    end.

-endif.

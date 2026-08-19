%%% @doc Maintains a cache of the data ranges served by each peer.
%%%
%%% Sync bucket jobs cache coarse peer availability. Chunk interval jobs retrieve
%%% detailed availability from /data_sync_record and /footprints and cache it in
%%% ETS.
%%%
%%% ar_sync_store_sweeper reads this cache and gives ar_sync_chunk_picker an
%%% immutable snapshot when deciding which chunks can be fetched from which peers.
-module(ar_sync_discovery).
-test_category([fast]).

-behaviour(gen_server).

-export([start_link/0, get_peers_for_offset/1, warm_peer_ranges/3,
        cached_peer_ranges/5]).

-ifdef(AR_TEST).
-export([collect_peers/0, reset_all_caches/0, inflight_count/0]).
-endif.

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include("ar.hrl").
-include("ar_sync.hrl").


-ifdef(AR_TEST).
-include_lib("eunit/include/eunit.hrl").
-define(DATA_DISCOVERY_COLLECT_PEERS_FREQUENCY_MS, 2 * 1000).
-define(MAINTENANCE_INTERVAL_MS, 10_000).
-define(SYNC_BUCKET_JOB_INTERVAL_MS, 60_000).
-define(CHUNK_INTERVAL_CACHE_TTL_MS, 3_600_000).
-define(QUERY_SYNC_INTERVALS_COUNT_LIMIT, 10).
-else.
-define(DATA_DISCOVERY_COLLECT_PEERS_FREQUENCY_MS, 4 * 60 * 1000).
-define(MAINTENANCE_INTERVAL_MS, 60_000).
-define(SYNC_BUCKET_JOB_INTERVAL_MS, 60 * 60 * 1000). %% 1 hour
-define(CHUNK_INTERVAL_CACHE_TTL_MS, 24 * 60 * 60 * 1000). %% 24 hours
-define(QUERY_SYNC_INTERVALS_COUNT_LIMIT, 1000).
-endif.

%% Each peer, store, and mode has independent metadata, but querying multiple
%% locations for the same combination concurrently only duplicates work.
-define(MAX_CHUNK_INTERVAL_JOBS_PER_PEER_STORE_MODE, 1).

%% Bound each kind of metadata work so discovery cannot saturate the shared
%% request path and starve chunk fetching.
-define(MAX_DISCOVERY_JOBS_PER_KIND, 200).

%% Peer collection considers at most one thousand current peers per cycle.
-define(MAX_DISCOVERY_PEERS, 1000).

%% A bounded queue keeps detailed metadata close enough to the sweep frontier
%% to be useful within its fixed warming interval.
-define(MAX_PENDING_CHUNK_INTERVAL_JOBS, 1024).

%% Even when the opportunistic queue is full, retain one byte and one footprint
%% job for each of the thirty peers a sweep range may consider. This lets a late
%% store establish metadata work without evicting another store's jobs.
-define(MIN_PENDING_CHUNK_INTERVAL_JOBS_PER_STORE,
    2 * ?QUERY_BEST_PEERS_COUNT).

%% A normal query range needs about four pages. Sixteen bounds malformed
%% or unusually fragmented responses without constraining expected peers.
-define(MAX_CHUNK_INTERVAL_PAGES, 16).

%% Retain coarse rows for three job intervals so a brief failure does
%% not immediately retire advertised availability.
-define(SYNC_BUCKET_CACHE_TTL_MS, 3 * ?SYNC_BUCKET_JOB_INTERVAL_MS).

%% Drop the oldest tenth of the chunk interval cache when it exceeds its
%% byte budget; still-needed rows are re-warmed on demand.
-define(CHUNK_INTERVAL_CACHE_TRIM_DIVISOR, 10).

%%%===================================================================
%%% State and data structures.
%%%===================================================================

%% Per-peer cache of chunk intervals reported by each peer
%% (via /data_sync_record for byte mode and /footprints for
%% footprint mode). Populated by the chunk interval jobs in this module;
%% read by ar_sync_store_sweeper and passed to ar_sync_chunk_picker to compute
%% fetchable intervals.
%%
%% Rows are `{Key, Intervals, MonotonicMs}', keyed by
%% `{Mode, Location, Peer}'. The timestamp is used for demand-side freshness
%% checks and oldest-first cache trimming.
%% The chunk interval cache's byte limit is its carve-out of the
%% [sync, cache_size] budget — one knob for all sync memory — owned by
%% ar_data_sync (interval_cache_size_limit/0). When the cap fires we drop the
%% oldest 10% of rows in a single pass; still-needed rows are re-warmed on
%% demand.


%% Discovery work pending or inflight.
-record(discovery_job, {
    key,
    kind,
    peer,
    store_id = undefined,
    mode = undefined,
    start = undefined,
    requested_at = undefined,
    pid = undefined
}).

%% Shared state for sync bucket and chunk interval jobs.
-record(discovery_jobs, {
    %% Exact job key => #discovery_job{}.
    pending = #{},
    %% Key => #discovery_job{ pid = pid() }.
    inflight = #{},
    %% Maximum concurrent jobs in this collection.
    max_inflight = ?MAX_DISCOVERY_JOBS_PER_KIND,
    %% Opportunistic queued-job limit. Detailed metadata may exceed it by the
    %% small per-store guarantee; sync bucket work has one exact job per peer.
    max_pending = ?MAX_DISCOVERY_PEERS
}).

-record(state, {
    %% Authoritative current peer set used to reject cache results from
    %% removed peers.
    tracked_peers = sets:new(),
    jobs = #{
        %% Coarse per-peer jobs. Each job fetches byte and footprint metadata
        %% sequentially, and its exact key limits one job per peer.
        sync_bucket => #discovery_jobs{},
        %% Demand-driven chunk interval jobs. Sync bucket jobs only update
        %% which peers hold which coarse sync buckets; chunk intervals
        %% from /footprints and /data_sync_record are fetched on demand for a
        %% bounded span beginning at a store's sweep frontier. Concurrency is
        %% limited so discovery cannot saturate the shared request path and
        %% starve /chunk2 fetching.
        chunk_interval => #discovery_jobs{
            max_pending = ?MAX_PENDING_CHUNK_INTERVAL_JOBS
        }
    }
}).

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
        [byte, footprint]),
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
            ets:member(?SYNC_BUCKET_CACHE_TABLE,
                row_key(sync_bucket, Mode, SyncBucket, Peer));
        _ ->
            false
    end.

sync_bucket(byte, Offset) ->
    Offset div ?NETWORK_DATA_BUCKET_SIZE;
sync_bucket(footprint, Offset) ->
    ar_footprint_record:get_footprint_bucket(Offset + ?DATA_CHUNK_SIZE).

get_peers_for_sync_bucket(Mode, SyncBucket) ->
    get_peers_for_sync_bucket(Mode, SyncBucket, sets:new()).

%% @doc Return Peers plus peers advertising SyncBucket for Mode. Rows for both
%% modes share the ?SYNC_BUCKET_CACHE_TABLE ordered_set under
%% {Mode, SyncBucket, Peer} keys
%% (byte and footprint bucket numbers are different index spaces, so Mode
%% leads); no_peer sorts before any peer tuple, so this is a prefix walk.
get_peers_for_sync_bucket(Mode, SyncBucket, Peers) ->
    get_peers_for_sync_bucket(
        Mode, SyncBucket, Peers,
        row_key(sync_bucket, Mode, SyncBucket, no_peer)).

get_peers_for_sync_bucket(Mode, SyncBucket, Peers, Cursor) ->
    case ets:next(?SYNC_BUCKET_CACHE_TABLE, Cursor) of
        {Mode, SyncBucket, Peer} = Key ->
            get_peers_for_sync_bucket(
                Mode, SyncBucket, sets:add_element(Peer, Peers), Key);
        _ ->
            Peers
    end.

get_peers_for_sync_bucket_range(
        _Mode, StartSyncBucket, EndSyncBucket, Peers)
        when StartSyncBucket > EndSyncBucket ->
    Peers;
get_peers_for_sync_bucket_range(
        Mode, StartSyncBucket, EndSyncBucket, Peers) ->
    collect_sync_bucket_peers(Mode, EndSyncBucket, Peers,
        row_key(sync_bucket, Mode, StartSyncBucket, no_peer)).

%% One prefix walk over the whole bucket range. Rows are keyed
%% {Mode, SyncBucket, Peer} in an ordered_set, so ets:next lands on the next
%% populated row and empty buckets cost nothing. Visiting each bucket in turn
%% instead costs a lookup per bucket, and a store's range spans far more
%% buckets than the peers serving it ever populate.
collect_sync_bucket_peers(Mode, EndSyncBucket, Peers, Cursor) ->
    case ets:next(?SYNC_BUCKET_CACHE_TABLE, Cursor) of
        {Mode, SyncBucket, Peer} = Key when SyncBucket =< EndSyncBucket ->
            collect_sync_bucket_peers(
                Mode, EndSyncBucket, sets:add_element(Peer, Peers), Key);
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
    lists:foreach(
        fun(Peer) ->
            warm_peer_range(byte, StoreID, Peer, Offset),
            warm_peer_range(footprint, StoreID, Peer, Offset)
        end,
        Peers),
    ok.

warm_peer_range(Mode, StoreID, Peer, Offset) ->
    maybe
        true ?= peer_supports(Mode, Peer),
        true ?= peer_has_sync_bucket(Mode, Peer, Offset),
        true ?= chunk_interval_refresh_needed(Mode, Peer, Offset),
        PeerRange = #peer_range{
            store_id = StoreID,
            offset = Offset,
            peer = Peer
        },
        gen_server:cast(?MODULE,
            {refresh_chunk_intervals,
                chunk_interval_job(Mode, PeerRange)})
    else
        false ->
            ok
    end.

chunk_interval_refresh_needed(Mode, Peer, Offset) ->
    case chunk_interval_lookup(Mode, Peer, Offset) of
        {hit, _Intervals} -> false;
        _MissingOrStale -> true
    end.

%% @doc Return non-empty cached peer ranges without scheduling discovery work.
%% `ok' means every relevant peer/mode has a cached row; successful empty and
%% stale rows count as cached, while a missing row returns `cache_miss'.
cached_peer_ranges(StoreID, Peers, Offset, RangeStart, RangeEnd) ->
    {PeerRanges, AnyCacheMiss} = lists:foldl(
        fun(Peer, Acc) ->
            cached_peer_ranges_for_peer(StoreID, Peer, Offset,
                RangeStart, RangeEnd, Acc)
        end,
        {[], false},
        Peers),
    CacheStatus = case AnyCacheMiss of
        true -> cache_miss;
        false -> ok
    end,
    {lists:reverse(PeerRanges), CacheStatus}.

cached_peer_ranges_for_peer(StoreID, Peer, Offset, RangeStart, RangeEnd,
        Acc) ->
    BasePeerRange = #peer_range{
        store_id = StoreID,
        offset = Offset,
        peer = Peer,
        intervals = ar_intervals:new()
    },
    Acc2 = cached_peer_range(byte, BasePeerRange, RangeStart, RangeEnd, Acc),
    cached_peer_range(
        footprint, BasePeerRange, RangeStart, RangeEnd, Acc2).

cached_peer_range(Mode, BasePeerRange, RangeStart, RangeEnd,
        {PeerRanges, AnyCacheMiss}) ->
    #peer_range{ peer = Peer, offset = Offset, store_id = StoreID } =
        BasePeerRange,
    maybe
        true ?= peer_supports(Mode, Peer),
        true ?= peer_has_sync_bucket(Mode, Peer, Offset),
        {_Freshness, Intervals} ?= get_chunk_intervals(
            Mode, Peer, Offset, RangeStart, RangeEnd),
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
    #footprint{ store_id = StoreID, partition = Partition,
        footprint = Footprint }.

%% @doc Return a peer's cached availability as byte intervals. Byte metadata
%% is clipped to the requested range; footprint metadata is converted from
%% footprint-record space.
get_chunk_intervals(_Mode, _Peer, _Offset, RangeStart, RangeEnd)
        when RangeStart >= RangeEnd ->
    {ok, ar_intervals:new()};
get_chunk_intervals(Mode, Peer, Offset, RangeStart, RangeEnd) ->
    case chunk_interval_lookup(Mode, Peer, Offset) of
        miss ->
            cache_miss;
        {Freshness, Intervals} ->
            ByteIntervals = chunk_intervals_to_byte_intervals(
                Mode, Intervals, RangeStart, RangeEnd),
            case Freshness of
                hit -> {ok, ByteIntervals};
                stale -> {stale, ByteIntervals}
            end
    end.

chunk_intervals_to_byte_intervals(byte, Intervals, RangeStart, RangeEnd) ->
    ByteRange = ar_intervals:from_list([{RangeEnd, RangeStart}]),
    ar_intervals:intersection(Intervals, ByteRange);
chunk_intervals_to_byte_intervals(footprint, Intervals, _RangeStart, _RangeEnd) ->
    ar_footprint_record:footprint_intervals_to_byte_intervals(Intervals).

interval_location(byte, Offset) ->
    Step = ar_sync_cursor:query_range_step_size(),
    (Offset div Step) * Step;
interval_location(footprint, Offset) ->
    ar_footprint_record:get_location(Offset + ?DATA_CHUNK_SIZE).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    %% ar_sync_discovery* tables are created in ar_data_sync_sup so the
    %% gen_server crashing or restarting doesn't wipe the cache.
    ar_timer:send_after(
        ?DATA_DISCOVERY_COLLECT_PEERS_FREQUENCY_MS, self(), collect_peers),
    schedule_sync_bucket_refresh(),
    %% Periodic maintenance updates cache state and emits discovery metrics.
    %% It fires inside the gen_server so it has direct access to State.
    ar_timer:send_after(?MAINTENANCE_INTERVAL_MS, self(), maintenance),
    [ok, ok] = ar_events:subscribe([peer, node_state]),
    {ok, #state{}}.

handle_call(inflight_count, _From, State) ->
    %% Expose asynchronous job count so a caller can determine quiescence.
    #state{ jobs = JobsByKind } = State,
    NumInflight = maps:fold(
        fun(_Kind, Jobs, Acc) ->
            Acc + inflight_count(Jobs)
        end,
        0,
        JobsByKind),
    {reply, NumInflight, State};
handle_call(Request, _From, State) ->
    ?LOG_WARNING([{event, unhandled_call}, {request, Request}]),
    {reply, ok, State}.

handle_cast({add_peers, Peers}, State) ->
    State2 = lists:foldl(fun add_peer/2, State, Peers),
    {noreply, start_jobs(sync_bucket, State2)};

handle_cast({refresh_chunk_intervals, ChunkIntervalJob}, State) ->
    State2 = enqueue_job(ChunkIntervalJob, State),
    {noreply, start_jobs(chunk_interval, State2)};

handle_cast({job_result, Peer, Result}, State) ->
    State2 = case is_peer_tracked(Peer, State) of
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
    ar_timer:send_after(
        ?DATA_DISCOVERY_COLLECT_PEERS_FREQUENCY_MS, self(), collect_peers),
    collect_peers(),
    {noreply, State};

handle_info(refresh_sync_buckets, State) ->
    schedule_sync_bucket_refresh(),
    State2 = enqueue_sync_bucket_jobs(State),
    {noreply, start_jobs(sync_bucket, State2)};

handle_info(maintenance, State0) ->
    ar_timer:send_after(?MAINTENANCE_INTERVAL_MS, self(), maintenance),
    State = start_jobs(chunk_interval, run_maintenance(State0)),
    {noreply, start_jobs(sync_bucket, State)};

handle_info(Message, State) ->
    ?LOG_WARNING([{event, unhandled_info}, {message, Message}]),
    {noreply, State}.

terminate(Reason, State) ->
    #state{ jobs = JobsByKind } = State,
    terminate_jobs(JobsByKind),
    ?LOG_INFO([{pid, self()},{callback, terminate},{reason, Reason}]),
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
    case ar_sync_deps:is_joined() of
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
                        ar_sync_deps:get_peers(current)
                end,
            gen_server:cast(?MODULE,
                {add_peers, lists:sublist(Peers, ?MAX_DISCOVERY_PEERS)})
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
            JobsByKind)
    },
    NumSyncBuckets = delete_rows(?SYNC_BUCKET_CACHE_TABLE, Peer),
    NumChunkIntervals = delete_rows(?CHUNK_INTERVAL_CACHE_TABLE, Peer),
    record_chunk_interval_evictions(peer_removed, NumChunkIntervals),
    ?LOG_DEBUG([{event, peer_removed_from_discovery},
            {peer, arweave_util:format_peer(Peer)},
            {reason, Reason},
            {had_sync_buckets, NumSyncBuckets > 0},
            {remaining_buckets_rows,
                ets:info(?SYNC_BUCKET_CACHE_TABLE, size)}]),
    State3 = start_jobs(chunk_interval, State2),
    {noreply, start_jobs(sync_bucket, State3)}.

is_peer_tracked(Peer, State) ->
    #state{ tracked_peers = TrackedPeers } = State,
    sets:is_element(Peer, TrackedPeers).

peer_supports(byte, _Peer) ->
    true;
peer_supports(footprint, Peer) ->
    ar_sync_deps:get_peer_release(Peer) >= ?GET_FOOTPRINT_SUPPORT_RELEASE.

%%%===================================================================
%%% Generic job functions.
%%%===================================================================

inflight_count(#discovery_jobs{ inflight = Inflight }) ->
    map_size(Inflight).

job_exists(Key, State) ->
    Jobs = jobs_for_key(Key, State),
    #discovery_jobs{
        pending = Pending,
        inflight = Inflight
    } = Jobs,
    PendingMatch = case maps:get(Key, Pending, undefined) of
        #discovery_job{ key = Key } -> true;
        _ -> false
    end,
    PendingMatch orelse maps:is_key(Key, Inflight).

jobs_for_key(Key, #state{ jobs = JobsByKind }) ->
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
    #discovery_job{ key = Key, kind = Kind, peer = Peer } = Job,
    #state{ jobs = JobsByKind } = State,
    Jobs = maps:get(Kind, JobsByKind),
    #discovery_jobs{ pending = Pending } = Jobs,
    Jobs2 = case {
        is_peer_tracked(Peer, State),
        job_exists(Key, State),
        has_pending_capacity(Job, Jobs)
    } of
        {false, _, _} -> %% Untracked peer.
            Jobs;
        {true, true, _} -> %% Refresh pending demand without duplicating it.
            refresh_pending_job(Job, Jobs);
        {true, false, false} -> %% Global and per-store allowances are full.
            Jobs;
        {true, false, true} -> %% Enqueue new job.
            Jobs#discovery_jobs{
                pending = maps:put(Key, Job, Pending)
            }
    end,
    State#state{ jobs = JobsByKind#{Kind := Jobs2} }.

refresh_pending_job(#discovery_job{ key = Key } = Job,
        #discovery_jobs{ pending = Pending } = Jobs) ->
    case maps:is_key(Key, Pending) of
        true ->
            RefreshedJob = Job#discovery_job{
                requested_at = ar_timer:monotonic_ms()
            },
            Jobs#discovery_jobs{
                pending = maps:put(Key, RefreshedJob, Pending)
            };
        false -> Jobs
    end.

has_pending_capacity(
        #discovery_job{ kind = chunk_interval, store_id = StoreID },
        #discovery_jobs{
        pending = Pending,
        max_pending = MaxPending
    }) ->
    map_size(Pending) < MaxPending orelse
        pending_job_count(StoreID, Pending)
            < ?MIN_PENDING_CHUNK_INTERVAL_JOBS_PER_STORE;
has_pending_capacity(_Job, #discovery_jobs{
        pending = Pending,
        max_pending = MaxPending
    }) ->
    map_size(Pending) < MaxPending.

pending_job_count(StoreID, Pending) ->
    maps:fold(
        fun(_Key, #discovery_job{ store_id = PendingStoreID }, Count)
                when PendingStoreID =:= StoreID ->
            Count + 1;
            (_Key, _Job, Count) ->
            Count
        end,
        0,
        Pending).

job_counts_by_store(Jobs) ->
    maps:fold(
        fun(_Key, #discovery_job{ store_id = StoreID }, Counts) ->
            arweave_util:increment_map_value(StoreID, Counts)
        end,
        #{},
        Jobs).

%% @doc Spawn Job and track its process PID on the inflight job.
start_job(Job, Jobs) ->
    #discovery_job{ key = Key } = Job,
    #discovery_jobs{ inflight = Inflight } = Jobs,
    {Pid, _Ref} = spawn_monitor(fun() -> run_job(Job) end),
    Jobs#discovery_jobs{
        inflight = maps:put(Key, Job#discovery_job{ pid = Pid }, Inflight)
    }.

run_job(#discovery_job{ kind = sync_bucket, peer = Peer }) ->
    fetch_sync_buckets(Peer);
run_job(#discovery_job{ kind = chunk_interval } = Job) ->
    refresh_chunk_intervals(Job).

pid_to_job(Pid, Inflight) ->
    case lists:search(
            fun({_Key, #discovery_job{ pid = JobPid }}) ->
                JobPid =:= Pid
            end,
            maps:to_list(Inflight)) of
        {value, {Key, _Job}} -> {ok, Key};
        false -> error
    end.

finish_job(Pid, State) ->
    #state{ jobs = JobsByKind } = State,
    Result = maps:fold(
        fun(_Kind, _Jobs, {ok, _, _} = Acc) ->
                Acc;
            (Kind, Jobs, error) ->
                #discovery_jobs{ inflight = Inflight } = Jobs,
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
        JobsByKind),
    case Result of
        {ok, Kind, Jobs2} ->
            Jobs3 = start_jobs(Jobs2),
            State#state{ jobs = JobsByKind#{Kind := Jobs3} };
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
            fun(_Key, #discovery_job{ peer = JobPeer }) ->
                JobPeer =/= Peer
            end,
            Pending),
        inflight = maps:filter(
            fun(_Key, #discovery_job{
                    peer = JobPeer,
                    pid = JobPID
                }) when JobPeer =:= Peer ->
                    terminate_job(JobPID),
                    false;
                (_Key, _Job) ->
                    true
            end,
            Inflight)
    }.

%% @doc Start pending jobs while preferring idle peers and stores.
start_jobs(Kind, State) ->
    #state{ jobs = JobsByKind } = State,
    Jobs = maps:get(Kind, JobsByKind),
    Jobs2 = start_jobs(Jobs),
    State#state{ jobs = JobsByKind#{Kind := Jobs2} }.

start_jobs(Jobs) ->
    case has_capacity(Jobs) of
        false ->
            %% Do not inspect pending jobs at the concurrency limit.
            Jobs;
        true ->
            {PeerLoad, StoreLoad} = compute_job_load(Jobs),
            PeerStoreModeCounts = inflight_peer_store_mode_counts(Jobs),
            start_pending_jobs(
                Jobs, PeerLoad, StoreLoad, PeerStoreModeCounts)
    end.

start_pending_jobs(Jobs, PeerLoad, StoreLoad, PeerStoreModeCounts) ->
    case has_capacity(Jobs) of
        true ->
            case take_next_job(
                    Jobs, PeerLoad, StoreLoad, PeerStoreModeCounts) of
                {ok, #discovery_job{
                        peer = Peer,
                        store_id = StoreID
                    } = Job, Jobs2} ->
                    Jobs3 = start_job(Job, Jobs2),
                    PeerStoreMode = peer_store_mode_key(
                        Job#discovery_job.key),
                    start_pending_jobs(Jobs3,
                        arweave_util:increment_map_value(Peer, PeerLoad),
                        arweave_util:increment_map_value(StoreID, StoreLoad),
                        arweave_util:increment_map_value(
                            PeerStoreMode, PeerStoreModeCounts));
                none ->
                    Jobs
            end;
        false ->
            Jobs
    end.

inflight_peer_store_mode_counts(#discovery_jobs{ inflight = Inflight }) ->
    maps:fold(
        fun(Key, _Job, Acc) ->
            arweave_util:increment_map_value(peer_store_mode_key(Key), Acc)
        end,
        #{},
        Inflight).

has_capacity(#discovery_jobs{ max_inflight = infinity }) ->
    true;
has_capacity(#discovery_jobs{ max_inflight = MaxInflight } = Jobs) ->
    inflight_count(Jobs) < MaxInflight.

compute_job_load(#discovery_jobs{ inflight = Inflight }) ->
    maps:fold(
        fun(_Key, Job, {PeerLoad, StoreLoad}) ->
            #discovery_job{
                peer = Peer,
                store_id = StoreID
            } = Job,
            {arweave_util:increment_map_value(Peer, PeerLoad),
                arweave_util:increment_map_value(StoreID, StoreLoad)}
        end,
        {#{}, #{}},
        Inflight).

take_next_job(Jobs, PeerLoad, StoreLoad, PeerStoreModeCounts) ->
    #discovery_jobs{ pending = Pending } = Jobs,
    RunnableByLane = maps:fold(
        fun(PendingKey, Job, Acc) ->
            Lane = peer_store_mode_key(PendingKey),
            case maps:get(Lane, PeerStoreModeCounts, 0)
                    < ?MAX_CHUNK_INTERVAL_JOBS_PER_PEER_STORE_MODE of
                true ->
                    Candidate = {PendingKey, Job},
                    maps:update_with(Lane,
                        fun(Existing) ->
                            preferred_pending_job(Candidate, Existing)
                        end,
                        Candidate,
                        Acc);
                false ->
                    Acc
            end
        end,
        #{},
        Pending),
    case maps:values(RunnableByLane) of
        [] ->
            none;
        PendingJobs ->
            %% Tuple ordering prefers less-loaded peers, then less-loaded stores.
            %% Job keys provide deterministic ordering when both loads are equal.
            RankedJobs = lists:map(
                fun({Key, Job}) ->
                    {job_load(Job, PeerLoad, StoreLoad), Key, Job}
                end,
                PendingJobs),
            {_Load, SelectedKey, Selected} = lists:min(RankedJobs),
            {ok, Selected, Jobs#discovery_jobs{
                pending = maps:remove(SelectedKey, Pending)
            }}
    end.

preferred_pending_job(
        {Key1, Job1} = Candidate1,
        {Key2, Job2} = Candidate2) ->
    case pending_job_priority(Job1, Key1)
            >= pending_job_priority(Job2, Key2) of
        true -> Candidate1;
        false -> Candidate2
    end.

pending_job_priority(#discovery_job{
        kind = chunk_interval,
        requested_at = undefined,
        start = Start
    }, _Key) ->
    {0, 0, -Start};
pending_job_priority(#discovery_job{
        kind = chunk_interval,
        requested_at = RequestedAt,
        start = Start
    }, _Key) ->
    {1, RequestedAt, -Start};
pending_job_priority(_Job, Key) ->
    {0, 0, Key}.

job_load(Job, PeerLoad, StoreLoad) ->
    #discovery_job{ peer = Peer, store_id = StoreID } = Job,
    {maps:get(Peer, PeerLoad, 0), maps:get(StoreID, StoreLoad, 0)}.

terminate_jobs(JobsByKind) ->
    maps:foreach(
        fun(_Kind, #discovery_jobs{ inflight = Inflight }) ->
            maps:foreach(
                fun(_Key, #discovery_job{ pid = JobPID }) ->
                    terminate_job(JobPID)
                end,
                Inflight)
        end,
        JobsByKind).

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
    #state{ tracked_peers = TrackedPeers } = State,
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
enqueue_sync_bucket_jobs(#state{ tracked_peers = TrackedPeers } = State) ->
    sets:fold(
        fun(Peer, Acc) -> enqueue_job(sync_bucket_job(Peer), Acc) end,
        State,
        TrackedPeers).

%% @doc Schedule the next node-wide refresh with +/-25% jitter so nodes that
%% start together do not repeatedly query peers at the same time.
schedule_sync_bucket_refresh() ->
    {ok, _} = ar_timer:send_after(
        sync_bucket_refresh_delay_ms(), self(), refresh_sync_buckets),
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
            {ok, SyncBuckets} ?= ar_sync_deps:get_sync_buckets(Peer, Mode),
            gen_server:cast(?MODULE,
                {job_result, Peer, {sync_buckets, Mode, SyncBuckets}})
        else
            false ->
                ok;
            {error, request_type_not_found} ->
                ?LOG_DEBUG([{event, sync_buckets_request_type_not_found},
                    {peer, arweave_util:format_peer(Peer)}, {mode, Mode}]),
                error;
            {error, RequestError} ->
                ar_http_iface_client:log_failed_request(RequestError,
                    [{event, failed_to_fetch_sync_buckets},
                    {peer, arweave_util:format_peer(Peer)},
                    {mode, Mode},
                    {reason, io_lib:format("~p", [RequestError])}]),
                error;
            Other ->
                ?LOG_DEBUG([{event, failed_to_fetch_sync_buckets},
                    {peer, arweave_util:format_peer(Peer)},
                    {mode, Mode},
                    {reason, io_lib:format("~p", [Other])}]),
                error
        end
    catch Class:Reason:Stacktrace ->
        ?LOG_WARNING([{event, refresh_sync_buckets_failed},
            {peer, arweave_util:format_peer(Peer)},
            {mode, Mode},
            {class, Class},
            {reason, io_lib:format("~p", [Reason])},
            {stacktrace, Stacktrace}])
    end,
    ok.

%%%===================================================================
%%% Cache manipulation.
%%%===================================================================

record_job_result(Peer, {sync_buckets, Mode, SyncBuckets}, State) ->
    store_sync_buckets(Mode, Peer, SyncBuckets),
    ?LOG_DEBUG([{event, processed_sync_buckets},
        {peer, arweave_util:format_peer(Peer)}, {mode, Mode}]),
    State;
record_job_result(Peer,
        {chunk_intervals, _StoreID, Offset, Mode, {ok, Intervals}}, State) ->
    store_row(chunk_interval, Mode, Offset, Peer, Intervals),
    State;
record_job_result(Peer,
        {chunk_intervals, _StoreID, Offset, Mode, _Error}, State) ->
    %% Do not continue serving stale metadata when its replacement cannot be
    %% fetched. A later sweep will request it again.
    delete_row(chunk_interval, Mode, Offset, Peer),
    State.

row_key(sync_bucket, Mode, SyncBucket, Peer) ->
    {Mode, SyncBucket, Peer};
row_key(chunk_interval, Mode, Offset, Peer) ->
    {Mode, interval_location(Mode, Offset), Peer}.

store_row(Kind, Mode, Location, Peer, Value) ->
    ets:insert(table(Kind),
        {row_key(Kind, Mode, Location, Peer),
            Value, ar_timer:monotonic_ms()}),
    ok.

delete_row(Kind, Mode, Location, Peer) ->
    ets:delete(table(Kind), row_key(Kind, Mode, Location, Peer)).

table(sync_bucket) ->
    ?SYNC_BUCKET_CACHE_TABLE;
table(chunk_interval) ->
    ?CHUNK_INTERVAL_CACHE_TABLE.

%% @doc Delete all cache rows for Peer from a discovery table.
delete_rows(Table, Peer) ->
    ets:select_delete(Table,
        [{{{'_', '_', Peer}, '_', '_'}, [], [true]}]).

expiration_cutoff(CacheTTLMs) ->
    ar_timer:monotonic_ms() - CacheTTLMs.

%%%===================================================================
%%% Sync bucket cache.
%%%===================================================================

store_sync_buckets(Mode, Peer, SyncBuckets) ->
    ar_sync_buckets:foreach(
        fun(SyncBucket, Share) ->
            mark_chunk_intervals_stale_on_share_change(
                Mode, Peer, SyncBucket, Share),
            store_row(sync_bucket, Mode, SyncBucket, Peer, Share)
        end,
        sync_bucket_size(Mode),
        sync_bucket_range_end(Mode, ar_sync_deps:get_weave_size()),
        SyncBuckets
    ),
    ok.

sync_bucket_size(byte) ->
    ar_sync_buckets:get_network_data_bucket_size();
sync_bucket_size(footprint) ->
    ar_sync_buckets:get_network_footprint_bucket_size().

sync_bucket_range_end(byte, WeaveSize) ->
    WeaveSize;
sync_bucket_range_end(footprint, WeaveSize) ->
    ar_footprint_record:max_offset(WeaveSize).

%% @doc Mark cached chunk intervals stale when a refreshed sync bucket's share
%% differs from the cached share.
mark_chunk_intervals_stale_on_share_change(
        Mode, Peer, SyncBucket, NewShare) ->
    Key = row_key(sync_bucket, Mode, SyncBucket, Peer),
    case ets:lookup(?SYNC_BUCKET_CACHE_TABLE, Key) of
        [{_, NewShare, _}] ->
            ok;
        [] ->
            ok;
        [{_, _ChangedShare, _}] ->
            mark_chunk_intervals_stale(Mode, Peer, SyncBucket)
    end.

%% @doc The inclusive chunk-interval location range a coarse bucket covers. Byte
%% rows are keyed by byte offset; footprint rows by {Partition, Footprint},
%% whose lexicographic order matches the partition-major, footprint-major
%% linear layout (ar_footprint_record:get_offset/1), so a bucket's rows are
%% contiguous in both modes. Footprint buckets don't align exactly to
%% footprint boundaries (the +1 linear-offset convention lets an edge
%% footprint straddle), so the range is a one-footprint superset at each
%% end. Extra rows are refreshed or removed with the bucket.
chunk_interval_location_range(byte, SyncBucket) ->
    Lo = SyncBucket * ?NETWORK_DATA_BUCKET_SIZE,
    {Lo, Lo + ?NETWORK_DATA_BUCKET_SIZE - 1};
chunk_interval_location_range(footprint, SyncBucket) ->
    FootprintSize = ar_replica_2_9:get_footprint_size(),
    FootprintsPerPartition = ar_replica_2_9:get_footprints_per_partition(),
    LoGlobal = (SyncBucket * ?NETWORK_FOOTPRINT_BUCKET_SIZE) div FootprintSize,
    HiGlobal = ((SyncBucket + 1) * ?NETWORK_FOOTPRINT_BUCKET_SIZE) div FootprintSize,
    {{LoGlobal div FootprintsPerPartition, LoGlobal rem FootprintsPerPartition},
        {HiGlobal div FootprintsPerPartition, HiGlobal rem FootprintsPerPartition}}.

%% @doc Retire sync bucket rows whose stamp was not refreshed within
%% ?SYNC_BUCKET_CACHE_TTL_MS (withdrawn advertisements and silently dead
%% peers), dropping its cached chunk intervals with it.
%% Runs from periodic maintenance.
delete_expired_sync_buckets() ->
    Cutoff = expiration_cutoff(?SYNC_BUCKET_CACHE_TTL_MS),
    Expired = ets:select(?SYNC_BUCKET_CACHE_TABLE,
            [{ {'$1', '_', '$2'}, [{'<', '$2', Cutoff}], ['$1'] }]),
    lists:foreach(
        fun({Mode, SyncBucket, Peer}) ->
            delete_row(sync_bucket, Mode, SyncBucket, Peer),
            delete_chunk_intervals(Mode, Peer, SyncBucket)
        end, Expired),
    case Expired of
        [] -> ok;
        _ -> ?LOG_DEBUG([{event, deleted_expired_sync_buckets},
                {count, length(Expired)}])
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
    Start = case Mode of
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
            gen_server:cast(?MODULE,
                {job_result, Peer,
                    {chunk_intervals, StoreID, Start, Mode, Result}})
        else
            false ->
                ok
        end
    catch Class:Reason:Stacktrace ->
        ?LOG_WARNING([{event, chunk_interval_job_crashed}, {class, Class},
                {peer, arweave_util:format_peer(Peer)}, {mode, Mode},
                {reason, io_lib:format("~p", [Reason])}, {stacktrace, Stacktrace}])
    end.

%%%===================================================================
%%% Chunk interval cache.
%%%===================================================================

mark_chunk_intervals_stale(Mode, Peer, SyncBucket) ->
    StaleTimestamp =
        expiration_cutoff(?CHUNK_INTERVAL_CACHE_TTL_MS) - 1,
    lists:foreach(
        fun(Location) ->
            ets:update_element(?CHUNK_INTERVAL_CACHE_TABLE,
                {Mode, Location, Peer},
                {3, StaleTimestamp})
        end,
        chunk_interval_locations(Mode, Peer, SyncBucket)).

delete_chunk_intervals(Mode, Peer, SyncBucket) ->
    Locations = chunk_interval_locations(Mode, Peer, SyncBucket),
    lists:foreach(
        fun(Location) ->
            ets:delete(?CHUNK_INTERVAL_CACHE_TABLE,
                {Mode, Location, Peer})
        end,
        Locations),
    record_chunk_interval_evictions(withdrawn, length(Locations)).

chunk_interval_locations(Mode, Peer, SyncBucket) ->
    {Lo, Hi} = chunk_interval_location_range(Mode, SyncBucket),
    ets:select(?CHUNK_INTERVAL_CACHE_TABLE,
        [{ {{Mode, '$1', Peer}, '_', '_'},
            [{'>=', '$1', {const, Lo}}, {'=<', '$1', {const, Hi}}],
            ['$1'] }]).

%% @doc Look up a cached interval row, age-gated by the shared safety-floor
%% TTL. A row past it comes back as {stale, ...}: callers still use the
%% intervals (an aged row beats stalling the pipeline on a miss) but treat the
%% row as needing a chunk interval job. refresh_chunk_intervals/1 uses this
%% same freshness result for both modes, so a stale row is replaced rather than
%% reused. Primary freshness is share-diffing; this TTL is the safety floor.
chunk_interval_lookup(Mode, Peer, Offset) ->
    Key = row_key(chunk_interval, Mode, Offset, Peer),
    case ets:lookup(?CHUNK_INTERVAL_CACHE_TABLE, Key) of
        [{_, Intervals, MonotonicMs}] ->
            Cutoff = expiration_cutoff(?CHUNK_INTERVAL_CACHE_TTL_MS),
            case MonotonicMs < Cutoff of
                true -> {stale, Intervals};
                false -> {hit, Intervals}
            end;
        [] -> miss
    end.

%% Trim runs from periodic maintenance rather than after every stored result.
%% A burst of cache-result messages could otherwise trigger repeated 10%
%% deletions while ETS memory accounting still reflects rows already removed.
%% The maintenance cadence gives ETS accounting time to settle between checks.
trim_chunk_interval_cache() ->
    Bytes = ets:info(?CHUNK_INTERVAL_CACHE_TABLE, memory)
            * erlang:system_info(wordsize),
    MaxBytes = ar_data_sync:interval_cache_size_limit(),
    case Bytes > MaxBytes of
        false ->
            ok;
        true ->
            Size = ets:info(?CHUNK_INTERVAL_CACHE_TABLE, size),
            ToDelete = max(1, Size div ?CHUNK_INTERVAL_CACHE_TRIM_DIVISOR),
            Timestamps = ets:select(?CHUNK_INTERVAL_CACHE_TABLE,
                    [{ {'_', '_', '$1'}, [], ['$1'] }]),
            Sorted = lists:sort(Timestamps),
            Threshold = lists:nth(min(ToDelete, length(Sorted)), Sorted),
            Deleted = ets:select_delete(?CHUNK_INTERVAL_CACHE_TABLE,
                    [{ {'_', '_', '$1'},
                            [{'=<', '$1', Threshold}], [true] }]),
            record_chunk_interval_evictions(trim, Deleted),
            MbAfter = ets:info(?CHUNK_INTERVAL_CACHE_TABLE, memory)
                    * erlang:system_info(wordsize) div ?MiB,
            ?LOG_DEBUG([{event, chunk_interval_cache_trimmed},
                    {rows_before, Size},
                    {rows_deleted, Deleted},
                    {rows_after, Size - Deleted},
                    {mb_before, Bytes div ?MiB},
                    {mb_after, MbAfter},
                    {cap_mb, MaxBytes div ?MiB}])
    end.

%%%===================================================================
%%% Chunk interval HTTP requests.
%%%===================================================================

fetch_chunk_intervals(byte, Peer, Left) ->
    Right = Left + ar_sync_cursor:query_range_step_size(),
    %% Older peers ignore the right bound and return an open-ended range, which
    %% is cut to Right after pagination.
    Bound =
        case ar_sync_deps:get_peer_release(Peer) >=
                ?GET_SYNC_RECORD_RIGHT_BOUND_SUPPORT_RELEASE of
            true -> Right;
            false -> none
        end,
    fetch_chunk_interval_pages(Peer, Bound, Left, Right,
        ?MAX_CHUNK_INTERVAL_PAGES, ar_intervals:new());
fetch_chunk_intervals(footprint, Peer, {Partition, Footprint}) ->
    case ar_sync_deps:fetch_chunk_intervals(
            Peer, {footprint, Partition, Footprint}) of
        not_found ->
            {ok, ar_intervals:new()};
        Reply ->
            Reply
    end.

fetch_chunk_interval_pages(
        _Peer, _Bound, _Left, _Right, 0, _Intervals) ->
    {error, interval_page_limit};
fetch_chunk_interval_pages(
        Peer, Bound, Left, Right, PagesLeft, Intervals) ->
    case ar_sync_deps:fetch_chunk_intervals(Peer,
            {byte, Left + 1, Bound, ?QUERY_SYNC_INTERVALS_COUNT_LIMIT}) of
        {ok, Page} ->
            case collect_chunk_interval_page(Left, Right, Intervals, Page) of
                {continue, PageEnd, Intervals2} ->
                    fetch_chunk_interval_pages(Peer, Bound, PageEnd,
                        Right, PagesLeft - 1, Intervals2);
                Result ->
                    Result
            end;
        Error ->
            Error
    end.

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
            case PageEnd >= Right
                    orelse Count < ?QUERY_SYNC_INTERVALS_COUNT_LIMIT of
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
    arweave_metrics:gauge_set(discovery_peers_scanned,
        sets:size(TrackedPeers)),
    arweave_metrics:gauge_set(chunk_interval_cache_size, [rows],
        ets:info(?CHUNK_INTERVAL_CACHE_TABLE, size)),
    arweave_metrics:gauge_set(chunk_interval_cache_size, [bytes],
        ets:info(?CHUNK_INTERVAL_CACHE_TABLE, memory)
            * erlang:system_info(wordsize)),
    maps:foreach(
        fun(Kind, Jobs) ->
            #discovery_jobs{
                pending = Pending,
                inflight = Inflight,
                max_inflight = MaxInflight
            } = Jobs,
            arweave_metrics:gauge_set(
                sync_discovery_jobs, [Kind, pending], map_size(Pending)),
            arweave_metrics:gauge_set(
                sync_discovery_jobs, [Kind, inflight], map_size(Inflight)),
            case MaxInflight of
                infinity -> ok;
                _ ->
                    arweave_metrics:gauge_set(
                        sync_discovery_jobs, [Kind, max_inflight], MaxInflight)
            end
        end,
        JobsByKind),
    emit_chunk_interval_job_metrics(JobsByKind),
    emit_sync_discovery_peers().

emit_chunk_interval_job_metrics(JobsByKind) ->
    #discovery_jobs{ pending = Pending, inflight = Inflight } =
        maps:get(chunk_interval, JobsByKind),
    PendingCounts = job_counts_by_store(Pending),
    InflightCounts = job_counts_by_store(Inflight),
    lists:foreach(
        fun(StoreID) ->
            Label = ar_storage_module:label(StoreID),
            arweave_metrics:gauge_set(sync_chunk_interval_jobs_by_store,
                [pending, Label], maps:get(StoreID, PendingCounts, 0)),
            arweave_metrics:gauge_set(sync_chunk_interval_jobs_by_store,
                [inflight, Label], maps:get(StoreID, InflightCounts, 0))
        end,
        ar_sync_store_sweeper:store_ids()).

record_chunk_interval_evictions(_Reason, 0) ->
    ok;
record_chunk_interval_evictions(Reason, Count) ->
    arweave_metrics:counter_inc(chunk_interval_cache_evictions, [Reason], Count).

emit_sync_discovery_peers() ->
    Modes = [byte, footprint],
    StorePeerSets = lists:flatmap(
        fun(Module) -> get_peers_for_store(Module, Modes) end,
        arweave_config:storage_modules()),
    lists:foreach(
        fun({Mode, StoreID, Peers}) ->
            arweave_metrics:gauge_set(sync_discovery_peers,
                [Mode, ar_storage_module:label(StoreID)], sets:size(Peers))
        end,
        StorePeerSets),
    ModePeerSets = lists:map(
        fun(Mode) ->
            {Mode, get_peers_for_mode(Mode, StorePeerSets)}
        end,
        Modes),
    lists:foreach(
        fun({Mode, Peers}) ->
            arweave_metrics:gauge_set(
                sync_discovery_peers, [Mode, "all"], sets:size(Peers))
        end,
        ModePeerSets),
    AllPeers = lists:foldl(
        fun({_Mode, Peers}, Acc) -> sets:union(Acc, Peers) end,
        sets:new(),
        ModePeerSets),
    arweave_metrics:gauge_set(
        sync_discovery_peers, [union, "all"], sets:size(AllPeers)).

get_peers_for_mode(Mode, StorePeerSets) ->
    lists:foldl(
        fun({PeerMode, _StoreID, Peers}, Acc) ->
            case PeerMode =:= Mode of
                true -> sets:union(Acc, Peers);
                false -> Acc
            end
        end,
        sets:new(),
        StorePeerSets).

get_peers_for_store(Module, Modes) ->
    StoreID = ar_storage_module:id(Module),
    {RangeStart, RangeEnd} = ar_storage_module:module_range(Module),
    [{Mode, StoreID, get_peers_for_range(Mode, RangeStart, RangeEnd)}
        || Mode <- Modes].

get_peers_for_range(Mode, RangeStart, RangeEnd) ->
    {StartSyncBucket, EndSyncBucket} =
        sync_bucket_range(Mode, RangeStart, RangeEnd),
    get_peers_for_sync_bucket_range(
        Mode, StartSyncBucket, EndSyncBucket, sets:new()).

sync_bucket_range(byte, RangeStart, RangeEnd) ->
    {sync_bucket(byte, RangeStart), sync_bucket(byte, RangeEnd - 1)};
sync_bucket_range(footprint, RangeStart, RangeEnd) ->
    {sync_bucket(footprint, RangeStart),
        sync_bucket(footprint, RangeEnd - ?DATA_CHUNK_SIZE)}.

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

interval_page(First, Count) ->
    ar_intervals:from_list([
        {2 * Index, 2 * Index - 1}
        || Index <- lists:seq(First, First + Count - 1)
    ]).

with_mocked_chunk_interval_pages(Peer, Release, PageFun, TestFun) ->
    Right = ar_sync_cursor:query_range_step_size(),
    FetchMock =
        case Release >= ?GET_SYNC_RECORD_RIGHT_BOUND_SUPPORT_RELEASE of
            true ->
                {ar_http_iface_client, get_sync_record,
                    fun(P, Start, QueryRight, Limit) when P =:= Peer,
                            QueryRight =:= Right,
                            Limit =:= ?QUERY_SYNC_INTERVALS_COUNT_LIMIT ->
                        PageFun(Start)
                    end};
            false ->
                {ar_http_iface_client, get_sync_record,
                    fun(P, Start, Limit) when P =:= Peer,
                            Limit =:= ?QUERY_SYNC_INTERVALS_COUNT_LIMIT ->
                        PageFun(Start)
                    end}
        end,
    ar_test_util:with_mocked([
        {ar_peers, get_peer_release,
            fun(P) when P =:= Peer -> Release end},
        FetchMock
    ], TestFun, 30).

chunk_interval_job_for_test(Peer, StoreID, Start) ->
    AlignedStart = interval_location(byte, Start),
    #discovery_job{
        key = {chunk_interval, Peer, StoreID, byte, AlignedStart},
        kind = chunk_interval,
        peer = Peer,
        store_id = StoreID,
        mode = byte,
        start = AlignedStart
    }.

%%%===================================================================
%%% Tests.
%%%===================================================================

enqueue_job_deduplicates_pending_and_inflight_test() ->
    Job = chunk_interval_job_for_test(peer, store, 0),
    TrackedPeers = sets:from_list([Job#discovery_job.peer]),
    State = #state{ tracked_peers = TrackedPeers },
    State2 = enqueue_job(Job, State),
    PendingJobs = maps:get(chunk_interval, State2#state.jobs),
    ?assertEqual(1, map_size(PendingJobs#discovery_jobs.pending)),
    State3 = enqueue_job(Job, State2),
    RefreshedJobs = maps:get(chunk_interval, State3#state.jobs),
    ?assertEqual(1, map_size(RefreshedJobs#discovery_jobs.pending)),
    RefreshedJob = maps:get(Job#discovery_job.key,
        RefreshedJobs#discovery_jobs.pending),
    ?assertNotEqual(undefined, RefreshedJob#discovery_job.requested_at),
    {ok, RefreshedJob, TakenJobs} = take_next_job(
        RefreshedJobs, #{}, #{},
        inflight_peer_store_mode_counts(RefreshedJobs)),
    ?assertEqual(0, map_size(TakenJobs#discovery_jobs.pending)),
    InflightJob = RefreshedJob#discovery_job{ pid = job_pid },
    InflightJobs = TakenJobs#discovery_jobs{
        inflight = #{Job#discovery_job.key => InflightJob}
    },
    State4 = State3#state{ jobs = (State3#state.jobs)#{
        chunk_interval := InflightJobs } },
    ?assertEqual(State4, enqueue_job(Job, State4)).

enqueue_job_rejects_untracked_peer_test() ->
    Job = chunk_interval_job_for_test(peer, store, 0),
    State = #state{},
    ?assertEqual(State, enqueue_job(Job, State)).

enqueue_job_keeps_distinct_pending_locations_test() ->
    Step = ar_sync_cursor:query_range_step_size(),
    Later = chunk_interval_job_for_test(peer, store, Step),
    Earlier = chunk_interval_job_for_test(peer, store, 0),
    Latest = chunk_interval_job_for_test(peer, store, 2 * Step),
    TrackedPeers = sets:from_list([peer]),
    State = #state{ tracked_peers = TrackedPeers },
    JobsByKind = State#state.jobs,
    %% Distinct active readahead locations remain independent while the global
    %% pending ceiling bounds their aggregate queue.
    State2 = State#state{ jobs = JobsByKind#{
        chunk_interval := #discovery_jobs{} } },
    State3 = enqueue_job(Later, State2),
    State4 = enqueue_job(Earlier, State3),
    State5 = enqueue_job(Latest, State4),
    Jobs = maps:get(chunk_interval, State5#state.jobs),
    ?assertEqual(3, map_size(Jobs#discovery_jobs.pending)),
    ?assert(job_exists(Earlier#discovery_job.key, State5)),
    ?assert(job_exists(Later#discovery_job.key, State5)),
    ?assert(job_exists(Latest#discovery_job.key, State5)).

enqueue_job_discards_when_pending_limit_reached_test() ->
    Step = ar_sync_cursor:query_range_step_size(),
    %% Sixty jobs consume the store guarantee. The sixty-first is rejected
    %% because the one-job opportunistic limit was exceeded long ago.
    Existing = [
        chunk_interval_job_for_test({peer, N}, store, N * Step)
        || N <- lists:seq(1,
            ?MIN_PENDING_CHUNK_INTERVAL_JOBS_PER_STORE)
    ],
    Incoming = chunk_interval_job_for_test(peer, store,
        (?MIN_PENDING_CHUNK_INTERVAL_JOBS_PER_STORE + 1) * Step),
    TrackedPeers = sets:from_list(
        [Incoming#discovery_job.peer
            | [Job#discovery_job.peer || Job <- Existing]]),
    State = #state{ tracked_peers = TrackedPeers },
    JobsByKind = State#state.jobs,
    %% A one-job opportunistic ceiling leaves only the store guarantee.
    ChunkIntervalJobs = #discovery_jobs{ max_pending = 1 },
    State2 = State#state{ jobs = JobsByKind#{
        chunk_interval := ChunkIntervalJobs } },
    FullStoreState = lists:foldl(fun enqueue_job/2, State2, Existing),
    ?assertEqual(FullStoreState, enqueue_job(Incoming, FullStoreState)).

enqueue_job_guarantees_late_store_without_eviction_test() ->
    Step = ar_sync_cursor:query_range_step_size(),
    Existing = chunk_interval_job_for_test(peer1, store1, 0),
    %% A late store may add sixty jobs after the one-job global limit is full:
    %% one byte and one footprint request for each of thirty candidate peers.
    Guaranteed = [
        chunk_interval_job_for_test({peer2, N}, store2, N * Step)
        || N <- lists:seq(1,
            ?MIN_PENDING_CHUNK_INTERVAL_JOBS_PER_STORE)
    ],
    Extra = chunk_interval_job_for_test(peer2, store2,
        (?MIN_PENDING_CHUNK_INTERVAL_JOBS_PER_STORE + 1) * Step),
    TrackedPeers = sets:from_list(
        [Existing#discovery_job.peer, Extra#discovery_job.peer
            | [Job#discovery_job.peer || Job <- Guaranteed]]),
    State = #state{ tracked_peers = TrackedPeers },
    JobsByKind = State#state.jobs,
    ChunkIntervalJobs = #discovery_jobs{ max_pending = 1 },
    State2 = State#state{ jobs = JobsByKind#{
        chunk_interval := ChunkIntervalJobs } },
    FullState = enqueue_job(Existing, State2),
    GuaranteedState = lists:foldl(fun enqueue_job/2, FullState, Guaranteed),
    Jobs = maps:get(chunk_interval, GuaranteedState#state.jobs),
    Pending = Jobs#discovery_jobs.pending,
    ?assertEqual(1 + ?MIN_PENDING_CHUNK_INTERVAL_JOBS_PER_STORE,
        map_size(Pending)),
    ?assert(maps:is_key(Existing#discovery_job.key, Pending)),
    ?assert(lists:all(
        fun(Job) -> maps:is_key(Job#discovery_job.key, Pending) end,
        Guaranteed)),
    %% Once both the global limit and store guarantee are full, another job is
    %% dropped without moving any existing work.
    ?assertEqual(GuaranteedState, enqueue_job(Extra, GuaranteedState)).

enqueue_job_uses_global_capacity_after_store_guarantee_test() ->
    Step = ar_sync_cursor:query_range_step_size(),
    %% Sixty jobs consume the store guarantee, but the sixty-first still uses
    %% the final slot under a sixty-one-job opportunistic limit.
    Existing = [
        chunk_interval_job_for_test({peer, N}, store, N * Step)
        || N <- lists:seq(1,
            ?MIN_PENDING_CHUNK_INTERVAL_JOBS_PER_STORE)
    ],
    Incoming = chunk_interval_job_for_test(peer, store,
        (?MIN_PENDING_CHUNK_INTERVAL_JOBS_PER_STORE + 1) * Step),
    TrackedPeers = sets:from_list(
        [Incoming#discovery_job.peer
            | [Job#discovery_job.peer || Job <- Existing]]),
    State = #state{ tracked_peers = TrackedPeers },
    JobsByKind = State#state.jobs,
    ChunkIntervalJobs = #discovery_jobs{
        max_pending = ?MIN_PENDING_CHUNK_INTERVAL_JOBS_PER_STORE + 1
    },
    State2 = State#state{ jobs = JobsByKind#{
        chunk_interval := ChunkIntervalJobs } },
    State3 = lists:foldl(fun enqueue_job/2, State2, Existing),
    State4 = enqueue_job(Incoming, State3),
    Jobs = maps:get(chunk_interval, State3#state.jobs),
    ?assertEqual(?MIN_PENDING_CHUNK_INTERVAL_JOBS_PER_STORE,
        map_size(Jobs#discovery_jobs.pending)),
    Jobs2 = maps:get(chunk_interval, State4#state.jobs),
    ?assertEqual(?MIN_PENDING_CHUNK_INTERVAL_JOBS_PER_STORE + 1,
        map_size(Jobs2#discovery_jobs.pending)),
    ?assert(maps:is_key(Incoming#discovery_job.key,
        Jobs2#discovery_jobs.pending)).

%% @doc Completing a chunk interval job leaves sync bucket job state unchanged.
finish_job_updates_matching_kind_test() ->
    Peer = peer,
    Job = (chunk_interval_job_for_test(Peer, store, 0))#discovery_job{
        pid = self()
    },
    State0 = #state{ tracked_peers = sets:from_list([Peer]) },
    SyncBucketJobs = maps:get(sync_bucket, State0#state.jobs),
    ChunkIntervalJobs = #discovery_jobs{
        inflight = #{Job#discovery_job.key => Job}
    },
    State = State0#state{
        jobs = (State0#state.jobs)#{
            chunk_interval := ChunkIntervalJobs
        }
    },
    State2 = finish_job(self(), State),
    ?assertEqual(
        SyncBucketJobs, maps:get(sync_bucket, State2#state.jobs)),
    ?assertEqual(
        #{},
        (maps:get(chunk_interval, State2#state.jobs))#discovery_jobs.inflight).

add_peer_enqueues_one_sync_bucket_job_test() ->
    Peer = peer,
    State = add_peer(Peer, #state{}),
    Jobs = maps:get(sync_bucket, State#state.jobs),
    ?assert(sets:is_element(Peer, State#state.tracked_peers)),
    ?assertEqual(1, map_size(Jobs#discovery_jobs.pending)),
    ?assert(job_exists({sync_bucket, Peer}, State)),
    %% Re-collecting an already tracked peer must not duplicate its job.
    ?assertEqual(State, add_peer(Peer, State)).

enqueue_sync_bucket_jobs_deduplicates_active_peers_test() ->
    Peer1 = peer1,
    Peer2 = peer2,
    Job1 = (sync_bucket_job(Peer1))#discovery_job{ pid = job_pid },
    State0 = #state{ tracked_peers = sets:from_list([Peer1, Peer2]) },
    State = State0#state{
        jobs = (State0#state.jobs)#{
            sync_bucket := #discovery_jobs{
                inflight = #{Job1#discovery_job.key => Job1}
            }
        }
    },
    State2 = enqueue_sync_bucket_jobs(State),
    Jobs = maps:get(sync_bucket, State2#state.jobs),
    ?assertEqual(#{Job1#discovery_job.key => Job1},
        Jobs#discovery_jobs.inflight),
    ?assertEqual(#{
        {sync_bucket, Peer2} => sync_bucket_job(Peer2)
    }, Jobs#discovery_jobs.pending).

remove_jobs_keeps_other_peers_test() ->
    RemovedPeer = removed_peer,
    KeptPeer = kept_peer,
    RemovedInflightJob = chunk_interval_job_for_test(
        RemovedPeer, inflight_store, 0),
    RemovedPendingJob = chunk_interval_job_for_test(
        RemovedPeer, pending_store, 1),
    KeptPendingJob = chunk_interval_job_for_test(KeptPeer, kept_store, 2),
    RemovedJobPID = spawn(fun() -> receive stop -> ok end end),
    RemovedInflightJob2 =
        RemovedInflightJob#discovery_job{ pid = RemovedJobPID },
    Jobs = #discovery_jobs{
        pending = #{
            RemovedPendingJob#discovery_job.key => RemovedPendingJob,
            KeptPendingJob#discovery_job.key => KeptPendingJob
        },
        inflight = #{
            RemovedInflightJob2#discovery_job.key => RemovedInflightJob2
        }
    },
    Jobs2 = remove_jobs(RemovedPeer, Jobs),
    State0 = #state{},
    State = State0#state{ jobs = (State0#state.jobs)#{
        chunk_interval := Jobs2 } },
    ?assertNot(is_process_alive(RemovedJobPID)),
    ?assertNot(job_exists(RemovedPendingJob#discovery_job.key, State)),
    ?assertNot(job_exists(RemovedInflightJob2#discovery_job.key, State)),
    ?assert(job_exists(KeptPendingJob#discovery_job.key, State)).

removed_peer_job_results_are_ignored_test() ->
    reset_all_caches(),
    Peer = {10, 0, 0, 1, 1984},
    Offset = 0,
    Intervals = ar_intervals:from_list([{?DATA_CHUNK_SIZE, 0}]),
    TrackedState = add_peer(Peer, #state{}),
    _ = handle_cast(
        {job_result, Peer,
            {chunk_intervals, test_store, Offset,
                byte, {ok, Intervals}}},
        TrackedState),
    ?assertEqual({hit, Intervals}, chunk_interval_lookup(byte, Peer, Offset)),
    store_row(sync_bucket, byte, 0, Peer, 1.0),
    {noreply, RemovedState} =
        do_remove_peer(Peer, test, TrackedState),
    ?assertEqual(miss, chunk_interval_lookup(byte, Peer, Offset)),
    ?assertNot(ets:member(?SYNC_BUCKET_CACHE_TABLE,
        row_key(sync_bucket, byte, 0, Peer))),
    _ = handle_cast(
        {job_result, Peer,
            {chunk_intervals, test_store, Offset,
                byte, {ok, Intervals}}},
        RemovedState),
    ?assertEqual(miss, chunk_interval_lookup(byte, Peer, Offset)),
    SyncBuckets = ar_sync_buckets:from_intervals(Intervals),
    _ = handle_cast(
        {job_result, Peer, {sync_buckets, byte, SyncBuckets}},
        RemovedState),
    ?assertNot(ets:member(?SYNC_BUCKET_CACHE_TABLE,
        row_key(sync_bucket, byte, 0, Peer))).

%% @doc Selection prefers the least-loaded peer before considering store load.
take_next_job_prefers_less_loaded_peer_test() ->
    Step = ar_sync_cursor:query_range_step_size(),
    Inflight1 = chunk_interval_job_for_test(p1, s1, 0),
    Inflight2 = chunk_interval_job_for_test(p1, s2, Step),
    Inflight3 = chunk_interval_job_for_test(p2, s1, 2 * Step),
    Q1 = chunk_interval_job_for_test(p1, s3, 3 * Step),
    Q2 = chunk_interval_job_for_test(p2, s2, 4 * Step),
    Jobs = #discovery_jobs{
        pending = #{
            Q1#discovery_job.key => Q1,
            Q2#discovery_job.key => Q2
        },
        inflight = #{
            Inflight1#discovery_job.key =>
                Inflight1#discovery_job{ pid = self() },
            Inflight2#discovery_job.key =>
                Inflight2#discovery_job{ pid = self() },
            Inflight3#discovery_job.key =>
                Inflight3#discovery_job{ pid = self() }
        } },
    {PeerLoad, StoreLoad} = compute_job_load(Jobs),
    ?assertEqual(#{p1 => 2, p2 => 1}, PeerLoad),
    ?assertEqual(#{s1 => 2, s2 => 1}, StoreLoad),
    {ok, Selected, _Jobs2} = take_next_job(
        Jobs, PeerLoad, StoreLoad,
        inflight_peer_store_mode_counts(Jobs)),
    ?assertEqual(p2, Selected#discovery_job.peer).

%% @doc A newer frontier waits behind the active request for the same peer,
%% store, and mode while unrelated stores continue using discovery capacity.
take_next_job_limits_peer_store_mode_test() ->
    Step = ar_sync_cursor:query_range_step_size(),
    Inflight = chunk_interval_job_for_test(peer, store1, 0),
    SamePendingKey = chunk_interval_job_for_test(peer, store1, Step),
    OtherStore = chunk_interval_job_for_test(peer, store2, Step),
    Jobs = #discovery_jobs{
        pending = #{
            SamePendingKey#discovery_job.key => SamePendingKey,
            OtherStore#discovery_job.key => OtherStore
        },
        inflight = #{
            Inflight#discovery_job.key =>
                Inflight#discovery_job{ pid = self() }
        }
    },
    {PeerLoad, StoreLoad} = compute_job_load(Jobs),
    Counts = inflight_peer_store_mode_counts(Jobs),
    SameCombinationJobs = Jobs#discovery_jobs{
        pending = #{
            SamePendingKey#discovery_job.key => SamePendingKey
        }
    },
    ?assertEqual(none, take_next_job(
        SameCombinationJobs, PeerLoad, StoreLoad, Counts)),
    {ok, Selected, Jobs2} = take_next_job(
        Jobs, PeerLoad, StoreLoad, Counts),
    ?assertEqual(store2, Selected#discovery_job.store_id),
    ?assertEqual(
        #{SamePendingKey#discovery_job.key => SamePendingKey},
        Jobs2#discovery_jobs.pending).

take_next_job_prefers_recently_requested_location_test() ->
    Step = ar_sync_cursor:query_range_step_size(),
    Old = chunk_interval_job_for_test(peer, store, 0),
    New = chunk_interval_job_for_test(peer, store, Step),
    Jobs = #discovery_jobs{ pending = #{
        Old#discovery_job.key => Old,
        New#discovery_job.key => New
    }},
    {ok, Selected, _Jobs2} = take_next_job(Jobs, #{}, #{}, #{}),
    %% Equal-age readahead keeps the earlier queue location first.
    ?assertEqual(Old#discovery_job.key, Selected#discovery_job.key),
    TouchedNew = New#discovery_job{ requested_at = 1 },
    TouchedJobs = Jobs#discovery_jobs{ pending = #{
        Old#discovery_job.key => Old,
        TouchedNew#discovery_job.key => TouchedNew
    }},
    {ok, Selected2, _Jobs3} = take_next_job(
        TouchedJobs, #{}, #{}, #{}),
    %% Re-requesting the later location makes it the active frontier demand.
    ?assertEqual(TouchedNew#discovery_job.key, Selected2#discovery_job.key).

%% At the job limit, start_jobs/2 leaves pending and inflight work unchanged.
start_jobs_respects_inflight_limit_test() ->
    Limit = ?MAX_DISCOVERY_JOBS_PER_KIND,
    InflightJobs = [
        chunk_interval_job_for_test({p, N}, {s, N}, N)
        || N <- lists:seq(1, Limit)
    ],
    Inflight = maps:from_list([
        {Job#discovery_job.key, Job#discovery_job{ pid = self() }}
        || Job <- InflightJobs
    ]),
    PendingJob = chunk_interval_job_for_test(
        {p, 1}, pending_store, Limit + 1),
    DefaultJobsByKind = (#state{})#state.jobs,
    DefaultSyncBucketJobs = maps:get(sync_bucket, DefaultJobsByKind),
    ?assertEqual(Limit, DefaultSyncBucketJobs#discovery_jobs.max_inflight),
    ?assertEqual(?MAX_DISCOVERY_PEERS,
        DefaultSyncBucketJobs#discovery_jobs.max_pending),
    DefaultJobs = maps:get(chunk_interval, DefaultJobsByKind),
    ?assertEqual(Limit, DefaultJobs#discovery_jobs.max_inflight),
    ?assertEqual(?MAX_PENDING_CHUNK_INTERVAL_JOBS,
        DefaultJobs#discovery_jobs.max_pending),
    Jobs = DefaultJobs#discovery_jobs{
        inflight = Inflight,
        pending = #{PendingJob#discovery_job.key => PendingJob}
    },
    ?assertEqual(Jobs, start_jobs(Jobs)).

get_peers_for_offset_unions_sync_bucket_sources_test() ->
    reset_all_caches(),
    Offset = ?DATA_CHUNK_SIZE,
    BytePeer = {10, 0, 0, 4, 1984},
    FootprintPeer = {10, 0, 0, 5, 1984},
    store_row(sync_bucket, byte,
        Offset div ?NETWORK_DATA_BUCKET_SIZE, BytePeer, 1.0),
    store_row(sync_bucket, footprint,
        ar_footprint_record:get_footprint_bucket(Offset + ?DATA_CHUNK_SIZE),
        FootprintPeer, 1.0),
    ?assertEqual(lists:sort([BytePeer, FootprintPeer]),
        lists:sort(get_peers_for_offset(Offset))).

cached_peer_ranges_includes_byte_and_footprint_ranges_test() ->
    ar_test_util:with_mocked([
        {ar_peers, get_peer_release,
            fun(_) -> ?GET_FOOTPRINT_SUPPORT_RELEASE end}
    ], fun() ->
        reset_all_caches(),
        Peer = {10, 0, 0, 6, 1984},
        Offset = ?DATA_CHUNK_SIZE,
        RangeStart = 0,
        RangeEnd = 4 * ?DATA_CHUNK_SIZE,
        ByteIntervals = ar_intervals:from_list([
            {2 * ?DATA_CHUNK_SIZE, ?DATA_CHUNK_SIZE}
        ]),
        FootprintIntervals = ar_intervals:from_list([{4, 0}]),
        {Partition, Footprint} = ar_footprint_record:get_location(
            Offset + ?DATA_CHUNK_SIZE),
        FootprintKey = #footprint{ store_id = test_store,
            partition = Partition, footprint = Footprint },
        store_row(sync_bucket, byte,
            Offset div ?NETWORK_DATA_BUCKET_SIZE, Peer, 1.0),
        store_row(sync_bucket, footprint,
            ar_footprint_record:get_footprint_bucket(
                Offset + ?DATA_CHUNK_SIZE),
            Peer, 1.0),
        store_row(chunk_interval, byte, 0, Peer, ByteIntervals),
        store_row(chunk_interval, footprint, Offset, Peer, FootprintIntervals),
        ExpectedFootprintIntervals =
            ar_footprint_record:footprint_intervals_to_byte_intervals(
                FootprintIntervals),
        ?assertEqual(
            {[
                #peer_range{
                    store_id = test_store,
                    offset = Offset,
                    peer = Peer,
                    intervals = ByteIntervals,
                    footprint = none
                },
                #peer_range{
                    store_id = test_store,
                    offset = Offset,
                    peer = Peer,
                    intervals = ExpectedFootprintIntervals,
                    footprint = FootprintKey
                }
            ], ok},
            cached_peer_ranges(
                test_store, [Peer], Offset, RangeStart, RangeEnd))
    end).

get_chunk_intervals_limits_byte_metadata_to_requested_range_test() ->
    reset_all_caches(),
    Peer = {10, 0, 0, 1, 1984},
    Chunk = ?DATA_CHUNK_SIZE,
    %% The cached range spans four chunks while the query selects its middle two.
    Intervals = ar_intervals:from_list([{4 * Chunk, 0}]),
    Expected = ar_intervals:from_list([{3 * Chunk, Chunk}]),
    store_row(chunk_interval, byte, Chunk, Peer, Intervals),
    ?assertEqual({ok, Expected}, get_chunk_intervals(
        byte, Peer, Chunk, Chunk, 3 * Chunk)).

irrelevant_peer_has_no_cache_miss_without_detail_metadata_test() ->
    reset_all_caches(),
    Peer = {10, 0, 0, 7, 1984},
    ?assertEqual({[], ok}, cached_peer_ranges(
        ?DEFAULT_MODULE, [Peer], ?DATA_CHUNK_SIZE, 0, ?DATA_CHUNK_SIZE)).

cached_empty_metadata_is_ok_test() ->
    reset_all_caches(),
    Peer = {10, 0, 0, 7, 1984},
    store_row(sync_bucket, byte, 0, Peer, 1.0),
    store_row(chunk_interval, byte, 0, Peer, ar_intervals:new()),
    ?assertEqual({[], ok}, cached_peer_ranges(
        ?DEFAULT_MODULE, [Peer], 0, 0, ?DATA_CHUNK_SIZE)).

warming_schedules_miss_but_cached_read_has_no_side_effect_test() ->
    reset_all_caches(),
    Peer = {10, 0, 0, 8, 1984},
    store_row(sync_bucket, byte, 0, Peer, 1.0),
    DiscoveryPid = whereis(?MODULE),
    1 = erlang:trace(DiscoveryPid, true, ['receive']),
    try
        ?assertEqual({[], cache_miss}, cached_peer_ranges(
            ?DEFAULT_MODULE, [Peer], 0, 0, ?DATA_CHUNK_SIZE)),
        _ = sys:get_state(DiscoveryPid),
        receive
            {trace, DiscoveryPid, 'receive',
                    {'$gen_cast', {refresh_chunk_intervals, _Job}}} ->
                ?assert(false)
        after 0 ->
            ok
        end,
        ?assertEqual(ok,
            warm_peer_ranges(?DEFAULT_MODULE, [Peer], 0)),
        %% Reading the state is a mailbox barrier for the preceding cast.
        _ = sys:get_state(DiscoveryPid),
        RefreshRequest = receive
            {trace, DiscoveryPid, 'receive',
                    {'$gen_cast', {refresh_chunk_intervals, Job}}} ->
                Job
        %% The server mailbox barrier does not order delivery of trace messages.
        after 1000 ->
            none
        end,
        ?assertMatch(
            #discovery_job{ kind = chunk_interval, mode = byte },
            RefreshRequest)
    after
        1 = erlang:trace(DiscoveryPid, false, ['receive'])
    end.

%% An aged row is served as {stale, Intervals}: the data is still used while the
%% demand path requests a chunk interval job. A fresh row stays a plain hit.
stale_interval_rows_remain_usable_test() ->
    reset_all_caches(),
    Peer = {10, 0, 0, 1, 1984},
    RangeEnd = ?DATA_CHUNK_SIZE,
    Intervals = ar_intervals:from_list([{RangeEnd, 0}]),
    ByteKey = {byte, 0, Peer},
    store_row(chunk_interval, byte, 0, Peer, Intervals),
    ?assertEqual({ok, Intervals},
        get_chunk_intervals(byte, Peer, 0, 0, RangeEnd)),
    StaleMs = ar_timer:monotonic_ms()
            - ?CHUNK_INTERVAL_CACHE_TTL_MS - 1,
    ets:insert(?CHUNK_INTERVAL_CACHE_TABLE, {ByteKey, Intervals, StaleMs}),
    ?assertEqual({stale, Intervals},
        get_chunk_intervals(byte, Peer, 0, 0, RangeEnd)),
    %% A stale byte row remains usable without making the cached read impure.
    store_row(sync_bucket, byte, 0, Peer, 1.0),
    ?assertEqual(
        {[#peer_range{
            store_id = ?DEFAULT_MODULE,
            offset = 0,
            peer = Peer,
            intervals = Intervals,
            footprint = none
        }], ok},
        cached_peer_ranges(?DEFAULT_MODULE, [Peer], 0, 0, RangeEnd)),
    %% Footprint rows age against the same safety floor.
    FootprintOffset = 0,
    FootprintIntervals = ar_intervals:from_list([{1, 0}]),
    ExpectedFootprintIntervals =
        ar_footprint_record:footprint_intervals_to_byte_intervals(
            FootprintIntervals),
    FpKey = row_key(chunk_interval, footprint, FootprintOffset, Peer),
    store_row(chunk_interval, footprint,
        FootprintOffset, Peer, FootprintIntervals),
    ?assertEqual({ok, ExpectedFootprintIntervals}, get_chunk_intervals(
        footprint, Peer, FootprintOffset, 0, RangeEnd)),
    FpStaleMs = ar_timer:monotonic_ms()
            - ?CHUNK_INTERVAL_CACHE_TTL_MS - 1,
    ets:insert(?CHUNK_INTERVAL_CACHE_TABLE,
        {FpKey, FootprintIntervals, FpStaleMs}),
    ?assertEqual({stale, ExpectedFootprintIntervals}, get_chunk_intervals(
        footprint, Peer, FootprintOffset, 0, RangeEnd)).

warming_stale_metadata_requests_refresh_test() ->
    reset_all_caches(),
    Peer = {10, 0, 0, 9, 1984},
    Intervals = ar_intervals:from_list([{?DATA_CHUNK_SIZE, 0}]),
    StaleMs = ar_timer:monotonic_ms() - ?CHUNK_INTERVAL_CACHE_TTL_MS - 1,
    store_row(sync_bucket, byte, 0, Peer, 1.0),
    ets:insert(?CHUNK_INTERVAL_CACHE_TABLE,
        {row_key(chunk_interval, byte, 0, Peer), Intervals, StaleMs}),
    DiscoveryPid = whereis(?MODULE),
    1 = erlang:trace(DiscoveryPid, true, ['receive']),
    try
        ?assertEqual(ok, warm_peer_ranges(?DEFAULT_MODULE, [Peer], 0)),
        _ = sys:get_state(DiscoveryPid),
        receive
            {trace, DiscoveryPid, 'receive',
                    {'$gen_cast', {refresh_chunk_intervals,
                        #discovery_job{ mode = byte }}}} ->
                ok
        %% The server mailbox barrier does not order delivery of trace messages.
        after 1000 ->
            ?assert(false)
        end
    after
        1 = erlang:trace(DiscoveryPid, false, ['receive'])
    end.

%% A changed byte sync-bucket share marks the covered chunk intervals stale.
%% Unchanged shares and chunk intervals in other sync buckets remain fresh.
byte_share_change_marks_chunk_intervals_stale_test() ->
    reset_all_caches(),
    Peer = {10, 0, 0, 1, 1984},
    Intervals = ar_intervals:from_list([{100, 0}]),
    store_row(chunk_interval, byte, 0, Peer, Intervals),
    store_row(chunk_interval, byte,
        ?NETWORK_DATA_BUCKET_SIZE, Peer, Intervals),
    store_row(sync_bucket, byte, 0, Peer, 0.5),
    mark_chunk_intervals_stale_on_share_change(byte, Peer, 0, 0.5),
    ?assertMatch({hit, _}, chunk_interval_lookup(byte, Peer, 0)),
    mark_chunk_intervals_stale_on_share_change(byte, Peer, 0, 0.7),
    ?assertMatch({stale, _}, chunk_interval_lookup(byte, Peer, 0)),
    ?assertMatch({hit, _},
        chunk_interval_lookup(byte, Peer, ?NETWORK_DATA_BUCKET_SIZE)).

%% A changed footprint sync-bucket share has the same stale-marking behavior.
footprint_share_change_marks_chunk_intervals_stale_test() ->
    reset_all_caches(),
    Peer = {10, 0, 0, 1, 1984},
    Intervals = ar_intervals:from_list([{100, 0}]),
    InSyncBucketOffset = 0,
    %% Two buckets away is beyond the one-footprint boundary superset marked
    %% stale with bucket zero.
    FarAwayChunkEnd =
        ar_footprint_record:get_padded_offset_from_footprint_offset(
            2 * ?NETWORK_FOOTPRINT_BUCKET_SIZE + 1),
    FarAwayOffset = FarAwayChunkEnd - ?DATA_CHUNK_SIZE,
    SyncBucket = sync_bucket(footprint, InSyncBucketOffset),
    store_row(chunk_interval, footprint,
        InSyncBucketOffset, Peer, Intervals),
    store_row(chunk_interval, footprint, FarAwayOffset, Peer, Intervals),
    store_row(sync_bucket, footprint, SyncBucket, Peer, 0.5),
    mark_chunk_intervals_stale_on_share_change(
        footprint, Peer, SyncBucket, 0.5),
    ?assertMatch({hit, _},
        chunk_interval_lookup(footprint, Peer, InSyncBucketOffset)),
    mark_chunk_intervals_stale_on_share_change(
        footprint, Peer, SyncBucket, 0.7),
    ?assertMatch({stale, _},
        chunk_interval_lookup(footprint, Peer, InSyncBucketOffset)),
    ?assertMatch({hit, _},
        chunk_interval_lookup(footprint, Peer, FarAwayOffset)).

%% Sync buckets not updated within ?SYNC_BUCKET_CACHE_TTL_MS are retired
%% together with their cached chunk intervals; recently updated buckets remain.
expired_sync_buckets_delete_chunk_intervals_test() ->
    reset_all_caches(),
    Peer = {10, 0, 0, 9, 1984},
    Intervals = ar_intervals:from_list([{100, 0}]),
    Now = ar_timer:monotonic_ms(),
    Old = Now - ?SYNC_BUCKET_CACHE_TTL_MS - 1,
    ets:insert(?SYNC_BUCKET_CACHE_TABLE,
        {row_key(sync_bucket, byte, 0, Peer), 0.5, Old}),
    ets:insert(?SYNC_BUCKET_CACHE_TABLE,
        {row_key(sync_bucket, byte, 1, Peer), 0.5, Now}),
    store_row(chunk_interval, byte, 0, Peer, Intervals),
    store_row(chunk_interval, byte,
        ?NETWORK_DATA_BUCKET_SIZE, Peer, Intervals),
    delete_expired_sync_buckets(),
    ?assertNot(ets:member(?SYNC_BUCKET_CACHE_TABLE,
        row_key(sync_bucket, byte, 0, Peer))),
    ?assertEqual(miss, chunk_interval_lookup(byte, Peer, 0)),
    ?assert(ets:member(?SYNC_BUCKET_CACHE_TABLE,
        row_key(sync_bucket, byte, 1, Peer))),
    ?assertMatch({hit, _},
        chunk_interval_lookup(byte, Peer, ?NETWORK_DATA_BUCKET_SIZE)).

complete_chunk_interval_range_is_paginated_test_() ->
    Peer = {10, 0, 0, 8, 1984},
    Page1 = interval_page(1, ?QUERY_SYNC_INTERVALS_COUNT_LIMIT),
    Page1End = element(1, ar_intervals:largest(Page1)),
    NextStart = Page1End + 1,
    Page2 = interval_page(?QUERY_SYNC_INTERVALS_COUNT_LIMIT + 1, 1),
    Expected = ar_intervals:union(Page1, Page2),
    with_mocked_chunk_interval_pages(
        Peer,
        ?GET_SYNC_RECORD_RIGHT_BOUND_SUPPORT_RELEASE,
        fun
            (1) -> {ok, Page1};
            (Start) when Start =:= NextStart -> {ok, Page2}
        end,
        fun() ->
            ?assertEqual({ok, Expected},
                fetch_chunk_intervals(byte, Peer, 0))
        end).

failed_chunk_interval_page_aborts_fetch_test_() ->
    Peer = {10, 0, 0, 9, 1984},
    Page1 = interval_page(1, ?QUERY_SYNC_INTERVALS_COUNT_LIMIT),
    Page1End = element(1, ar_intervals:largest(Page1)),
    NextStart = Page1End + 1,
    with_mocked_chunk_interval_pages(
        Peer,
        ?GET_SYNC_RECORD_RIGHT_BOUND_SUPPORT_RELEASE,
        fun
            (1) -> {ok, Page1};
            (Start) when Start =:= NextStart -> {error, timeout}
        end,
        fun() ->
            ?assertEqual({error, timeout},
                fetch_chunk_intervals(byte, Peer, 0))
        end).

legacy_chunk_interval_pages_are_cut_to_query_range_test_() ->
    Peer = {10, 0, 0, 10, 1984},
    Page1 = interval_page(1, ?QUERY_SYNC_INTERVALS_COUNT_LIMIT),
    Page1End = element(1, ar_intervals:largest(Page1)),
    NextStart = Page1End + 1,
    Right = ar_sync_cursor:query_range_step_size(),
    Page2 = ar_intervals:from_list([{Right + 3, Right - 1}]),
    Expected = ar_intervals:cut(ar_intervals:union(Page1, Page2), Right),
    with_mocked_chunk_interval_pages(
        Peer,
        ?GET_SYNC_RECORD_RIGHT_BOUND_SUPPORT_RELEASE - 1,
        fun
            (1) -> {ok, Page1};
            (Start) when Start =:= NextStart -> {ok, Page2}
        end,
        fun() ->
            ?assertEqual({ok, Expected},
                fetch_chunk_intervals(byte, Peer, 0))
        end).

chunk_interval_pagination_is_bounded_test_() ->
    Peer = {10, 0, 0, 11, 1984},
    with_mocked_chunk_interval_pages(
        Peer,
        ?GET_SYNC_RECORD_RIGHT_BOUND_SUPPORT_RELEASE,
        fun(Start) ->
            Left = Start - 1,
            {ok, ar_intervals:from_list([
                {Left + 2 * I, Left + 2 * I - 1}
                || I <- lists:seq(1, ?QUERY_SYNC_INTERVALS_COUNT_LIMIT)
            ])}
        end,
        fun() ->
            ?assertEqual({error, interval_page_limit},
                fetch_chunk_intervals(byte, Peer, 0)),
            ?assertEqual(?MAX_CHUNK_INTERVAL_PAGES,
                meck:num_calls(ar_http_iface_client, get_sync_record, 4))
        end).

%% The coarse-diff gate: reuse a cached footprint only while its bucket share is
%% unchanged and the row is fresh; refetch when missing, share-changed, or stale.
%% A fresh cached footprint row is reused without an HTTP refetch; the mocked
%% request would fail if it were called.
refresh_chunk_intervals_reuses_cache_test() ->
    reset_all_caches(),
    Peer = {10, 0, 0, 1, 1984},
    Offset = 0,
    Intervals = ar_intervals:from_list([{1, 0}]),
    Expected = ar_footprint_record:footprint_intervals_to_byte_intervals(
        Intervals),
    store_row(sync_bucket, footprint,
        sync_bucket(footprint, Offset), Peer, 1.0),
    store_row(chunk_interval, footprint, Offset, Peer, Intervals),
    meck:new(ar_http_iface_client, [passthrough]),
    meck:expect(ar_http_iface_client, get_footprints,
            fun(_, _, _) -> {error, timeout} end),
    try
        Job = #discovery_job{
            kind = chunk_interval,
            peer = Peer,
            store_id = test_store,
            mode = footprint,
            start = Offset
        },
        ?assertEqual(ok,
            refresh_chunk_intervals(Job)),
        ?assertEqual(0,
            meck:num_calls(ar_http_iface_client, get_footprints, 3)),
        ?assertEqual({ok, Expected}, get_chunk_intervals(
            footprint, Peer, Offset, 0, ?DATA_CHUNK_SIZE))
    after
        meck:unload(ar_http_iface_client)
    end.

-endif.

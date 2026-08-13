%%% @doc Maintains a cache of the data ranges served by each peer.
%%%
%%% Sync bucket jobs cache coarse peer availability. Chunk interval jobs retrieve
%%% detailed availability from /data_sync_record and /footprints and cache it in
%%% ETS.
%%%
%%% ar_sync_chunk_picker reads this cache when deciding which chunks can be
%%% fetched from which peers.
-module(ar_sync_discovery).
-test_category([fast]).

-behaviour(gen_server).

-export([start_link/0, get_peers_for_offset/1,
        get_peer_ranges_for_peers/5]).

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

%% A freshly booted node needs enough chunk interval capacity to warm all
%% simulated stores before peer concurrency caps are available.
-define(MIN_CHUNK_INTERVAL_JOBS, 8).
-define(MAX_CHUNK_INTERVAL_JOBS, 200).
-define(MAX_PENDING_CHUNK_INTERVAL_JOBS, 1024).

%% One chunk interval job per this many published chunk-fetch slots.
-define(FETCH_CAPACITY_PER_CHUNK_INTERVAL_JOB, 8).

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
%% read by ar_sync_chunk_picker to compute fetchable intervals.
%%
%% Rows are `{Key, Intervals, MonotonicMs}', keyed by
%% `{Mode, Location, Peer}'. The timestamp is used for demand-side freshness
%% checks and oldest-first cache trimming.
%% The chunk interval cache's byte limit is its carve-out of the
%% [sync, cache_size] budget — one knob for all sync memory — owned by
%% ar_data_sync (interval_cache_size_limit/0). When the cap fires we drop the
%% oldest 10% of rows in a single pass; still-needed rows are re-warmed on
%% demand.


%% Discovery work pending, inflight, or scheduled for later.
-record(discovery_job, {
    key,
    kind,
    peer,
    store_id = undefined,
    mode = undefined,
    start = undefined,
    pid = undefined,
    timer_ref = undefined,
    token = undefined
}).

%% Shared state for sync bucket and chunk interval jobs.
-record(discovery_jobs, {
    %% Pending identity => #discovery_job{}. Chunk interval requests coalesce
    %% by peer, store, and mode so the newest requested frontier replaces old work.
    pending = #{},
    %% Key => #discovery_job{ pid = pid() }.
    inflight = #{},
    %% Key => #discovery_job{ timer_ref = reference(), token = reference() }.
    %% Empty for job types without schedules.
    scheduled = #{},
    %% Maximum concurrent jobs in this collection.
    max_inflight = infinity,
    %% Sync bucket work is bounded by the tracked peer set. Demand-driven
    %% chunk interval work supplies an explicit pending limit.
    max_pending = infinity
}).

-record(state, {
    %% Authoritative current peer set used to reject cache results from
    %% removed peers.
    tracked_peers = sets:new(),
    jobs = #{
        %% Coarse per-peer jobs bounded by
        %% [sync, max_concurrent_sync_bucket_jobs].
        sync_bucket => #discovery_jobs{},
        %% Demand-driven chunk interval jobs. Sync bucket jobs only update
        %% which peers hold which coarse sync buckets; chunk intervals
        %% from /footprints and /data_sync_record are fetched on demand for a
        %% bounded span beginning at a store's sweep frontier. Concurrency is
        %% limited so discovery cannot saturate the shared request path and
        %% starve /chunk2 fetching.
        chunk_interval => #discovery_jobs{
            max_inflight = ?MIN_CHUNK_INTERVAL_JOBS,
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
        ar_sync_cursor:kinds()),
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

%% @doc Return each peer's non-empty cached intervals for the requested range.
%% Missing or stale metadata also schedules one bounded forward job for that
%% peer, but stale intervals remain usable while their replacement is fetched.
get_peer_ranges_for_peers(StoreID, Peers, Offset, RangeStart, RangeEnd) ->
    Results = lists:map(
        fun(Peer) ->
            get_peer_ranges_for_peer(
                StoreID, Peer, Offset, RangeStart, RangeEnd)
        end,
        Peers),
    {PeerRanges, ChunkIntervalJobs} = combine_peer_range_results(Results),
    case ChunkIntervalJobs of
        [] -> ok;
        _ ->
            gen_server:cast(
                ?MODULE, {refresh_chunk_intervals, ChunkIntervalJobs})
    end,
    PeerRanges.

get_peer_ranges_for_peer(StoreID, Peer, Offset, RangeStart, RangeEnd) ->
    PeerRange = #peer_range{
        store_id = StoreID,
        offset = Offset,
        peer = Peer,
        intervals = ar_intervals:new()
    },
    Results = lists:map(
        fun(Mode) ->
            get_peer_ranges_for_peer(
                Mode, PeerRange, RangeStart, RangeEnd)
        end,
        peer_kinds(Peer)),
    combine_peer_range_results(Results).

combine_peer_range_results(Results) ->
    {PeerRangeGroups, JobGroups} = lists:unzip(Results),
    {lists:append(PeerRangeGroups), lists:append(JobGroups)}.

get_peer_ranges_for_peer(Mode, BasePeerRange, RangeStart, RangeEnd) ->
    #peer_range{ peer = Peer, offset = Offset, store_id = StoreID } =
        BasePeerRange,
    case peer_has_sync_bucket(Mode, Peer, Offset) of
        false ->
            {[], []};
        true ->
            Location = interval_location(Mode, Offset),
            {Intervals, ShouldRefresh} =
                case get_chunk_intervals(Mode, Peer, Location) of
                    {error, cache_miss} ->
                        {ar_intervals:new(), true};
                    {Freshness, CachedIntervals} ->
                        {CachedIntervals, Freshness =:= stale}
                end,
            PeerRange = BasePeerRange#peer_range{
                footprint = footprint_key(Mode, StoreID, Location)
            },
            PeerRanges = build_peer_ranges(
                Mode, PeerRange, Intervals, RangeStart, RangeEnd),
            ChunkIntervalJobs = case ShouldRefresh of
                true -> [chunk_interval_job(Mode, PeerRange)];
                false -> []
            end,
            {PeerRanges, ChunkIntervalJobs}
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

%% @doc Return a peer's cached chunk intervals at the mode-specific location.
%% Byte locations are offsets; footprint locations are `{Partition, Footprint}'.
-spec get_chunk_intervals(Mode, Peer, Location) ->
        {ok, ar_intervals:intervals()} | {stale, ar_intervals:intervals()}
            | {error, cache_miss} when
    Mode :: byte | footprint,
    Peer :: term(),
    Location :: non_neg_integer() | {non_neg_integer(), non_neg_integer()}.
get_chunk_intervals(Mode, Peer, Location) ->
    case chunk_interval_lookup(Mode, Peer, Location) of
        {hit, Intervals} ->
            {ok, Intervals};
        {stale, Intervals} ->
            {stale, Intervals};
        miss ->
            {error, cache_miss}
    end.

interval_location(byte, Offset) ->
    Offset;
interval_location(footprint, Offset) ->
    ar_footprint_record:get_location(Offset + ?DATA_CHUNK_SIZE).

%% @doc Convert cached intervals into non-empty byte-space peer ranges. Byte
%% ranges are clipped to the request; a footprint location returns its complete
%% advertised footprint so downstream admission can preserve entropy reuse.
-spec build_peer_ranges(byte | footprint, #peer_range{},
        ar_intervals:intervals(), non_neg_integer(), non_neg_integer()) ->
        [#peer_range{}].
build_peer_ranges(_Mode, _PeerRange, _Intervals, RangeStart, RangeEnd)
        when RangeStart >= RangeEnd ->
    [];
build_peer_ranges(Mode, PeerRange, Intervals, RangeStart, RangeEnd) ->
    ByteIntervals = case Mode of
        byte ->
            ByteRange = ar_intervals:from_list([{RangeEnd, RangeStart}]),
            ar_intervals:intersection(Intervals, ByteRange);
        footprint ->
            ar_footprint_record:footprint_intervals_to_byte_intervals(Intervals)
    end,
    case ar_intervals:is_empty(ByteIntervals) of
        true -> [];
        false -> [PeerRange#peer_range{ intervals = ByteIntervals }]
    end.

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
    %% Periodic maintenance updates job limits and cache state, then emits
    %% discovery metrics. It fires from
    %% inside the gen_server so it has direct access to State.
    ar_timer:send_after(?MAINTENANCE_INTERVAL_MS, self(), maintenance),
    [ok, ok] = ar_events:subscribe([peer, node_state]),
    {ok, #state{}}.

handle_call(inflight_count, _From, State) ->
    %% Sim-driver support: discovery and fetch job processes sleep on the
    %% simulated clock, and the driver's settle invariant needs both counts.
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

handle_cast({refresh_chunk_intervals, ChunkIntervalJobs}, State) ->
    State2 = lists:foldl(
        fun(ChunkIntervalJob, Acc) ->
            enqueue_job(ChunkIntervalJob, Acc)
        end,
        State,
        ChunkIntervalJobs),
    {noreply, start_jobs(chunk_interval, State2)};

handle_cast({job_result, Peer, Result}, State) ->
    case is_peer_tracked(Peer, State) of
        true -> record_job_result(Peer, Result);
        false -> ok
    end,
    {noreply, State};

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

handle_info(maintenance, State0) ->
    ar_timer:send_after(?MAINTENANCE_INTERVAL_MS, self(), maintenance),
    State = start_jobs(chunk_interval, run_maintenance(State0)),
    {noreply, start_jobs(sync_bucket, State)};

handle_info({enqueue_scheduled_job, Kind, Key, Token}, State) ->
    State2 = enqueue_scheduled_job(Kind, Key, Token, State),
    {noreply, start_jobs(Kind, State2)};

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

run_maintenance(State0) ->
    ChunkIntervalMaxInflight = chunk_interval_max_inflight(
        ar_sync_peer:published_concurrency_cap_total()),
    #state{ jobs = JobsByKind } = State0,
    ChunkIntervalJobs = maps:get(chunk_interval, JobsByKind),
    ChunkIntervalJobs2 = ChunkIntervalJobs#discovery_jobs{
        max_inflight = ChunkIntervalMaxInflight
    },
    State = State0#state{
        jobs = JobsByKind#{chunk_interval := ChunkIntervalJobs2}
    },
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
            %% A thousand is above the expected known-peer population while still
            %% bounding the work submitted in one collection cycle.
            MaxPeers = 1000,
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
            gen_server:cast(?MODULE, {add_peers, lists:sublist(Peers, MaxPeers)})
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

peer_kinds(Peer) ->
    lists:filter(
        fun(Mode) -> peer_supports(Mode, Peer) end,
        ar_sync_cursor:kinds()).

peer_supports(byte, _Peer) ->
    true;
peer_supports(footprint, Peer) ->
    ar_sync_deps:get_peer_release(Peer) >= ?GET_FOOTPRINT_SUPPORT_RELEASE.

%%%===================================================================
%%% Generic job functions.
%%%===================================================================

inflight_count(#discovery_jobs{ inflight = Inflight }) ->
    map_size(Inflight).

job_exists(Key, Jobs) ->
    #discovery_jobs{
        pending = Pending,
        inflight = Inflight,
        scheduled = Scheduled
    } = Jobs,
    PendingMatch = case maps:get(pending_key(Key), Pending, undefined) of
        #discovery_job{ key = Key } -> true;
        _ -> false
    end,
    PendingMatch
        orelse maps:is_key(Key, Inflight)
        orelse maps:is_key(Key, Scheduled).

pending_key({chunk_interval, Peer, StoreID, Mode, _Start}) ->
    {chunk_interval, Peer, StoreID, Mode};
pending_key(Key) ->
    Key.

enqueue_job(Job, State) ->
    #discovery_job{ kind = Kind } = Job,
    #state{ tracked_peers = TrackedPeers, jobs = JobsByKind } = State,
    Jobs = maps:get(Kind, JobsByKind),
    {_Result, Jobs2} = enqueue_job(Job, Jobs, TrackedPeers),
    State#state{ jobs = JobsByKind#{Kind := Jobs2} }.

%% @doc Add Job when its peer is tracked and its key is not already active.
%% A newer chunk interval request replaces pending work for the same peer,
%% store, and mode. Other work is deferred when the pending limit is full.
enqueue_job(Job, Jobs, TrackedPeers) ->
    #discovery_job{ key = Key, peer = Peer } = Job,
    #discovery_jobs{
        pending = Pending,
        max_pending = MaxPending
    } = Jobs,
    PendingKey = pending_key(Key),
    case {
        sets:is_element(Peer, TrackedPeers),
        job_exists(Key, Jobs),
        maps:is_key(PendingKey, Pending),
        map_size(Pending) >= MaxPending
    } of
        {false, _, _, _} ->
            {untracked, Jobs};
        {true, true, _, _} ->
            {duplicate, Jobs};
        {true, false, true, _} ->
            {replaced, Jobs#discovery_jobs{
                pending = maps:put(PendingKey, Job, Pending)
            }};
        {true, false, false, true} ->
            {full, Jobs};
        {true, false, false, false} ->
            {enqueued, Jobs#discovery_jobs{
                pending = maps:put(PendingKey, Job, Pending)
                }}
    end.

%% @doc Move a scheduled job into the pending set when its timer token still
%% matches. A stale timer leaves the current schedule unchanged.
enqueue_scheduled_job(Kind, Key, Token, State) ->
    #state{ jobs = JobsByKind } = State,
    Jobs = maps:get(Kind, JobsByKind),
    #discovery_jobs{ scheduled = Scheduled } = Jobs,
    case maps:get(Key, Scheduled, undefined) of
        #discovery_job{ token = Token } = ScheduledJob ->
            Jobs2 = Jobs#discovery_jobs{
                scheduled = maps:remove(Key, Scheduled)
            },
            PendingJob = ScheduledJob#discovery_job{
                timer_ref = undefined,
                token = undefined
            },
            State2 = State#state{
                jobs = JobsByKind#{Kind := Jobs2}
            },
            enqueue_job(PendingJob, State2);
        _ ->
            State
    end.

%% @doc Schedule Job unless its peer is no longer tracked or the same job key
%% is already pending, inflight, or scheduled.
schedule_job(Job, DelayMs, State) ->
    #discovery_job{ key = Key, kind = Kind, peer = Peer } = Job,
    #state{ jobs = JobsByKind } = State,
    Jobs = maps:get(Kind, JobsByKind),
    maybe
        true ?= is_peer_tracked(Peer, State),
        true ?= not job_exists(Key, Jobs),
        Token = make_ref(),
        {ok, TimerRef} = ar_timer:send_after(
            DelayMs, self(), {enqueue_scheduled_job, Kind, Key, Token}),
        #discovery_jobs{ scheduled = Scheduled } = Jobs,
        ScheduledJob = Job#discovery_job{
            pid = undefined,
            timer_ref = TimerRef,
            token = Token
        },
        Jobs2 = Jobs#discovery_jobs{
            scheduled = maps:put(Key, ScheduledJob, Scheduled)
        },
        State#state{ jobs = JobsByKind#{Kind := Jobs2} }
    else
        false -> State
    end.

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
    safe_refresh_chunk_intervals(Job).

pid_to_job(Pid, Inflight) ->
    case lists:search(
            fun({_Key, #discovery_job{ pid = JobPid }}) ->
                JobPid =:= Pid
            end,
            maps:to_list(Inflight)) of
        {value, {Key, Job}} -> {ok, Key, Job};
        false -> error
    end.

finish_job(Pid, State) ->
    #state{ jobs = JobsByKind } = State,
    Result = maps:fold(
        fun(_Kind, _Jobs, {ok, _, _, _} = Acc) ->
                Acc;
            (Kind, Jobs, error) ->
                #discovery_jobs{ inflight = Inflight } = Jobs,
                case pid_to_job(Pid, Inflight) of
                    {ok, Key, Job} ->
                        Jobs2 = Jobs#discovery_jobs{
                            inflight = maps:remove(Key, Inflight)
                        },
                        {ok, Kind,
                            Job#discovery_job{ pid = undefined }, Jobs2};
                    error ->
                        error
                end
        end,
        error,
        JobsByKind),
    case Result of
        {ok, Kind, Job, Jobs2} ->
            State2 = State#state{
                jobs = JobsByKind#{Kind := Jobs2}
            },
            job_completed(Job, State2);
        error ->
            State
    end.

job_completed(#discovery_job{ kind = sync_bucket } = Job, State) ->
    State2 = schedule_job(Job, sync_bucket_job_delay_ms(), State),
    start_jobs(sync_bucket, State2);
job_completed(#discovery_job{ kind = chunk_interval }, State) ->
    start_jobs(chunk_interval, State).

%% @doc Remove all work for Peer and stop its inflight job processes.
remove_jobs(Peer, Jobs) ->
    #discovery_jobs{
        pending = Pending,
        inflight = Inflight,
        scheduled = Scheduled
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
            Inflight),
        scheduled = maps:filter(
            fun(_Key, #discovery_job{
                    peer = JobPeer,
                    timer_ref = TimerRef
                }) when JobPeer =:= Peer ->
                    _ = ar_timer:cancel(TimerRef),
                    false;
                (_Key, _Job) ->
                    true
            end,
            Scheduled)
    }.

%% @doc Start pending jobs while preferring idle peers and stores.
start_jobs(Kind, State) ->
    #state{ jobs = JobsByKind } = State,
    Jobs0 = maps:get(Kind, JobsByKind),
    Jobs = case Kind of
        sync_bucket ->
            Jobs0#discovery_jobs{
                max_inflight = arweave_config:get(
                    [sync, max_concurrent_sync_bucket_jobs])
            };
        chunk_interval ->
            Jobs0
    end,
    Jobs2 = start_jobs(Jobs),
    State#state{ jobs = JobsByKind#{Kind := Jobs2} }.

start_jobs(Jobs) ->
    case has_capacity(Jobs) of
        false ->
            %% Do not inspect pending jobs at the concurrency limit.
            Jobs;
        true ->
            {PeerLoad, StoreLoad} = compute_job_load(Jobs),
            start_pending_jobs(Jobs, PeerLoad, StoreLoad)
    end.

start_pending_jobs(Jobs, PeerLoad, StoreLoad) ->
    case has_capacity(Jobs) of
        true ->
            case take_next_job(Jobs, PeerLoad, StoreLoad) of
                {ok, #discovery_job{
                        peer = Peer,
                        store_id = StoreID
                    } = Job, Jobs2} ->
                    Jobs3 = start_job(Job, Jobs2),
                    start_pending_jobs(Jobs3,
                        arweave_util:increment_map_value(Peer, PeerLoad),
                        arweave_util:increment_map_value(StoreID, StoreLoad));
                none ->
                    Jobs
            end;
        false ->
            Jobs
    end.

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

take_next_job(Jobs, PeerLoad, StoreLoad) ->
    #discovery_jobs{ pending = Pending } = Jobs,
    case maps:to_list(Pending) of
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

%% @doc The delay before a peer's next sync bucket job, with +/-25% jitter so
%% peers do not all become due together.
sync_bucket_job_delay_ms() ->
    Spread = ?SYNC_BUCKET_JOB_INTERVAL_MS div 4,
    ?SYNC_BUCKET_JOB_INTERVAL_MS - Spread + rand:uniform(2 * Spread + 1) - 1.

%% Fetch the peer's coarse availability for each mode
%% sequentially so one peer occupies at most one metadata connection at a time.
fetch_sync_buckets(Peer) ->
    lists:foreach(
        fun(Mode) -> fetch_sync_buckets(Mode, Peer) end,
        peer_kinds(Peer)).

fetch_sync_buckets(Mode, Peer) ->
    try
        case ar_sync_deps:get_sync_buckets(Peer, Mode) of
            {ok, SyncBuckets} ->
                gen_server:cast(?MODULE,
                    {job_result, Peer, {sync_buckets, Mode, SyncBuckets}}),
                {ok, SyncBuckets};
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
    end.

%%%===================================================================
%%% Cache manipulation.
%%%===================================================================

record_job_result(Peer, {sync_buckets, Mode, SyncBuckets}) ->
    store_sync_buckets(Mode, Peer, SyncBuckets),
    ?LOG_DEBUG([{event, processed_sync_buckets},
        {peer, arweave_util:format_peer(Peer)}, {mode, Mode}]);
record_job_result(Peer,
        {chunk_intervals, StoreID, Offset, Mode, Location, {ok, Intervals}}) ->
    store_row(chunk_interval, Mode, Location, Peer, Intervals),
    notify_chunk_intervals_updated(StoreID, Offset, Intervals);
record_job_result(Peer,
        {chunk_intervals, _StoreID, _Offset, Mode, Location, _Error}) ->
    %% Do not continue serving stale metadata when its replacement cannot be
    %% fetched. A later sweep will request it again.
    delete_row(chunk_interval, Mode, Location, Peer).

row_key(sync_bucket, Mode, SyncBucket, Peer) ->
    {Mode, SyncBucket, Peer};
row_key(chunk_interval, byte, Offset, Peer) ->
    %% Byte rows use the step-aligned location queried by chunk interval jobs.
    Step = ar_sync_cursor:query_range_step_size(),
    {byte, (Offset div Step) * Step, Peer};
row_key(chunk_interval, footprint, Location, Peer) ->
    {footprint, Location, Peer}.

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
        byte ->
            Step = ar_sync_cursor:query_range_step_size(),
            (Offset div Step) * Step;
        footprint ->
            Offset
    end,
    #discovery_job{
        key = {chunk_interval, Peer, StoreID, Mode, Start},
        kind = chunk_interval,
        peer = Peer,
        store_id = StoreID,
        mode = Mode,
        start = Start
    }.

%% @doc Bound metadata concurrency to a fraction of chunk-fetch capacity.
%% The minimum does not depend on active peer caps: a peer needs refreshed
%% metadata before it can serve chunks and earn a cap.
chunk_interval_max_inflight(PublishedCapTotal) ->
    max(?MIN_CHUNK_INTERVAL_JOBS,
        min(PublishedCapTotal div ?FETCH_CAPACITY_PER_CHUNK_INTERVAL_JOB,
            ?MAX_CHUNK_INTERVAL_JOBS)).

safe_refresh_chunk_intervals(ChunkIntervalJob) ->
    #discovery_job{
        kind = chunk_interval,
        peer = Peer,
        mode = Mode
    } = ChunkIntervalJob,
    try
        refresh_chunk_intervals(ChunkIntervalJob)
    catch Class:Reason:Stacktrace ->
        ?LOG_WARNING([{event, chunk_interval_job_crashed}, {class, Class},
                {peer, arweave_util:format_peer(Peer)}, {mode, Mode},
                {reason, io_lib:format("~p", [Reason])}, {stacktrace, Stacktrace}])
    end.

%% @doc Refresh chunk interval metadata at and immediately after the requested
%% frontier. The periodically refreshed coarse map determines which requests
%% are useful. Byte and footprint positions use their native traversal order.
refresh_chunk_intervals(Job) ->
    #discovery_job{
        kind = chunk_interval,
        peer = Peer,
        store_id = StoreID,
        mode = Mode,
        start = Start
    } = Job,
    Offsets = chunk_interval_offsets(Mode, StoreID, Start),
    lists:foreach(
        fun(Offset) ->
            refresh_chunk_intervals(Mode, Peer, StoreID, Offset)
        end,
        Offsets).

refresh_chunk_intervals(Mode, Peer, StoreID, Offset) ->
    case peer_has_sync_bucket(Mode, Peer, Offset) of
        true ->
            Location = interval_location(Mode, Offset),
            case chunk_interval_lookup(Mode, Peer, Location) of
                {hit, _Intervals} ->
                    ok;
                _MissOrStale ->
                    Result = fetch_chunk_intervals(Mode, Peer, Location),
                    gen_server:cast(?MODULE,
                        {job_result, Peer,
                            {chunk_intervals, StoreID, Offset,
                                Mode, Location, Result}})
            end;
        false ->
            ok
    end.

notify_chunk_intervals_updated(StoreID, Offset, Intervals) ->
    case ar_intervals:is_empty(Intervals) of
        true -> ok;
        false -> ar_events:send(sync_discovery,
            {chunk_intervals_updated, StoreID, Offset})
    end.

%% Eight byte query ranges keep metadata just ahead of the byte cursor without
%% building a second readahead queue.
chunk_interval_offsets(byte, _StoreID, Start) ->
    Step = ar_sync_cursor:query_range_step_size(),
    lists:seq(Start, Start + 7 * Step, Step);
%% One footprint response describes up to 1024 chunks. Fetch only the requested
%% footprint so one readahead worker cannot hold a job slot across many HTTP
%% requests while other peer/store fronts remain cold.
chunk_interval_offsets(footprint, _StoreID, Start) ->
    [Start].

%%%===================================================================
%%% Chunk interval cache.
%%%===================================================================

mark_chunk_intervals_stale(Mode, Peer, SyncBucket) ->
    StaleTimestamp =
        expiration_cutoff(?CHUNK_INTERVAL_CACHE_TTL_MS) - 1,
    lists:foreach(
        fun(Location) ->
            ets:update_element(?CHUNK_INTERVAL_CACHE_TABLE,
                row_key(chunk_interval, Mode, Location, Peer),
                {3, StaleTimestamp})
        end,
        chunk_interval_locations(Mode, Peer, SyncBucket)).

delete_chunk_intervals(Mode, Peer, SyncBucket) ->
    Locations = chunk_interval_locations(Mode, Peer, SyncBucket),
    lists:foreach(
        fun(Location) ->
            delete_row(chunk_interval, Mode, Location, Peer)
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
chunk_interval_lookup(Mode, Peer, Location) ->
    Key = row_key(chunk_interval, Mode, Location, Peer),
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
    emit_sync_discovery_peers().

record_chunk_interval_evictions(_Reason, 0) ->
    ok;
record_chunk_interval_evictions(Reason, Count) ->
    arweave_metrics:counter_inc(chunk_interval_cache_evictions, [Reason], Count).

emit_sync_discovery_peers() ->
    Modes = ar_sync_cursor:kinds(),
    StorageModules = lists:map(
        fun arweave_config:config_to_storage_module/1,
        arweave_config:get([storage_modules])),
    StorePeerSets = lists:flatmap(
        fun(Module) -> get_peers_for_store(Module, Modes) end,
        StorageModules),
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
    Step = ar_sync_cursor:query_range_step_size(),
    AlignedStart = (Start div Step) * Step,
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

chunk_interval_max_inflight_test() ->
    ?assertEqual(?MIN_CHUNK_INTERVAL_JOBS, chunk_interval_max_inflight(0)),
    ?assertEqual(?MIN_CHUNK_INTERVAL_JOBS,
        chunk_interval_max_inflight(
            ?FETCH_CAPACITY_PER_CHUNK_INTERVAL_JOB
                * ?MIN_CHUNK_INTERVAL_JOBS - 1)),
    %% 320 published fetch slots provide 40 chunk interval jobs at 8:1.
    ?assertEqual(40, chunk_interval_max_inflight(320)),
    ?assertEqual(?MAX_CHUNK_INTERVAL_JOBS, chunk_interval_max_inflight(1600)),
    ?assertEqual(?MAX_CHUNK_INTERVAL_JOBS, chunk_interval_max_inflight(6400)).

enqueue_job_deduplicates_pending_and_inflight_test() ->
    Job = chunk_interval_job_for_test(peer, store, 0),
    TrackedPeers = sets:from_list([Job#discovery_job.peer]),
    {enqueued, PendingJobs} =
        enqueue_job(Job, #discovery_jobs{}, TrackedPeers),
    ?assertEqual(1, map_size(PendingJobs#discovery_jobs.pending)),
    ?assertEqual(
        {duplicate, PendingJobs},
        enqueue_job(Job, PendingJobs, TrackedPeers)),
    {ok, Job, TakenJobs} = take_next_job(PendingJobs, #{}, #{}),
    ?assertEqual(0, map_size(TakenJobs#discovery_jobs.pending)),
    InflightJob = Job#discovery_job{ pid = job_pid },
    InflightJobs = TakenJobs#discovery_jobs{
        inflight = #{Job#discovery_job.key => InflightJob}
    },
    ?assertEqual(
        {duplicate, InflightJobs},
        enqueue_job(Job, InflightJobs, TrackedPeers)).

enqueue_job_rejects_untracked_peer_test() ->
    Job = chunk_interval_job_for_test(peer, store, 0),
    Jobs = #discovery_jobs{},
    ?assertEqual({untracked, Jobs},
        enqueue_job(Job, Jobs, sets:new())).

enqueue_job_replaces_older_pending_lane_test() ->
    Step = ar_sync_cursor:query_range_step_size(),
    Existing = chunk_interval_job_for_test(peer, store, 0),
    Incoming = chunk_interval_job_for_test(peer, store, Step),
    TrackedPeers = sets:from_list([peer]),
    {enqueued, Jobs} =
        enqueue_job(Existing, #discovery_jobs{ max_pending = 1 }, TrackedPeers),
    {replaced, Jobs2} = enqueue_job(Incoming, Jobs, TrackedPeers),
    ?assertEqual(1, map_size(Jobs2#discovery_jobs.pending)),
    ?assertNot(job_exists(Existing#discovery_job.key, Jobs2)),
    ?assert(job_exists(Incoming#discovery_job.key, Jobs2)).

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
    ?assertEqual(#{}, Jobs#discovery_jobs.scheduled),
    ?assertEqual(1, map_size(Jobs#discovery_jobs.pending)),
    ?assert(job_exists({sync_bucket, Peer}, Jobs)),
    %% Re-collecting an already tracked peer must not duplicate its job.
    ?assertEqual(State, add_peer(Peer, State)).

scheduled_job_requires_current_token_test() ->
    Peer = peer,
    Job = sync_bucket_job(Peer),
    Key = Job#discovery_job.key,
    Token = make_ref(),
    ScheduledJob = Job#discovery_job{
        timer_ref = timer_ref,
        token = Token
    },
    State0 = #state{ tracked_peers = sets:from_list([Peer]) },
    State = State0#state{
        jobs = (State0#state.jobs)#{
            sync_bucket := #discovery_jobs{
                scheduled = #{Key => ScheduledJob}
            }
        }
    },
    %% A stale timer token leaves the current scheduled job unchanged.
    ?assertEqual(State,
        enqueue_scheduled_job(sync_bucket, Key, make_ref(), State)),
    State2 = enqueue_scheduled_job(sync_bucket, Key, Token, State),
    Jobs2 = maps:get(sync_bucket, State2#state.jobs),
    ?assertEqual(#{}, Jobs2#discovery_jobs.scheduled),
    ?assertEqual(
        #{Key => Job},
        Jobs2#discovery_jobs.pending).

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
            pending_key(RemovedPendingJob#discovery_job.key) => RemovedPendingJob,
            pending_key(KeptPendingJob#discovery_job.key) => KeptPendingJob
        },
        inflight = #{
            RemovedInflightJob2#discovery_job.key => RemovedInflightJob2
        }
    },
    Jobs2 = remove_jobs(RemovedPeer, Jobs),
    ?assertNot(is_process_alive(RemovedJobPID)),
    ?assertNot(job_exists(RemovedPendingJob#discovery_job.key, Jobs2)),
    ?assertNot(job_exists(RemovedInflightJob2#discovery_job.key, Jobs2)),
    ?assert(job_exists(KeptPendingJob#discovery_job.key, Jobs2)).

removed_peer_job_results_are_ignored_test() ->
    reset_all_caches(),
    Peer = {10, 0, 0, 1, 1984},
    Location = 0,
    Intervals = ar_intervals:from_list([{?DATA_CHUNK_SIZE, 0}]),
    TrackedState = add_peer(Peer, #state{}),
    _ = handle_cast(
        {job_result, Peer,
            {chunk_intervals, test_store, Location,
                byte, Location, {ok, Intervals}}},
        TrackedState),
    ?assertEqual({hit, Intervals}, chunk_interval_lookup(byte, Peer, Location)),
    store_row(sync_bucket, byte, 0, Peer, 1.0),
    {noreply, RemovedState} =
        do_remove_peer(Peer, test, TrackedState),
    ?assertEqual(miss, chunk_interval_lookup(byte, Peer, Location)),
    ?assertNot(ets:member(?SYNC_BUCKET_CACHE_TABLE,
        row_key(sync_bucket, byte, 0, Peer))),
    _ = handle_cast(
        {job_result, Peer,
            {chunk_intervals, test_store, Location,
                byte, Location, {ok, Intervals}}},
        RemovedState),
    ?assertEqual(miss, chunk_interval_lookup(byte, Peer, Location)),
    SyncBuckets = ar_sync_buckets:from_intervals(Intervals),
    _ = handle_cast(
        {job_result, Peer, {sync_buckets, byte, SyncBuckets}},
        RemovedState),
    ?assertNot(ets:member(?SYNC_BUCKET_CACHE_TABLE,
        row_key(sync_bucket, byte, 0, Peer))).

chunk_intervals_update_notifies_requesting_store_test_() ->
    ar_test_util:with_mocked([
        {ar_events, send,
            fun(sync_discovery, Event) ->
                put(sync_discovery_event, Event),
                ok
            end}
    ], fun() ->
        reset_all_caches(),
        erase(sync_discovery_event),
        Peer = {10, 0, 0, 12, 1984},
        Intervals = ar_intervals:from_list([{?DATA_CHUNK_SIZE, 0}]),
        ?assertEqual(ok, record_job_result(Peer,
            {chunk_intervals, test_store, 0, byte, 0, {ok, Intervals}})),
        ?assertEqual(
            {chunk_intervals_updated, test_store, 0},
            get(sync_discovery_event))
    end, 30).

%% The one-entry limit models any full pending set without constructing the
%% production-sized set. An active job explains why pending work has not started.
enqueue_job_defers_when_pending_limit_reached_test() ->
    Existing = chunk_interval_job_for_test(existing_peer, store, 0),
    Inflight = (chunk_interval_job_for_test(inflight_peer, store, 0))#discovery_job{
        pid = self()
    },
    Incoming = chunk_interval_job_for_test(incoming_peer, store, 0),
    Jobs = #discovery_jobs{
        pending = #{pending_key(Existing#discovery_job.key) => Existing},
        inflight = #{Inflight#discovery_job.key => Inflight},
        max_inflight = 1,
        max_pending = 1
    },
    TrackedPeers = sets:from_list([
        Existing#discovery_job.peer,
        Inflight#discovery_job.peer,
        Incoming#discovery_job.peer
    ]),
    ?assertEqual({full, Jobs},
        enqueue_job(Incoming, Jobs, TrackedPeers)),
    ?assertNot(job_exists(Incoming#discovery_job.key, Jobs)),
    ?assert(job_exists(Existing#discovery_job.key, Jobs)).

%% @doc Selection prefers the least-loaded peer before considering store load.
take_next_job_prefers_less_loaded_peer_test() ->
    Step = ar_sync_cursor:query_range_step_size(),
    Inflight1 = chunk_interval_job_for_test(p1, s1, 0),
    Inflight2 = chunk_interval_job_for_test(p1, s2, Step),
    Inflight3 = chunk_interval_job_for_test(p2, s1, 2 * Step),
    Q1 = chunk_interval_job_for_test(p1, s3, 3 * Step),
    Q2 = chunk_interval_job_for_test(p2, s1, 4 * Step),
    Jobs = #discovery_jobs{
        pending = #{
            pending_key(Q1#discovery_job.key) => Q1,
            pending_key(Q2#discovery_job.key) => Q2
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
    {ok, Selected, _Jobs2} =
        take_next_job(Jobs, PeerLoad, StoreLoad),
    ?assertEqual(p2, Selected#discovery_job.peer).

%% At the job limit, start_jobs/2 leaves pending and inflight work unchanged.
start_jobs_respects_inflight_limit_test() ->
    Limit = ?MIN_CHUNK_INTERVAL_JOBS,
    InflightJobs = [
        chunk_interval_job_for_test({p, N}, {s, N}, N)
        || N <- lists:seq(1, Limit)
    ],
    Inflight = maps:from_list([
        {Job#discovery_job.key, Job#discovery_job{ pid = self() }}
        || Job <- InflightJobs
    ]),
    PendingJob = chunk_interval_job_for_test(
        pending_peer, pending_store, Limit + 1),
    State0 = #state{},
    State = State0#state{
        tracked_peers = sets:from_list([
            Job#discovery_job.peer
            || Job <- [PendingJob | InflightJobs]
        ]),
        jobs = (State0#state.jobs)#{
            chunk_interval := #discovery_jobs{
                max_inflight = Limit,
                inflight = Inflight,
                pending = #{
                    pending_key(PendingJob#discovery_job.key) => PendingJob
                }
            }
        }
    },
    ?assertEqual(State, start_jobs(chunk_interval, State)).

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

get_peer_ranges_for_peers_includes_byte_and_footprint_ranges_test() ->
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
        store_row(chunk_interval, footprint,
            {Partition, Footprint}, Peer, FootprintIntervals),
        ExpectedFootprintIntervals =
            ar_footprint_record:footprint_intervals_to_byte_intervals(
                FootprintIntervals),
        ?assertEqual(
            [
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
            ],
            get_peer_ranges_for_peers(
                test_store, [Peer], Offset, RangeStart, RangeEnd))
    end).

build_peer_ranges_limits_chunk_intervals_to_requested_range_test() ->
    Chunk = ?DATA_CHUNK_SIZE,
    %% The cached range spans four chunks while the query selects its middle two.
    Intervals = ar_intervals:from_list([{4 * Chunk, 0}]),
    Expected = ar_intervals:from_list([{3 * Chunk, Chunk}]),
    PeerRange = #peer_range{
        store_id = test_store,
        offset = Chunk,
        peer = peer,
        intervals = ar_intervals:new(),
        footprint = none
    },
    ?assertEqual([
        #peer_range{
            store_id = test_store,
            offset = Chunk,
            peer = peer,
            intervals = Expected,
            footprint = none
        }
    ], build_peer_ranges(byte, PeerRange, Intervals, Chunk, 3 * Chunk)).

unadvertised_peer_is_omitted_test() ->
    reset_all_caches(),
    Peer = {10, 0, 0, 7, 1984},
    ?assertEqual({[], []}, get_peer_ranges_for_peer(
        ?DEFAULT_MODULE, Peer, ?DATA_CHUNK_SIZE, 0, ?DATA_CHUNK_SIZE)).

missing_chunk_interval_metadata_requests_job_test() ->
    reset_all_caches(),
    Peer = {10, 0, 0, 8, 1984},
    store_row(sync_bucket, byte, 0, Peer, 1.0),
    {PeerRanges, Jobs} = get_peer_ranges_for_peer(
        ?DEFAULT_MODULE, Peer, 0, 0, ?DATA_CHUNK_SIZE),
    ?assertEqual([], PeerRanges),
    ?assertMatch([#discovery_job{ kind = chunk_interval, mode = byte }], Jobs).

%% An aged row is served as {stale, Intervals}: the data is still used while the
%% demand path requests a chunk interval job. A fresh row stays a plain hit.
stale_interval_rows_remain_usable_test() ->
    reset_all_caches(),
    Peer = {10, 0, 0, 1, 1984},
    RangeEnd = ?DATA_CHUNK_SIZE,
    Intervals = ar_intervals:from_list([{RangeEnd, 0}]),
    ByteKey = {byte, 0, Peer},
    store_row(chunk_interval, byte, 0, Peer, Intervals),
    ?assertEqual({ok, Intervals}, get_chunk_intervals(byte, Peer, 0)),
    StaleMs = ar_timer:monotonic_ms()
            - ?CHUNK_INTERVAL_CACHE_TTL_MS - 1,
    ets:insert(?CHUNK_INTERVAL_CACHE_TABLE, {ByteKey, Intervals, StaleMs}),
    ?assertEqual({stale, Intervals}, get_chunk_intervals(byte, Peer, 0)),
    %% A stale byte row remains usable and also requests a chunk interval job.
    store_row(sync_bucket, byte, 0, Peer, 1.0),
    ?assertEqual(
        [#peer_range{
            store_id = ?DEFAULT_MODULE,
            offset = 0,
            peer = Peer,
            intervals = Intervals,
            footprint = none
        }],
        get_peer_ranges_for_peers(?DEFAULT_MODULE, [Peer], 0, 0, RangeEnd)),
    %% Footprint rows age against the same safety floor.
    FpKey = {footprint, {1, 2}, Peer},
    store_row(chunk_interval, footprint, {1, 2}, Peer, Intervals),
    ?assertEqual({ok, Intervals},
        get_chunk_intervals(footprint, Peer, {1, 2})),
    FpStaleMs = ar_timer:monotonic_ms()
            - ?CHUNK_INTERVAL_CACHE_TTL_MS - 1,
    ets:insert(?CHUNK_INTERVAL_CACHE_TABLE, {FpKey, Intervals, FpStaleMs}),
    ?assertEqual({stale, Intervals},
        get_chunk_intervals(footprint, Peer, {1, 2})).

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
    {InSyncBucket, _} = chunk_interval_location_range(footprint, 0),
    {FarAway, _} = chunk_interval_location_range(footprint, 1000),
    store_row(chunk_interval, footprint, InSyncBucket, Peer, Intervals),
    store_row(chunk_interval, footprint, FarAway, Peer, Intervals),
    store_row(sync_bucket, footprint, 0, Peer, 0.5),
    mark_chunk_intervals_stale_on_share_change(
        footprint, Peer, 0, 0.5),
    ?assertMatch({hit, _},
        chunk_interval_lookup(footprint, Peer, InSyncBucket)),
    mark_chunk_intervals_stale_on_share_change(
        footprint, Peer, 0, 0.7),
    ?assertMatch({stale, _},
        chunk_interval_lookup(footprint, Peer, InSyncBucket)),
    ?assertMatch({hit, _},
        chunk_interval_lookup(footprint, Peer, FarAway)).

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
    Location = interval_location(footprint, Offset),
    Intervals = ar_intervals:from_list([{100, 0}]),
    store_row(sync_bucket, footprint,
        sync_bucket(footprint, Offset), Peer, 1.0),
    store_row(chunk_interval, footprint, Location, Peer, Intervals),
    meck:new(ar_http_iface_client, [passthrough]),
    meck:expect(ar_http_iface_client, get_footprints,
            fun(_, _, _) -> {error, timeout} end),
    meck:new(ar_events, [passthrough]),
    try
        ?assertEqual(ok,
            refresh_chunk_intervals(footprint, Peer, test_store, Offset)),
        ?assertEqual(0,
            meck:num_calls(ar_http_iface_client, get_footprints, 3)),
        ?assertEqual(0,
            meck:num_calls(ar_events, send, [sync_discovery, '_'])),
        ?assertEqual({ok, Intervals},
            get_chunk_intervals(footprint, Peer, Location))
    after
        meck:unload(ar_events),
        meck:unload(ar_http_iface_client)
    end.

-endif.

%%% @doc Per-store capacity model for network sync.
%%%
%%% The scheduler stores one opaque collection containing every storage module.
%%% This module owns the store index as well as each store's queue priorities,
%%% peer index, claim accounting, and drain-rate sample.
-module(ar_sync_store).
-test_category([fast]).

-export([new/0,
        admission_headroom/2, claim_limit/2,
        chunks_in_claim/1, claimed_chunks/2, is_claimed/3,
        admit/3, release_claim/3,
        queues_empty/1, queued_count/2, queued_task_count/2,
        queued_tasks/1, queued_tasks/2, claims_empty/1,
        peers/1,
        record_write_completed/2, sample_drain_rates/2, drain_rate/2,
        cache_limit/2, pipeline_limit/2,
        start_dispatch/4, refresh_work/2, finish_dispatch/1,
        best_store/1, pop_work/2,
        add_reservations/2, bind_task/2, start_bound_task/2,
        enqueue_bound_tasks/2, bind_footprint/2,
        unclaimed_intervals/3, has_capacity/2, can_start_bound_task/2,
        fetching_count/2,
        stores_by_peer/1]).
-export_type([state/0, dispatch/0]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_sync.hrl").

-ifdef(AR_TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% Enough queued work to cover transient write variance without letting a slow
%% store monopolize the chunk cache.
-define(WRITE_QUEUE_TARGET_DURATION_MS, 5000).

%% A delayed fetch can occupy five seconds before entering the five-second
%% write queue. One additional second covers strict admission boundaries and
%% scheduler/completion-wave quantization.
-define(ASSIGNMENT_TARGET_DURATION_MS, 11_000).

%% Smooth write-completion bursts over roughly two drain-rate observations.
%% The first continuously backlogged interval seeds the estimate directly.
-define(DRAIN_RATE_ALPHA, 0.5).

%% Measure across at least one production scheduler interval so batched writes
%% are not interpreted as alternating zero-rate and burst-rate samples.
-define(DRAIN_SAMPLE_MIN_MS, 10_000).

%% Keep a small probe available after a zero or very low drain-rate sample.
%% Twenty-five chunks bound recovery work while leaving enough requests to
%% observe whether a stalled store has resumed draining.
-define(MIN_CLAIM_LIMIT, 25).

-record(store_state, {
    store_id,
    %% Persistent work retained across dispatch passes.
    work_queue = gb_sets:new(),
    claimed = ar_intervals:new(),
    %% Concrete tasks that can enter the fetch/write pipeline.
    claimed_chunks = 0,
    %% Whole-footprint credits retained only until a reservation binds.
    reservation_chunks = 0,
    queued_peer_counts = #{},
    queued_task_count = 0,
    completed_writes_since_sample = 0,
    drain_sample_started_ms = undefined,
    drain_sample_starved = false,
    drain_rate = undefined
}).

-opaque state() :: #{term() => #store_state{}}.

%% One store's mutable snapshot during a scheduler dispatch pass.
-record(store_dispatch, {
    state,
    %% Tasks already assigned to peer queues but not yet fetching.
    bound_count = 0,
    fetching_count = 0,
    writing_count = 0,
    cached_chunk_count = 0,
    %% Drain-derived limit for chunks that have reached the local cache.
    cache_limit = 0,
    %% Hard fair share for cached chunks plus fetches that can become cached.
    pipeline_limit = 0,
    disk_ready = false,
    %% Dispatch-pass copy plus bound footprints eligible during this pass.
    work_queue = gb_sets:new(),
    %% Peers offering queued tasks or bound footprint work to this store.
    peers = sets:new()
}).

-opaque dispatch() :: #{term() => #store_dispatch{}}.

%% Field order is dispatch precedence because records use Erlang term ordering.
-record(store_priority, {
    fetching_count,
    active_count,
    store_id
}).

%% Concrete chunks precede bound footprints and queued footprints at the same
%% position.
-record(work_priority, {
    position,
    kind_rank
}).

%%%===================================================================
%%% Persistent state.
%%%===================================================================

%% @doc Return an empty indexed store state.
new() ->
    #{}.

%% @doc Chunks the store may still claim before reaching its queue target.
admission_headroom(StoreID, States) ->
    admission_headroom(get_state(StoreID, States)).

%% @doc Target number of chunks kept ahead of a store. Admission applies it to
%% queued work and dispatch applies it to the fetched/write-stage cache. An
%% unmeasured store starts from a cache-derived bootstrap limit.
claim_limit(StoreID, States) ->
    claim_limit(get_state(StoreID, States)).

claim_limit(#store_state{ drain_rate = undefined }) ->
    bootstrap_limit(ar_sync_deps:chunk_cache_size_limit());
claim_limit(#store_state{ drain_rate = ChunksPerSecond }) ->
    max(?MIN_CLAIM_LIMIT,
        round(ChunksPerSecond * ?WRITE_QUEUE_TARGET_DURATION_MS / 1000)).


%% @doc Return the store capacity reserved by a task or reservation. A
%% footprint reservation claims one complete footprint until it is bound.
chunks_in_claim(#task{}) ->
    1;
chunks_in_claim(#footprint_reservation{} = Reservation) ->
    ar_sync_footprint:claim_size(Reservation).

%% @doc Number of concrete chunks and footprint claims currently held.
claimed_chunks(StoreID, States) ->
    State = get_state(StoreID, States),
    State#store_state.claimed_chunks + State#store_state.reservation_chunks.

%% @doc Return whether Offset belongs to an existing concrete chunk claim.
is_claimed(StoreID, Offset, States) ->
    is_claimed(Offset, get_state(StoreID, States)).

is_claimed(Offset, #store_state{ claimed = Claimed }) ->
    ar_intervals:is_inside(Claimed, Offset + 1).

%% @doc Claim and enqueue a candidate when the store has admission headroom.
%% A zero claim means the concrete chunk was already claimed.
admit(StoreID, Task, States) ->
    State = get_state(StoreID, States),
    case admit(Task, State) of
        {ok, ClaimedChunks, State2} ->
            {ok, ClaimedChunks, put_state(State2, States)};
        blocked ->
            blocked
    end.

admit(#task{ offset = Offset } = Task, State) ->
    case {is_claimed(Offset, State), admission_headroom(State) > 0} of
        {true, _} ->
            {ok, 0, State};
        {false, true} ->
            {ok, 1, enqueue_state(Task, State)};
        {false, false} ->
            blocked
    end;
admit(Reservation, State) ->
    case reservation_headroom(State) > 0 of
        true ->
            ClaimedChunks = chunks_in_claim(Reservation),
            {ok, ClaimedChunks, enqueue_state(Reservation, State)};
        false ->
            blocked
    end.

admission_headroom(#store_state{ queued_task_count = QueuedTaskCount } = State) ->
    max(0, claim_limit(State) - QueuedTaskCount).

reservation_headroom(#store_state{ queued_task_count = QueuedTaskCount,
        reservation_chunks = ReservationChunks } = State) ->
    max(0, claim_limit(State) - QueuedTaskCount - ReservationChunks).

%% @doc Release one concrete chunk claim after its task finishes.
release_claim(StoreID, Offset, States) ->
    State = get_state(StoreID, States),
    State2 = State#store_state{
        claimed = ar_intervals:delete(
            State#store_state.claimed, Offset + ?DATA_CHUNK_SIZE, Offset),
        claimed_chunks = max(0, State#store_state.claimed_chunks - 1)
    },
    put_state(State2, States).

%% @doc Return whether the store has no queued tasks.
queues_empty(States) ->
    maps:fold(
        fun(_StoreID, State, Empty) ->
            Empty andalso gb_sets:is_empty(State#store_state.work_queue)
        end,
        true,
        States).

%% @doc Number of tasks currently queued for the store.
queued_count(StoreID, States) ->
    gb_sets:size((get_state(StoreID, States))#store_state.work_queue).

%% @doc Number of concrete chunk tasks queued for StoreID.
queued_task_count(StoreID, States) ->
    (get_state(StoreID, States))#store_state.queued_task_count.

%% @doc Every concrete task currently waiting in a store queue.
queued_tasks(States) ->
    maps:fold(
        fun(_StoreID, State, Tasks) ->
            queued_tasks_in_state(State) ++ Tasks
        end,
        [],
        States).

%% @doc Queued tasks in dispatch order.
queued_tasks(StoreID, States) ->
    queued_tasks_in_state(get_state(StoreID, States)).

%% @doc Return whether the store retains no exact or reserved claims.
claims_empty(States) ->
    maps:fold(
        fun(_StoreID, State, Empty) ->
            Empty andalso claims_empty_for_store(State)
        end,
        true,
        States).

%% @doc Distinct peers named by queued tasks in the store.
peers(States) ->
    PeerSet = maps:fold(
        fun(_StoreID, State, Peers) ->
            maps:fold(
                fun(Peer, _Count, Acc) -> maps:put(Peer, true, Acc) end,
                Peers,
                State#store_state.queued_peer_counts)
        end,
        #{},
        States),
    maps:keys(PeerSet).

%%%===================================================================
%%% Drain-rate sampling.
%%%===================================================================

%% @doc Record one completed store write in the current drain sample.
record_write_completed(StoreID, States) ->
    State = get_state(StoreID, States),
    SampleStartedMs = case State#store_state.drain_sample_started_ms of
        undefined -> ar_timer:monotonic_ms();
        StartedMs -> StartedMs
    end,
    State2 = State#store_state{
        completed_writes_since_sample =
            State#store_state.completed_writes_since_sample + 1,
        drain_sample_started_ms = SampleStartedMs
    },
    put_state(State2, States).

%% @doc Sample completed writes at tick boundaries. If the fetched cache was
%% empty during the sample, observed throughput is demand-limited and a fully
%% consumed queue may grow. A continuously backlogged sample is authoritative
%% in both directions, including zero throughput.
sample_drain_rates(NowMs, States) ->
    maps:map(
        fun(StoreID, State) ->
            CachedChunkCount = ar_sync_deps:chunk_cache_size(StoreID),
            sample_drain_rate(CachedChunkCount, NowMs, State)
        end,
        States).

sample_drain_rate(CachedChunkCount, NowMs, State) ->
    #store_state{
        completed_writes_since_sample = CompletedWrites,
        drain_sample_started_ms = SampleStartedMs,
        drain_sample_starved = SampleStarved
    } = State,
    case SampleStartedMs of
        undefined when CachedChunkCount > 0 ->
            State#store_state{
                drain_sample_started_ms = NowMs
            };
        undefined ->
            State;
        _ when NowMs - SampleStartedMs >= ?DRAIN_SAMPLE_MIN_MS ->
            ElapsedMs = NowMs - SampleStartedMs,
            Sample = CompletedWrites * 1000 / ElapsedMs,
            Starved = SampleStarved orelse CachedChunkCount =:= 0,
            DrainRate2 = update_drain_rate(
                not Starved, CompletedWrites, Sample,
                State#store_state.drain_rate),
            State#store_state{
                completed_writes_since_sample = 0,
                drain_sample_started_ms = NowMs,
                drain_sample_starved = CachedChunkCount =:= 0,
                drain_rate = DrainRate2
            };
        _ ->
            State#store_state{
                drain_sample_starved =
                    SampleStarved orelse CachedChunkCount =:= 0
            }
    end.

update_drain_rate(true, _CompletedWrites, Sample, undefined) ->
    Sample;
update_drain_rate(true, _CompletedWrites, Sample, DrainRate)
        when Sample >= DrainRate ->
    Sample;
update_drain_rate(true, _CompletedWrites, Sample, DrainRate) ->
    arweave_util:ema(DrainRate, Sample, ?DRAIN_RATE_ALPHA);
update_drain_rate(false, CompletedWrites, Sample, undefined)
        when CompletedWrites > 0 ->
    Sample;
update_drain_rate(false, CompletedWrites, Sample, DrainRate)
        when CompletedWrites > 0 ->
    max(DrainRate, Sample);
update_drain_rate(false, 0, _Sample, DrainRate) ->
    DrainRate.

%% @doc Latest measured drain rate in chunks per second, or undefined.
drain_rate(StoreID, States) ->
    (get_state(StoreID, States))#store_state.drain_rate.

%% @doc Per-store share of the hard fetched-chunk cache. A measured store is
%% bounded by its write-rate-derived target; an unmeasured store starts with a
%% small probe.
cache_limit(StoreID, States) ->
    State = get_state(StoreID, States),
    do_cache_limit(State, configured_store_count()).

%% @doc Per-store hard bound covering cached chunks plus network fetches.
pipeline_limit(StoreID, States) ->
    State = get_state(StoreID, States),
    do_pipeline_limit(State, configured_store_count()).

do_cache_limit(State, StoreCount) ->
    FairShare = fair_share(StoreCount),
    min(FairShare, initial_cache_limit(State)).

do_pipeline_limit(State, StoreCount) ->
    min(fair_share(StoreCount), assignment_pipeline_limit(State)).

fair_share(StoreCount) ->
    max(1, ar_sync_deps:chunk_cache_size_limit() div StoreCount).

configured_store_count() ->
    max(1, length(arweave_config:storage_modules())).

initial_cache_limit(#store_state{ drain_rate = undefined }) ->
    ?MIN_CLAIM_LIMIT;
initial_cache_limit(State) ->
    claim_limit(State).

assignment_pipeline_limit(#store_state{ drain_rate = undefined }) ->
    ?MIN_CLAIM_LIMIT;
assignment_pipeline_limit(#store_state{ drain_rate = ChunksPerSecond }) ->
    max(?MIN_CLAIM_LIMIT, round(ChunksPerSecond
        * ?ASSIGNMENT_TARGET_DURATION_MS / 1000)).

%%%===================================================================
%%% Dispatch snapshot.
%%%===================================================================

%% @doc Snapshot every indexed store, existing task, and pending footprint
%% footprint work for one scheduler dispatch pass.
start_dispatch(Footprints, Tasks, BoundTasks, States) ->
    StoreCount = configured_store_count(),
    Dispatches = maps:map(
        fun(StoreID, State) ->
            new_store_dispatch(StoreID, State,
                fair_share(StoreCount))
        end,
        States),
    Dispatches2 = add_reservations(Footprints, Dispatches),
    Dispatches3 = lists:foldl(
        fun(#task{ store_id = StoreID }, Acc) ->
            record_task(StoreID, bound, Acc)
        end,
        Dispatches2,
        BoundTasks),
    maps:fold(
        fun(_TaskRef, #task{ state = TaskState, store_id = StoreID }, Acc)
                when TaskState =:= fetching; TaskState =:= writing ->
                record_task(StoreID, TaskState, Acc);
            (_TaskRef, _Task, Acc) ->
                Acc
        end,
        Dispatches3,
        Tasks).

%% @doc Rebuild dispatch-local work after activation frees peer-queue slots.
%% Only successfully bound tasks were removed from persistent store state; work
%% skipped earlier in the pass becomes selectable again for the refill.
refresh_work(Footprints, Dispatches) ->
    Dispatches2 = maps:map(
        fun(_StoreID, #store_dispatch{ state = State } = Dispatch) ->
            Dispatch#store_dispatch{
                work_queue = State#store_state.work_queue,
                peers = sets:from_list(
                    maps:keys(State#store_state.queued_peer_counts))
            }
        end,
        Dispatches),
    add_reservations(Footprints, Dispatches2).

-ifdef(AR_TEST).
%% @doc Build a store dispatch without existing tasks or footprint work.
test_dispatch(States) ->
    start_dispatch(ar_sync_footprint:new(), #{}, [], States).
-endif.

%% @doc Return the indexed persistent state after a dispatch pass.
finish_dispatch(Dispatches) ->
    maps:map(
        fun(_StoreID, #store_dispatch{ state = State }) -> State end,
        Dispatches).

%% @doc Include an existing task in one store's dispatch load.
record_task(StoreID, TaskState, Dispatches) ->
    Dispatch = get_dispatch(StoreID, Dispatches),
    put_dispatch(do_record_task(TaskState, Dispatch), Dispatches).

do_record_task(fetching, #store_dispatch{ fetching_count = Count } = Dispatch) ->
    Dispatch#store_dispatch{ fetching_count = Count + 1 };
do_record_task(bound, #store_dispatch{ bound_count = Count } = Dispatch) ->
    Dispatch#store_dispatch{ bound_count = Count + 1 };
do_record_task(writing, #store_dispatch{ writing_count = Count } = Dispatch) ->
    Dispatch#store_dispatch{ writing_count = Count + 1 }.

%% @doc Return fetching load and total active load for store selection.
load(#store_dispatch{ bound_count = BoundCount,
        fetching_count = FetchingCount,
        writing_count = WritingCount }) ->
    NetworkCount = BoundCount + FetchingCount,
    {NetworkCount, NetworkCount + WritingCount}.

%% @doc Return whether the store can start another network fetch within its
%% write-stage limits.
has_capacity(#store_dispatch{
        state = State,
        bound_count = BoundCount,
        fetching_count = FetchingCount,
        cached_chunk_count = CachedChunkCount,
        cache_limit = CacheLimit,
        pipeline_limit = PipelineLimit,
        disk_ready = DiskReady
    }) ->
    NetworkCount = BoundCount + FetchingCount,
    DiskReady andalso CachedChunkCount < CacheLimit
        andalso CachedChunkCount + NetworkCount < PipelineLimit
        andalso initial_fetch_has_capacity(NetworkCount, State).

initial_fetch_has_capacity(FetchingCount,
        #store_state{ drain_rate = undefined }) ->
    FetchingCount < ?MIN_CLAIM_LIMIT;
initial_fetch_has_capacity(_FetchingCount, #store_state{}) ->
    true.

%% @doc Return whether one store can start another network fetch this pass.
has_capacity(StoreID, Dispatches) ->
    has_capacity(get_dispatch(StoreID, Dispatches)).

%% @doc Return whether an already-admitted peer-bound task may start. Moving a
%% task from bound to fetching does not add store pipeline load.
can_start_bound_task(StoreID, Dispatches) ->
    #store_dispatch{
        cached_chunk_count = CachedChunkCount,
        cache_limit = CacheLimit,
        disk_ready = DiskReady
    } = get_dispatch(StoreID, Dispatches),
    DiskReady andalso CachedChunkCount < CacheLimit.

%% @doc Return one destination store's active network-fetch count.
fetching_count(StoreID, Dispatches) ->
    (get_dispatch(StoreID, Dispatches))#store_dispatch.fetching_count.

%% @doc Select the least-loaded store with runnable work and capacity.
best_store(Dispatches) ->
    case maps:fold(fun do_best_store/3, none, Dispatches) of
        none -> none;
        {_Priority, StoreID} -> {ok, StoreID}
    end.

do_best_store(StoreID, Dispatch, Selected) ->
    case has_capacity(Dispatch) andalso has_ready_work(Dispatch) of
        false ->
            Selected;
        true ->
            Priority = store_priority(StoreID, Dispatch),
            case Selected of
                none -> {Priority, StoreID};
                {SelectedPriority, _SelectedStoreID}
                        when Priority < SelectedPriority ->
                    {Priority, StoreID};
                _ -> Selected
            end
    end.

%% @doc Pop the highest-priority work remaining in this dispatch pass.
pop_work(StoreID, Dispatches) ->
    Dispatch = get_dispatch(StoreID, Dispatches),
    case do_pop_work(Dispatch) of
        none ->
            none;
        {Work, Dispatch2} ->
            {Work, put_dispatch(Dispatch2, Dispatches)}
    end.

do_pop_work(#store_dispatch{ work_queue = WorkQueue } = Dispatch) ->
    case gb_sets:is_empty(WorkQueue) of
        true ->
            none;
        false ->
            {Priority, Work} = gb_sets:smallest(WorkQueue),
            {Work, Dispatch#store_dispatch{ work_queue =
                gb_sets:delete({Priority, Work}, WorkQueue) }}
    end.

%% @doc Add reservations with remaining intervals to this dispatch pass without
%% persisting second copies in their store queues.
add_reservations(Reservations, Dispatches) ->
    PendingReservations = ar_sync_footprint:pending_reservations(Reservations),
    lists:foldl(fun do_add_reservation/2, Dispatches, PendingReservations).

do_add_reservation(Reservation, Dispatches) ->
    StoreID = ar_sync_footprint:store_id(Reservation),
    Dispatch = get_dispatch(StoreID, Dispatches),
    WorkQueue = Dispatch#store_dispatch.work_queue,
    Peers = add_source_peers(
        ar_sync_footprint:sources(Reservation), Dispatch#store_dispatch.peers),
    put_dispatch(Dispatch#store_dispatch{
        work_queue = gb_sets:add_element(work_element(Reservation), WorkQueue),
        peers = Peers
    }, Dispatches).

%% @doc Move one selected task from the unbound store queue into a peer queue.
%% It remains queued for admission accounting until a fetch actually starts.
bind_task(Task, Dispatches) ->
    StoreID = Task#task.store_id,
    Dispatch = get_dispatch(StoreID, Dispatches),
    #store_dispatch{ state = State, bound_count = BoundCount } = Dispatch,
    put_dispatch(Dispatch#store_dispatch{
        state = bind_queued_task(Task, State),
        bound_count = BoundCount + 1
    }, Dispatches).

%% @doc Claim footprint children directly into their selected peer queue.
enqueue_bound_tasks(Tasks, Dispatches) ->
    lists:foldl(fun do_enqueue_bound_task/2, Dispatches, Tasks).

do_enqueue_bound_task(Task, Dispatches) ->
    StoreID = Task#task.store_id,
    Dispatch = get_dispatch(StoreID, Dispatches),
    #store_dispatch{ state = State, bound_count = BoundCount } = Dispatch,
    put_dispatch(Dispatch#store_dispatch{
        state = enqueue_bound_task(Task, State),
        bound_count = BoundCount + 1
    }, Dispatches).

%% @doc Move one peer-bound task into the active network-fetch stage.
start_bound_task(Task, Dispatches) ->
    StoreID = Task#task.store_id,
    Dispatch = get_dispatch(StoreID, Dispatches),
    #store_dispatch{
        state = State,
        bound_count = BoundCount,
        fetching_count = FetchingCount
    } = Dispatch,
    State2 = State#store_state{
        queued_task_count = max(0, State#store_state.queued_task_count - 1)
    },
    put_dispatch(Dispatch#store_dispatch{
        state = State2,
        bound_count = max(0, BoundCount - 1),
        fetching_count = FetchingCount + 1
    }, Dispatches).

%% @doc Bind a reservation and release its persistent whole-footprint claim.
bind_footprint(Reservation, Dispatches) ->
    StoreID = ar_sync_footprint:store_id(Reservation),
    Dispatch = get_dispatch(StoreID, Dispatches),
    State = Dispatch#store_dispatch.state,
    State2 = dequeue(Reservation, State),
    put_dispatch(Dispatch#store_dispatch{ state = State2#store_state{
        reservation_chunks = max(0,
            State2#store_state.reservation_chunks
                - chunks_in_claim(Reservation)) }
    }, Dispatches).

%% @doc Return chunks in Intervals not already claimed by this dispatch.
unclaimed_intervals(StoreID, Intervals, Dispatches) ->
    Dispatch = get_dispatch(StoreID, Dispatches),
    do_unclaimed_intervals(Intervals, Dispatch#store_dispatch.state).

%% @doc Return stores with capacity and pending work, indexed by source peer.
stores_by_peer(Dispatches) ->
    maps:fold(
        fun(StoreID, Dispatch, StoresByPeer) ->
            case has_capacity(Dispatch) of
                false ->
                    StoresByPeer;
                true ->
                    sets:fold(
                        fun(Peer, Acc) ->
                            maps:update_with(Peer,
                                fun(StoreIDs) -> [StoreID | StoreIDs] end,
                                [StoreID], Acc)
                        end,
                        StoresByPeer,
                        Dispatch#store_dispatch.peers)
            end
        end,
        #{},
        Dispatches).

%%%===================================================================
%%% Internal queue and claim operations.
%%%===================================================================

bootstrap_limit(ChunkCacheLimit) ->
    max(?MIN_CLAIM_LIMIT, ChunkCacheLimit div 4).

get_state(StoreID, States) ->
    maps:get(StoreID, States, #store_state{ store_id = StoreID }).

put_state(#store_state{ store_id = StoreID } = State, States) ->
    maps:put(StoreID, State, States).

get_dispatch(StoreID, Dispatches) ->
    case maps:find(StoreID, Dispatches) of
        {ok, Dispatch} -> Dispatch;
        error ->
            StoreCacheLimit = fair_share(configured_store_count()),
            new_store_dispatch(StoreID,
                #store_state{ store_id = StoreID }, StoreCacheLimit)
    end.

put_dispatch(#store_dispatch{ state = #store_state{ store_id = StoreID } } = Dispatch,
        Dispatches) ->
    maps:put(StoreID, Dispatch, Dispatches).

new_store_dispatch(StoreID, State, StorePipelineLimit) ->
    #store_dispatch{
        state = State,
        cached_chunk_count = ar_sync_deps:chunk_cache_size(StoreID),
        cache_limit = min(StorePipelineLimit, initial_cache_limit(State)),
        pipeline_limit = min(
            StorePipelineLimit, assignment_pipeline_limit(State)),
        disk_ready = ar_sync_deps:is_disk_space_sufficient(StoreID) =:= true,
        work_queue = State#store_state.work_queue,
        peers = sets:from_list(maps:keys(State#store_state.queued_peer_counts))
    }.

claims_empty_for_store(#store_state{
        claimed = Claimed,
        claimed_chunks = ClaimedChunks,
        reservation_chunks = ReservationChunks
    }) ->
    ClaimedChunks =:= 0 andalso ReservationChunks =:= 0
        andalso ar_intervals:is_empty(Claimed).

enqueue_state(#task{ offset = Offset } = Task, State) ->
    enqueue_task(Task, State#store_state{
        claimed = ar_intervals:add(
            State#store_state.claimed, Offset + ?DATA_CHUNK_SIZE, Offset),
        claimed_chunks = State#store_state.claimed_chunks + 1,
        queued_task_count = State#store_state.queued_task_count + 1
    });
enqueue_state(Reservation, State) ->
    enqueue_task(Reservation, State#store_state{
        reservation_chunks = State#store_state.reservation_chunks
            + chunks_in_claim(Reservation)
    }).

dequeue(#task{} = Task, State) ->
    State2 = bind_queued_task(Task, State),
    State2#store_state{
        queued_task_count = max(0, State2#store_state.queued_task_count - 1)
    };
dequeue(Reservation, State) ->
    State#store_state{
        work_queue = gb_sets:delete(
            work_element(Reservation), State#store_state.work_queue),
        queued_peer_counts = adjust_peer_counts(
            sources(Reservation), -1, State#store_state.queued_peer_counts)
    }.

bind_queued_task(#task{ sources = Sources } = Task, State) ->
    State#store_state{
        work_queue = gb_sets:delete(
            work_element(Task), State#store_state.work_queue),
        queued_peer_counts = adjust_peer_counts(
            Sources, -1, State#store_state.queued_peer_counts)
    }.

enqueue_bound_task(#task{ offset = Offset }, State) ->
    State#store_state{
        claimed = ar_intervals:add(
            State#store_state.claimed, Offset + ?DATA_CHUNK_SIZE, Offset),
        claimed_chunks = State#store_state.claimed_chunks + 1,
        queued_task_count = State#store_state.queued_task_count + 1
    }.

do_unclaimed_intervals(Intervals, #store_state{ claimed = Claimed }) ->
    ar_intervals:fold(
        fun({End, Start}, Acc) ->
            add_unclaimed_intervals(Start, End, Claimed, Acc)
        end,
        ar_intervals:new(),
        Intervals).

enqueue_task(Task, State) ->
    State#store_state{
        work_queue = gb_sets:add_element(
            work_element(Task), State#store_state.work_queue),
        queued_peer_counts = adjust_peer_counts(
            sources(Task), 1, State#store_state.queued_peer_counts)
    }.

queued_tasks_in_state(#store_state{ work_queue = WorkQueue }) ->
    lists:filtermap(
        fun({_Priority, #task{} = Task}) -> {true, Task};
            (_) -> false
        end,
        gb_sets:to_list(WorkQueue)).

sources(#task{ sources = Sources }) ->
    Sources;
sources(#footprint_reservation{} = Reservation) ->
    ar_sync_footprint:sources(Reservation).

work_element(#task{} = Work) ->
    {#work_priority{
        position = work_position(Work),
        kind_rank = work_kind_rank(Work)
    }, Work};
work_element(#footprint_reservation{} = Reservation) ->
    {#work_priority{
        position = work_position(Reservation),
        kind_rank = work_kind_rank(Reservation)
    }, ar_sync_footprint:key(Reservation)}.

work_position(#task{ footprint = #footprint{
        footprint = Footprint } }) ->
    Footprint;
work_position(#task{ footprint = none, offset = Offset }) ->
    BlockSize = ?PARTITION_SIZE div
        ar_replica_2_9:get_footprints_per_partition(),
    (Offset rem ?PARTITION_SIZE) div BlockSize;
work_position(#footprint_reservation{} = Reservation) ->
    ar_sync_footprint:sort_key(Reservation).

work_kind_rank(#task{}) ->
    0;
work_kind_rank(#footprint_reservation{} = Reservation) ->
    ar_sync_footprint:queue_rank(Reservation).

store_priority(StoreID, Dispatch) ->
    {FetchingCount, ActiveCount} = load(Dispatch),
    #store_priority{
        fetching_count = FetchingCount,
        active_count = ActiveCount,
        store_id = StoreID
    }.

has_ready_work(#store_dispatch{ work_queue = WorkQueue }) ->
    not gb_sets:is_empty(WorkQueue).

adjust_peer_counts(Sources, Delta, PeerCounts) ->
    PeerCounts2 = lists:foldl(
        fun(#task_source{ peer = Peer }, Acc) ->
            maps:update_with(Peer, fun(N) -> N + Delta end, Delta, Acc)
        end,
        PeerCounts,
        Sources),
    maps:filter(fun(_Peer, Count) -> Count > 0 end, PeerCounts2).

add_source_peers(Sources, Peers) ->
    lists:foldl(
        fun(#task_source{ peer = Peer }, Acc) ->
            sets:add_element(Peer, Acc)
        end,
        Peers,
        Sources).

add_unclaimed_intervals(Start, End, _Claimed, Intervals) when Start >= End ->
    Intervals;
add_unclaimed_intervals(Offset, End, Claimed, Intervals) ->
    Next = Offset + ?DATA_CHUNK_SIZE,
    ChunkInterval = ar_intervals:from_list([{Next, Offset}]),
    OverlapsClaim = not ar_intervals:is_empty(
        ar_intervals:intersection(Claimed, ChunkInterval)),
    OverlapsCandidate = not ar_intervals:is_empty(
        ar_intervals:intersection(Intervals, ChunkInterval)),
    Intervals2 = case OverlapsClaim orelse OverlapsCandidate of
        true -> Intervals;
        false -> ar_intervals:add(Intervals, Next, Offset)
    end,
    add_unclaimed_intervals(Next, End, Claimed, Intervals2).

%%%===================================================================
%%% Tests.
%%%===================================================================

-ifdef(AR_TEST).

capacity_limits_are_chunk_granular_test() ->
    %% A 100-chunk footprint keeps the limit arithmetic explicit.
    ar_replica_2_9:override_entropy_size(
        100 * ?DATA_CHUNK_SIZE div ?SUB_CHUNK_COUNT),
    try
        %% One quarter of a 2000-chunk cache provides 500 bootstrap chunks.
        ?assertEqual(500, bootstrap_limit(2000)),
        %% An unmeasured store starts with the twenty-five-chunk probe.
        ?assertEqual(25, do_cache_limit(#store_state{}, 1)),
        %% Five seconds at 100 chunks/s is 500 chunks.
        ?assertEqual(500,
            claim_limit(#store_state{ drain_rate = 100 })),
        %% Five seconds at 105 chunks/s is 525 chunks without footprint rounding.
        ?assertEqual(525,
            claim_limit(#store_state{ drain_rate = 105 })),
        %% Five seconds at fifteen chunks/s is a seventy-five-chunk cache limit.
        ?assertEqual(75,
            do_cache_limit(#store_state{ drain_rate = 15 }, 1)),
        %% Cached and fetching stages retain eleven seconds, or 165 chunks.
        ?assertEqual(165,
            do_pipeline_limit(#store_state{ drain_rate = 15 }, 1)),
        %% Five seconds at one chunk/s is below the 25-chunk progress probe.
        ?assertEqual(25,
            claim_limit(#store_state{ drain_rate = 1 }))
    after
        ar_replica_2_9:reset_all_overrides()
    end.

admission_headroom_counts_only_queued_tasks_test() ->
    %% One chunk/s resolves to the twenty-five-chunk progress floor. Concrete
    %% claims already fetching or writing remain for deduplication but do not
    %% consume future queue capacity.
    State = #store_state{
        drain_rate = 1,
        claimed_chunks = 25,
        queued_task_count = 4
    },
    ?assertEqual(21, admission_headroom(State)).

fetching_tasks_do_not_consume_store_cache_limit_test() ->
    %% Once drain is measured, peer and global limits bound network requests;
    %% the store cache limit applies only after chunks enter its cache.
    Dispatch = #store_dispatch{
        state = #store_state{ drain_rate = 1 },
        cached_chunk_count = 3,
        fetching_count = 10,
        cache_limit = 4,
        pipeline_limit = 14,
        disk_ready = true
    },
    ?assert(has_capacity(Dispatch)),
    ?assertNot(has_capacity(Dispatch#store_dispatch{
        cached_chunk_count = 4
    })),
    ?assertNot(has_capacity(Dispatch#store_dispatch{
        fetching_count = 11
    })).

unmeasured_store_limits_initial_fetch_probe_test() ->
    Dispatch = #store_dispatch{
        state = #store_state{},
        fetching_count = ?MIN_CLAIM_LIMIT - 1,
        cache_limit = 500,
        pipeline_limit = 500,
        disk_ready = true
    },
    ?assert(has_capacity(Dispatch)),
    ?assertNot(has_capacity(Dispatch#store_dispatch{
        fetching_count = ?MIN_CLAIM_LIMIT
    })).

footprint_claim_reserves_complete_footprint_test() ->
    Footprint = #footprint{ store_id = store1, partition = 0, footprint = 1 },
    OneChunk = ar_intervals:from_list([{?DATA_CHUNK_SIZE, 0}]),
    ThreeChunks = ar_intervals:from_list([
        {2 * ?DATA_CHUNK_SIZE + 1, 0}
    ]),
    Sources = [
        #task_source{ peer = peer1, footprint = Footprint,
            intervals = OneChunk },
        #task_source{ peer = peer2, footprint = Footprint,
            intervals = ThreeChunks }
    ],
    Reservation = ar_sync_footprint:new_reservation(store1, Footprint, Sources),
    %% Task-source coverage is unknown until dispatch binds the reservation, so admission
    %% reserves the complete footprint rather than either advertised subset.
    ChunkCacheLimit = ar_sync_deps:chunk_cache_size_limit(),
    InitialClaimedChunks = bootstrap_limit(ChunkCacheLimit) - 1,
    InitialState = put_state(#store_state{
        store_id = store1,
        claimed_chunks = InitialClaimedChunks,
        queued_task_count = InitialClaimedChunks
    }, new()),
    {ok, ClaimedChunks, State} = admit(store1, Reservation, InitialState),
    FootprintChunks = ar_sync_footprint:claim_size(Reservation),
    ?assertEqual(FootprintChunks, chunks_in_claim(Reservation)),
    ?assertEqual(FootprintChunks, ClaimedChunks),
    ?assertEqual(InitialClaimedChunks + FootprintChunks,
        claimed_chunks(store1, State)),
    ?assertEqual(1, queued_count(store1, State)),
    ?assertEqual(InitialClaimedChunks, queued_task_count(store1, State)),
    %% Reservation credit does not consume the one remaining concrete task of
    %% headroom; exact claims arbitrate overlap when the footprint binds.
    Task = #task{ offset = ?DATA_CHUNK_SIZE,
        sources = [#task_source{ peer = peer1 }] },
    {ok, 1, State2} = admit(store1, Task, State),
    ?assertEqual(InitialClaimedChunks + 1,
        queued_task_count(store1, State2)),
    %% A reservation may exceed the final positive headroom, but its complete
    %% footprint charge prevents another reservation from being admitted.
    Reservation2 = ar_sync_footprint:new_reservation(
        store1, Footprint#footprint{ footprint = 2 }, Sources),
    ?assertEqual(blocked, admit(store1, Reservation2, State2)).

dispatch_best_store_orders_by_load_test() ->
    %% The first dispatch has no active tasks. The next two have one fetch each,
    %% with the writing task making store_c the more heavily loaded tie.
    Task = #task{ offset = 0,
        sources = [#task_source{ peer = peer }] },
    WorkQueue = gb_sets:singleton(work_element(Task)),
    StoreA = #store_dispatch{ state = #store_state{ store_id = store_a },
        disk_ready = true, cache_limit = 100, pipeline_limit = 100,
        work_queue = WorkQueue },
    StoreB = #store_dispatch{ state = #store_state{ store_id = store_b },
        disk_ready = true, cache_limit = 100, pipeline_limit = 100,
        work_queue = WorkQueue, fetching_count = 1 },
    StoreC = #store_dispatch{ state = #store_state{ store_id = store_c },
        fetching_count = 1, cache_limit = 100, pipeline_limit = 100,
        writing_count = 1, disk_ready = true, work_queue = WorkQueue },
    Dispatches = #{store_c => StoreC, store_b => StoreB, store_a => StoreA},
    {ok, store_a} = best_store(Dispatches),
    {ok, store_b} = best_store(maps:remove(store_a, Dispatches)),
    {ok, store_c} = best_store(maps:remove(store_b,
        maps:remove(store_a, Dispatches))).

stores_by_peer_includes_task_and_footprint_demand_test() ->
    StoreID = store1,
    TaskPeer = task_peer,
    FootprintPeer = footprint_peer,
    Task = #task{
        offset = 0,
        store_id = StoreID,
        sources = [#task_source{ peer = TaskPeer }]
    },
    {ok, 1, States} = admit(StoreID, Task, new()),
    %% The first footprint and its first chunk are enough to represent bound
    %% footprint demand; no active child is required while intervals remain.
    Footprint = #footprint{
        store_id = StoreID,
        partition = 0,
        footprint = 0
    },
    Source = #task_source{
        peer = FootprintPeer,
        footprint = Footprint,
        intervals = ar_intervals:from_list([{?DATA_CHUNK_SIZE, 0}])
    },
    Reservation = ar_sync_footprint:test_reservation(
        StoreID, Footprint, [Source], FootprintPeer, 0, bound),
    Footprints = ar_sync_footprint:test_state([Reservation]),
    Dispatches0 = start_dispatch(Footprints, #{}, [], States),
    Dispatch = maps:get(StoreID, Dispatches0),
    Dispatches = #{StoreID => Dispatch#store_dispatch{ disk_ready = true }},
    ?assertEqual(#{
        TaskPeer => [StoreID],
        FootprintPeer => [StoreID]
    }, stores_by_peer(Dispatches)),
    BlockedDispatches = #{StoreID => Dispatch#store_dispatch{
        disk_ready = false
    }},
    ?assertEqual(#{}, stores_by_peer(BlockedDispatches)).

pop_work_returns_queued_task_test() ->
    Task = #task{ offset = 0,
        sources = [#task_source{ peer = peer }] },
    {ok, 1, State} = admit(store1, Task, new()),
    Dispatch = test_dispatch(State),
    ?assertMatch({#task{}, #{}}, pop_work(store1, Dispatch)).

work_removed_from_a_pass_returns_in_the_next_pass_test() ->
    First = #task{ offset = 0,
        sources = [#task_source{ peer = peer_a }] },
    Second = #task{ offset = ?DATA_CHUNK_SIZE,
        sources = [#task_source{ peer = peer_b }] },
    {ok, 1, State1} = admit(store1, First, new()),
    {ok, 1, State2} = admit(store1, Second, State1),
    Dispatch0 = test_dispatch(State2),
    {First, Dispatch1} = pop_work(store1, Dispatch0),
    %% Leaving blocked work out of the dispatch-local queue exposes the next
    %% item without changing persistent queued work.
    {Second, _Dispatch2} = pop_work(store1, Dispatch1),
    %% A new pass is rebuilt from persistent state, so the first task is ready
    %% again rather than carrying blocked state across scheduler callbacks.
    NewDispatch = test_dispatch(State2),
    {First, _NewDispatch2} = pop_work(store1, NewDispatch).

drain_rate_uses_tick_samples_test() ->
    SampleStartMs = 1_000,
    SampleIntervalMs = 10_000,
    InitialRate = 100,
    State0 = #store_state{ drain_rate = InitialRate,
        drain_sample_started_ms = SampleStartMs,
        completed_writes_since_sample = 200 },

    %% Twenty chunks/s without pending writes is only a lower bound, so it
    %% cannot reduce the existing 100-chunk/s estimate.
    State1 = sample_drain_rate(
        0, SampleStartMs + SampleIntervalMs, State0),
    ?assertEqual(100, State1#store_state.drain_rate),

    %% The same twenty-chunk/s sample with pending writes is authoritative.
    %% The half-weight EMA moves the estimate from 100 to 60 chunks/s.
    State2 = sample_drain_rate(1, SampleStartMs + 2 * SampleIntervalMs,
        State1#store_state{ completed_writes_since_sample = 200,
            drain_sample_starved = false }),
    ?assertEqual(60.0, State2#store_state.drain_rate),
    ?assertEqual(300, claim_limit(State2)),

    %% Eight hundred completions with no pending writes prove at least eighty
    %% chunks/s and raise the lower bound directly.
    State3 = sample_drain_rate(0, SampleStartMs + 3 * SampleIntervalMs,
        State2#store_state{ completed_writes_since_sample = 800 }),
    ?assertEqual(80.0, State3#store_state.drain_rate),

    %% A higher authoritative sample takes effect immediately so a recovered
    %% store is not held to its earlier capacity for several long windows.
    IncreasedState = sample_drain_rate(1,
        SampleStartMs + 4 * SampleIntervalMs,
        State3#store_state{ completed_writes_since_sample = 1000,
            drain_sample_starved = false }),
    ?assertEqual(100.0, IncreasedState#store_state.drain_rate),

    %% A fully idle interval carries no new capacity evidence.
    State4 = sample_drain_rate(
        0, SampleStartMs + 5 * SampleIntervalMs, IncreasedState),
    ?assertEqual(100.0, State4#store_state.drain_rate),

    %% Pending writes with no completions provide a zero-rate sample, moving
    %% the half-weight EMA down to forty chunks/s.
    State5 = sample_drain_rate(1, SampleStartMs + 6 * SampleIntervalMs,
        State4#store_state{ drain_sample_starved = false }),
    ?assertEqual(50.0, State5#store_state.drain_rate).

-endif.

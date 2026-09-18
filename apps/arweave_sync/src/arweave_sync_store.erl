%%% @doc Per-store capacity model for network sync.
%%%
%%% The scheduler stores one opaque collection containing every store.
%%% This module owns the store index as well as each store's queue priorities,
%%% peer index, claim accounting, and drain-rate sample.
-module(arweave_sync_store).

-ifdef(AR_TEST).
%% Focused tests live outside the production module.
-export([
    admission_headroom/1,
    bootstrap_limit/1,
    claim_limit/1,
    do_cache_limit/2,
    do_pipeline_limit/2,
    has_capacity/1,
    put_state/2,
    sample_drain_rate/3,
    test_dispatch/1,
    work_element/1
]).
-endif.

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
-include_lib("arweave_sync/include/arweave_sync.hrl").

-include("arweave_sync_store.hrl").

-opaque state() :: #{term() => #store_state{}}.

-opaque dispatch() :: #{term() => #store_dispatch{}}.

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
    bootstrap_limit((arweave_sync_deps:chunk_cache()):limit());
claim_limit(#store_state{ drain_rate = ChunksPerSecond }) ->
    max(?MIN_CLAIM_LIMIT,
        round(ChunksPerSecond * ?WRITE_QUEUE_TARGET_DURATION_MS / 1000)).

%% @doc Return the store capacity reserved by a task or reservation. A
%% footprint reservation claims one complete footprint until it is bound.
chunks_in_claim(#task{}) ->
    1;
chunks_in_claim(#footprint_reservation{} = Reservation) ->
    arweave_sync_footprint:claim_size(Reservation).

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
    case (arweave_sync_deps:chunk_cache()):completed(StoreID) of
        undefined -> do_record_write_completed(StoreID, States);
        _ -> States
    end.

do_record_write_completed(StoreID, States) ->
    State = get_state(StoreID, States),
    SampleStartedMs = case State#store_state.drain_sample_started_ms of
        undefined -> (arweave_sync_deps:clock()):monotonic_ms();
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
            CachedChunkCount = (arweave_sync_deps:chunk_cache()):cached_size(StoreID),
            State2 = observe_writes(StoreID, State),
            sample_drain_rate(CachedChunkCount, NowMs, State2)
        end,
        States).

observe_writes(StoreID, State) ->
    case (arweave_sync_deps:chunk_cache()):completed(StoreID) of
        undefined -> State;
        Count ->
            Previous = State#store_state.observed_write_count,
            Completed = case Previous of
                undefined -> 0;
                _ -> max(0, Count - Previous)
            end,
            State#store_state{observed_write_count = Count,
                completed_writes_since_sample =
                    State#store_state.completed_writes_since_sample + Completed}
    end.

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
    max(1, (arweave_sync_deps:chunk_cache()):limit() div StoreCount).

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
    start_dispatch(arweave_sync_footprint:new(), #{}, [], States).
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
    PendingReservations = arweave_sync_footprint:pending_reservations(Reservations),
    lists:foldl(fun do_add_reservation/2, Dispatches, PendingReservations).

do_add_reservation(Reservation, Dispatches) ->
    StoreID = arweave_sync_footprint:store_id(Reservation),
    Dispatch = get_dispatch(StoreID, Dispatches),
    WorkQueue = Dispatch#store_dispatch.work_queue,
    Peers = add_source_peers(
        arweave_sync_footprint:sources(Reservation), Dispatch#store_dispatch.peers),
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
    StoreID = arweave_sync_footprint:store_id(Reservation),
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
        cached_chunk_count = (arweave_sync_deps:chunk_cache()):cached_size(StoreID),
        cache_limit = min(StorePipelineLimit, initial_cache_limit(State)),
        pipeline_limit = min(
            StorePipelineLimit, assignment_pipeline_limit(State)),
        disk_ready = (arweave_sync_deps:data_sync()):is_disk_space_sufficient(StoreID) =:= true,
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
    arweave_sync_footprint:sources(Reservation).

work_element(#task{} = Work) ->
    {#work_priority{
        position = work_position(Work),
        kind_rank = work_kind_rank(Work)
    }, Work};
work_element(#footprint_reservation{} = Reservation) ->
    {#work_priority{
        position = work_position(Reservation),
        kind_rank = work_kind_rank(Reservation)
    }, arweave_sync_footprint:key(Reservation)}.

work_position(#task{ footprint = #footprint{
        footprint = Footprint } }) ->
    Footprint;
work_position(#task{ footprint = none, offset = Offset }) ->
    BlockSize = ?PARTITION_SIZE div
        (arweave_sync_deps:constants()):get_replica_2_9_footprints_per_partition(),
    (Offset rem ?PARTITION_SIZE) div BlockSize;
work_position(#footprint_reservation{} = Reservation) ->
    arweave_sync_footprint:sort_key(Reservation).

work_kind_rank(#task{}) ->
    0;
work_kind_rank(#footprint_reservation{} = Reservation) ->
    arweave_sync_footprint:queue_rank(Reservation).

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

%%% @doc Per-store work queues, claims and limits. For each store,
%%% this module tracks a work queue of claimed work, the claimed chunks, and
%%% the store's write rate (chunks written per second). Each chunk of a
%%% store's work moves through four stages:
%%% - queued: claimed for the store and waiting in its work queue;
%%% - bound: in a peer queue, waiting to be fetched from that peer;
%%% - fetching: a fetch worker is requesting it from the peer;
%%% - writing: fetched and held in the chunk cache until the store writes it.
%%%
%%% arweave_sync_scheduler calls this module in a cycle:
%%% 1. Admission (admit/3), for each unit of work the sweeper offers: claim
%%%    its chunks and add it to the work queue, up to the claim limit. A
%%%    footprint reservation claims a whole footprint until it binds to a
%%%    peer; after that, only the chunks handed to the peer are claimed.
%%% 2. Dispatch (snapshot/4 to commit_plan/1), on each dispatch pass:
%%%    snapshot/4 counts each store's bound, fetching and writing chunks and
%%%    sets the store's cache and pipeline limits for the pass. The scheduler
%%%    takes work from the least-loaded store with disk space and room under
%%%    those limits (best_store/1, pop_work/2), and records each bind and each
%%%    started fetch (bind_task/2, bind_footprint/2, enqueue_bound_tasks/2,
%%%    start_bound_task/2). After the pass starts fetches, refresh_work/2 puts
%%%    the work that found no peer back into the pass's work queues so it can
%%%    be bound again.
%%% 3. Task completion (record_write_completed/2, release_claim/3): count each
%%%    write toward the current write sample, and release the task's claim
%%%    once the task finishes.
%%% 4. Tick (sample_write_rates/2): update the write rate from the write
%%%    sample, the writes of the last WRITE_SAMPLE_MIN_MS or more. A write
%%%    sample is driven when fetched chunks were waiting for the store to
%%%    write them the whole time. A driven write sample can raise or lower the
%%%    write rate; any other can only raise it.
%%%
%%% The write rate sets three limits, each at least MIN_CLAIM_LIMIT:
%%% - claim limit (claim_limit/2): limits queued and bound chunks to
%%%   WRITE_QUEUE_TARGET_DURATION_MS of writes.
%%% - cache limit (cache_limit/2): limits the store's chunks in the chunk
%%%   cache to the claim limit.
%%% - pipeline limit (pipeline_limit/3): limits bound and fetching chunks,
%%%   plus the store's chunks in the chunk cache, to
%%%   PIPELINE_TARGET_DURATION_MS of writes.
%%% Until a tick measures the write rate, the claim limit is a quarter of the
%%% chunk cache and the other two are MIN_CLAIM_LIMIT. The store's fair share
%%% of the chunk cache (fair_share/1) caps the cache and pipeline limits.
-module(arweave_sync_store).

-ifdef(AR_TEST).
-export([
    admission_headroom/1,
    bootstrap_limit/1,
    claim_limit/1,
    cache_limit/2,
    do_pipeline_limit/2,
    has_capacity/1,
    put_state/2,
    queued_count/2,
    sample_write_rate/3,
    test_plan/1,
    work_element/1,
    write_rate/2
]).
-endif.

-export([new/0,
        admission_headroom/2, claim_limit/2,
        chunks_in_claim/1, claimed_chunks/2,
        admit/3, release_claim/3,
        queues_empty/1, queued_task_count/2,
        claims_empty/1,
        source_peers/1,
        record_write_completed/2, sample_write_rates/2,
        fair_share/1, pipeline_limit/3,
        snapshot/4, refresh_work/2, commit_plan/1,
        best_store/1, pop_work/2,
        add_reservations/2, bind_task/2, start_bound_task/2,
        enqueue_bound_tasks/2, bind_footprint/2,
        unclaimed_intervals/3, has_capacity/2, rank/2,
        pipeline_limit/2,
        stores_with_room_by_peer/1]).
-export_type([state/0, plan/0]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_sync/include/arweave_sync.hrl").

-include("arweave_sync_store.hrl").
-include_lib("arweave_sync/include/arweave_sync_deps.hrl").

-opaque state() :: #{term() => #store_state{}}.

-opaque plan() :: #{term() => #store_plan{}}.

%%%===================================================================
%%% Public interface.
%%%===================================================================

new() ->
    #{}.

%% @doc Return how many chunks the store has claimed, counting a whole
%% footprint for each unbound reservation.
claimed_chunks(StoreID, States) ->
    State = get_state(StoreID, States),
    State#store_state.claimed_chunks + State#store_state.reservation_chunks.

queues_empty(States) ->
    maps:fold(
        fun(_StoreID, State, Empty) ->
            Empty andalso gb_sets:is_empty(State#store_state.work_queue)
        end,
        true,
        States).

%% @doc Return the number of units of work in the store's work queue: tasks
%% and footprint reservations not yet bound to a peer.
queued_count(StoreID, States) ->
    gb_sets:size((get_state(StoreID, States))#store_state.work_queue).

%% @doc Return the number of the store's tasks not yet fetching: tasks in its
%% work queue or in a peer queue. Footprint reservations are not counted.
queued_task_count(StoreID, States) ->
    (get_state(StoreID, States))#store_state.queued_task_count.


%% @doc Return whether no store holds a chunk claim or a footprint claim.
claims_empty(States) ->
    maps:fold(
        fun(_StoreID, State, Empty) ->
            Empty andalso claims_empty_for_store(State)
        end,
        true,
        States).

claims_empty_for_store(#store_state{
        claimed = Claimed,
        claimed_chunks = ClaimedChunks,
        reservation_chunks = ReservationChunks
    }) ->
    ClaimedChunks =:= 0 andalso ReservationChunks =:= 0
        andalso arweave_lib_intervals:is_empty(Claimed).

%% @doc Return the distinct source peers of the units of work in every store's
%% work queue.
source_peers(States) ->
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
%%% Admission.
%%%===================================================================

%% @doc Claim and queue a task or footprint reservation if the store has room
%% under its claim limit; a task whose chunk is already claimed claims nothing.
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

%% @doc Return how many chunks the store may still claim within its claim limit.
admission_headroom(StoreID, States) ->
    admission_headroom(get_state(StoreID, States)).

admission_headroom(#store_state{ queued_task_count = QueuedTaskCount } = State) ->
    max(0, claim_limit(State) - QueuedTaskCount).

reservation_headroom(#store_state{ queued_task_count = QueuedTaskCount,
        reservation_chunks = ReservationChunks } = State) ->
    max(0, claim_limit(State) - QueuedTaskCount - ReservationChunks).

%% @doc Return how many chunks a task or footprint reservation claims; a
%% reservation claims a whole footprint until it binds.
chunks_in_claim(#task{}) ->
    1;
chunks_in_claim(#footprint_reservation{} = Reservation) ->
    arweave_sync_footprint:claim_size(Reservation).

is_claimed(Offset, #store_state{ claimed = Claimed }) ->
    arweave_lib_intervals:is_inside(Claimed, Offset + 1).

enqueue_state(#task{ offset = Offset } = Task, State) ->
    enqueue_task(Task, State#store_state{
        claimed = arweave_lib_intervals:add(
            State#store_state.claimed, Offset + ?DATA_CHUNK_SIZE, Offset),
        claimed_chunks = State#store_state.claimed_chunks + 1,
        queued_task_count = State#store_state.queued_task_count + 1
    });
enqueue_state(Reservation, State) ->
    enqueue_task(Reservation, State#store_state{
        reservation_chunks = State#store_state.reservation_chunks
            + chunks_in_claim(Reservation)
    }).

enqueue_task(Task, State) ->
    State#store_state{
        work_queue = gb_sets:add_element(
            work_element(Task), State#store_state.work_queue),
        queued_peer_counts = adjust_peer_counts(
            sources(Task), 1, State#store_state.queued_peer_counts)
    }.

%%%===================================================================
%%% Dispatch: snapshot.
%%%===================================================================

%% @doc Snapshot every store for one dispatch pass, counting its bound,
%% fetching and writing tasks and adding its bound footprints that still have
%% chunks left.
snapshot(Footprints, Tasks, BoundTasks, States) ->
    FairShare = fair_share(States),
    Plan = maps:map(
        fun(StoreID, State) ->
            new_store_plan(StoreID, State, FairShare)
        end,
        States),
    Plan2 = add_reservations(Footprints, Plan),
    Plan3 = lists:foldl(
        fun(#task{ store_id = StoreID }, Acc) ->
            record_task(StoreID, bound, Acc)
        end,
        Plan2,
        BoundTasks),
    maps:fold(
        fun(_TaskRef, #task{ state = TaskState, store_id = StoreID }, Acc)
                    when TaskState =:= fetching; TaskState =:= writing ->
                record_task(StoreID, TaskState, Acc);
           (_TaskRef, _Task, Acc) ->
                Acc
        end,
        Plan3,
        Tasks).

new_store_plan(StoreID, State, FairShare) ->
    #store_plan{
        state = State,
        cached_chunk_count = ?DEP(chunk_cache):cached_size(StoreID),
        cache_limit = cache_limit(State, FairShare),
        pipeline_limit = do_pipeline_limit(State, FairShare),
        disk_ready = ?DEP(data_sync):is_disk_space_sufficient(StoreID) =:= true,
        work_queue = State#store_state.work_queue,
        peers = sets:from_list(maps:keys(State#store_state.queued_peer_counts))
    }.

record_task(StoreID, TaskState, Plan) ->
    StorePlan = get_store_plan(StoreID, Plan),
    put_store_plan(do_record_task(TaskState, StorePlan), Plan).

do_record_task(fetching, #store_plan{ fetching_count = Count } = StorePlan) ->
    StorePlan#store_plan{ fetching_count = Count + 1 };
do_record_task(bound, #store_plan{ bound_count = Count } = StorePlan) ->
    StorePlan#store_plan{ bound_count = Count + 1 };
do_record_task(writing, #store_plan{ writing_count = Count } = StorePlan) ->
    StorePlan#store_plan{ writing_count = Count + 1 }.

%% @doc Add the bound reservations that still have chunks left to this pass's
%% work queues, leaving the stores' own queues unchanged.
add_reservations(Reservations, Plan) ->
    PendingReservations =
        arweave_sync_footprint:pending_reservations(Reservations),
    lists:foldl(fun do_add_reservation/2, Plan, PendingReservations).

do_add_reservation(Reservation, Plan) ->
    StoreID = arweave_sync_footprint:store_id(Reservation),
    StorePlan = get_store_plan(StoreID, Plan),
    WorkQueue = StorePlan#store_plan.work_queue,
    Peers = add_source_peers(
        arweave_sync_footprint:sources(Reservation),
        StorePlan#store_plan.peers),
    put_store_plan(StorePlan#store_plan{
        work_queue = gb_sets:add_element(work_element(Reservation), WorkQueue),
        peers = Peers
    }, Plan).

%% @doc Return Peer => [StoreID]: for each source peer in the plan, the stores
%% that have room for more work and list the peer as a source
%% (#store_plan.peers). arweave_sync_peer splits each peer's task limit among
%% these stores.
stores_with_room_by_peer(Plan) ->
    maps:fold(
        fun(StoreID, StorePlan, StoresByPeer) ->
            case has_capacity(StorePlan) of
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
                        StorePlan#store_plan.peers)
            end
        end,
        #{},
        Plan).

%%%===================================================================
%%% Dispatch: store selection.
%%%===================================================================

%% @doc Return the least-loaded store that has work and room for it.
best_store(Plan) ->
    case maps:fold(fun do_best_store/3, none, Plan) of
        none -> none;
        {_Priority, StoreID} -> {ok, StoreID}
    end.

do_best_store(StoreID, StorePlan, Selected) ->
    case has_capacity(StorePlan) andalso has_ready_work(StorePlan) of
        false ->
            Selected;
        true ->
            Priority = store_priority(StoreID, StorePlan),
            case Selected of
                none -> {Priority, StoreID};
                {SelectedPriority, _SelectedStoreID}
                        when Priority < SelectedPriority ->
                    {Priority, StoreID};
                _ -> Selected
            end
    end.

has_ready_work(#store_plan{ work_queue = WorkQueue }) ->
    not gb_sets:is_empty(WorkQueue).

store_priority(StoreID, StorePlan) ->
    {NetworkCount, ActiveCount} = load(StorePlan),
    #store_priority{
        network_count = NetworkCount,
        active_count = ActiveCount,
        store_id = StoreID
    }.

load(#store_plan{ bound_count = BoundCount,
        fetching_count = FetchingCount,
        writing_count = WritingCount }) ->
    NetworkCount = BoundCount + FetchingCount,
    {NetworkCount, NetworkCount + WritingCount}.

%% @doc Return whether one store can take more work this pass.
has_capacity(StoreID, Plan) ->
    has_capacity(get_store_plan(StoreID, Plan)).

has_capacity(#store_plan{
        bound_count = BoundCount,
        fetching_count = FetchingCount,
        cached_chunk_count = CachedChunkCount,
        cache_limit = CacheLimit,
        pipeline_limit = PipelineLimit,
        disk_ready = DiskReady
    }) ->
    NetworkCount = BoundCount + FetchingCount,
    DiskReady andalso CachedChunkCount < CacheLimit
        andalso CachedChunkCount + NetworkCount < PipelineLimit.

%% @doc Take the store's highest-priority unit of work off the plan's work
%% queue.
pop_work(StoreID, Plan) ->
    StorePlan = get_store_plan(StoreID, Plan),
    case do_pop_work(StorePlan) of
        none ->
            none;
        {Work, StorePlan2} ->
            {Work, put_store_plan(StorePlan2, Plan)}
    end.

do_pop_work(#store_plan{ work_queue = WorkQueue } = StorePlan) ->
    case gb_sets:is_empty(WorkQueue) of
        true ->
            none;
        false ->
            {Priority, Work} = gb_sets:smallest(WorkQueue),
            {Work, StorePlan#store_plan{ work_queue =
                gb_sets:delete({Priority, Work}, WorkQueue) }}
    end.

%%%===================================================================
%%% Dispatch: binding.
%%%===================================================================

%% @doc Move a task from the store's work queue to a peer queue. The task still
%% counts against the claim limit until its fetch starts.
bind_task(Task, Plan) ->
    StoreID = Task#task.store_id,
    StorePlan = get_store_plan(StoreID, Plan),
    #store_plan{ state = State, bound_count = BoundCount } = StorePlan,
    put_store_plan(StorePlan#store_plan{
        state = bind_queued_task(Task, State),
        bound_count = BoundCount + 1
    }, Plan).

bind_queued_task(#task{ sources = Sources } = Task, State) ->
    State#store_state{
        work_queue = gb_sets:delete(
            work_element(Task), State#store_state.work_queue),
        queued_peer_counts = adjust_peer_counts(
            Sources, -1, State#store_state.queued_peer_counts)
    }.

%% @doc Return the store's pipeline limit for this pass.
pipeline_limit(StoreID, Plan) ->
    (get_store_plan(StoreID, Plan))#store_plan.pipeline_limit.

%% @doc Return the chunks in Intervals that the store has not claimed yet.
unclaimed_intervals(StoreID, Intervals, Plan) ->
    StorePlan = get_store_plan(StoreID, Plan),
    do_unclaimed_intervals(Intervals, StorePlan#store_plan.state).

do_unclaimed_intervals(Intervals, #store_state{ claimed = Claimed }) ->
    arweave_lib_intervals:fold(
        fun({End, Start}, Acc) ->
            add_unclaimed_intervals(Start, End, Claimed, Acc)
        end,
        arweave_lib_intervals:new(),
        Intervals).

add_unclaimed_intervals(Start, End, _Claimed, Intervals) when Start >= End ->
    Intervals;
add_unclaimed_intervals(Offset, End, Claimed, Intervals) ->
    Next = Offset + ?DATA_CHUNK_SIZE,
    ChunkInterval = arweave_lib_intervals:from_list([{Next, Offset}]),
    OverlapsClaim = not arweave_lib_intervals:is_empty(
        arweave_lib_intervals:intersection(Claimed, ChunkInterval)),
    OverlapsCandidate = not arweave_lib_intervals:is_empty(
        arweave_lib_intervals:intersection(Intervals, ChunkInterval)),
    Intervals2 = case OverlapsClaim orelse OverlapsCandidate of
        true -> Intervals;
        false -> arweave_lib_intervals:add(Intervals, Next, Offset)
    end,
    add_unclaimed_intervals(Next, End, Claimed, Intervals2).

%% @doc Bind a reservation, removing it from the store's work queue and
%% releasing its whole-footprint claim.
bind_footprint(Reservation, Plan) ->
    StoreID = arweave_sync_footprint:store_id(Reservation),
    StorePlan = get_store_plan(StoreID, Plan),
    State = StorePlan#store_plan.state,
    State2 = dequeue(Reservation, State),
    put_store_plan(StorePlan#store_plan{ state = State2#store_state{
        reservation_chunks = max(0,
            State2#store_state.reservation_chunks
                - chunks_in_claim(Reservation)) }
    }, Plan).

dequeue(Reservation, State) ->
    State#store_state{
        work_queue = gb_sets:delete(
            work_element(Reservation), State#store_state.work_queue),
        queued_peer_counts = adjust_peer_counts(
            sources(Reservation), -1, State#store_state.queued_peer_counts)
    }.

%% @doc Claim the tasks of a bound footprint, which go straight to a peer
%% queue.
enqueue_bound_tasks(Tasks, Plan) ->
    lists:foldl(fun do_enqueue_bound_task/2, Plan, Tasks).

do_enqueue_bound_task(Task, Plan) ->
    StoreID = Task#task.store_id,
    StorePlan = get_store_plan(StoreID, Plan),
    #store_plan{ state = State, bound_count = BoundCount } = StorePlan,
    put_store_plan(StorePlan#store_plan{
        state = enqueue_bound_task(Task, State),
        bound_count = BoundCount + 1
    }, Plan).

enqueue_bound_task(#task{ offset = Offset }, State) ->
    State#store_state{
        claimed = arweave_lib_intervals:add(
            State#store_state.claimed, Offset + ?DATA_CHUNK_SIZE, Offset),
        claimed_chunks = State#store_state.claimed_chunks + 1,
        queued_task_count = State#store_state.queued_task_count + 1
    }.

%%%===================================================================
%%% Dispatch: starting fetches.
%%%===================================================================

%% @doc Rank the store for starting one of its bound tasks; the lowest rank
%% wins. Stores compare by their fetches in flight, fewest first.
%%
%% Return false while the store cannot start a fetch, because:
%% - its disk is not ready; or
%% - its chunks in the chunk cache have reached its cache limit.
%% A bound task already counts toward the pipeline limit, so only these two
%% hold its fetch back.
rank(StoreID, Plan) ->
    #store_plan{
        cached_chunk_count = CachedChunkCount,
        cache_limit = CacheLimit,
        disk_ready = DiskReady,
        fetching_count = FetchingCount
    } = get_store_plan(StoreID, Plan),
    case DiskReady andalso CachedChunkCount < CacheLimit of
        true -> {true, FetchingCount};
        false -> false
    end.

%% @doc Move a bound task to the fetching stage.
start_bound_task(Task, Plan) ->
    StoreID = Task#task.store_id,
    StorePlan = get_store_plan(StoreID, Plan),
    #store_plan{
        state = State,
        bound_count = BoundCount,
        fetching_count = FetchingCount
    } = StorePlan,
    State2 = State#store_state{
        queued_task_count = max(0, State#store_state.queued_task_count - 1)
    },
    put_store_plan(StorePlan#store_plan{
        state = State2,
        bound_count = max(0, BoundCount - 1),
        fetching_count = FetchingCount + 1
    }, Plan).

%%%===================================================================
%%% Dispatch: refresh and finish.
%%%===================================================================

%% @doc Reset the pass's work queues from the stores' own queues, so work that
%% found no peer earlier in the pass can use the room the started fetches
%% freed. Binding removes only the bound work from a store's own queue.
refresh_work(Footprints, Plan) ->
    Plan2 = maps:map(
        fun(_StoreID, #store_plan{ state = State } = StorePlan) ->
            StorePlan#store_plan{
                work_queue = State#store_state.work_queue,
                peers = sets:from_list(
                    maps:keys(State#store_state.queued_peer_counts))
            }
        end,
        Plan),
    add_reservations(Footprints, Plan2).

%% @doc Return the store states to keep after a dispatch pass.
commit_plan(Plan) ->
    maps:map(
        fun(_StoreID, #store_plan{ state = State }) -> State end,
        Plan).

%%%===================================================================
%%% Task completion.
%%%===================================================================

%% @doc Record one completed store write in the current write sample.
record_write_completed(StoreID, States) ->
    case ?DEP(chunk_cache):completed(StoreID) of
        undefined -> do_record_write_completed(StoreID, States);
        _ -> States
    end.

do_record_write_completed(StoreID, States) ->
    State = get_state(StoreID, States),
    SampleStartedMs = case State#store_state.sample_started_ms of
        undefined -> ?DEP(clock):monotonic_ms();
        StartedMs -> StartedMs
    end,
    State2 = State#store_state{
        completed_writes_since_sample =
            State#store_state.completed_writes_since_sample + 1,
        sample_started_ms = SampleStartedMs
    },
    put_state(State2, States).

%% @doc Release a chunk's claim after its task finishes.
release_claim(StoreID, Offset, States) ->
    State = get_state(StoreID, States),
    State2 = State#store_state{
        claimed = arweave_lib_intervals:delete(
            State#store_state.claimed, Offset + ?DATA_CHUNK_SIZE, Offset),
        claimed_chunks = max(0, State#store_state.claimed_chunks - 1)
    },
    put_state(State2, States).

%%%===================================================================
%%% Tick.
%%%===================================================================

%% @doc Update each store's write rate from the writes completed since its
%% last write sample.
sample_write_rates(NowMs, States) ->
    maps:map(
        fun(StoreID, State) ->
            CachedChunkCount = ?DEP(chunk_cache):cached_size(StoreID),
            State2 = observe_writes(StoreID, State),
            sample_write_rate(CachedChunkCount, NowMs, State2)
        end,
        States).

observe_writes(StoreID, State) ->
    case ?DEP(chunk_cache):completed(StoreID) of
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

sample_write_rate(CachedChunkCount, NowMs, State) ->
    #store_state{
        completed_writes_since_sample = CompletedWrites,
        sample_started_ms = SampleStartedMs,
        sample_driven = SampleDriven
    } = State,
    case SampleStartedMs of
        undefined when CachedChunkCount > 0 ->
            State#store_state{
                sample_started_ms = NowMs
            };
        undefined ->
            State;
        _ when NowMs - SampleStartedMs >= ?WRITE_SAMPLE_MIN_MS ->
            ElapsedMs = NowMs - SampleStartedMs,
            Sample = CompletedWrites * 1000 / ElapsedMs,
            Driven = SampleDriven andalso CachedChunkCount > 0,
            WriteRate2 = update_write_rate(
                Driven, CompletedWrites, Sample,
                State#store_state.write_rate),
            State#store_state{
                completed_writes_since_sample = 0,
                sample_started_ms = NowMs,
                sample_driven = CachedChunkCount > 0,
                write_rate = WriteRate2
            };
        _ ->
            State#store_state{
                sample_driven =
                    SampleDriven andalso CachedChunkCount > 0
            }
    end.

update_write_rate(true, _CompletedWrites, Sample, undefined) ->
    Sample;
update_write_rate(true, _CompletedWrites, Sample, WriteRate)
        when Sample >= WriteRate ->
    Sample;
update_write_rate(true, _CompletedWrites, Sample, WriteRate) ->
    arweave_lib_util:ema(WriteRate, Sample, ?WRITE_RATE_ALPHA);
update_write_rate(false, CompletedWrites, Sample, undefined)
        when CompletedWrites > 0 ->
    Sample;
update_write_rate(false, CompletedWrites, Sample, WriteRate)
        when CompletedWrites > 0 ->
    max(WriteRate, Sample);
update_write_rate(false, 0, _Sample, WriteRate) ->
    WriteRate.

%%%===================================================================
%%% Limits.
%%%===================================================================

%% @doc Return the store's claim limit: how many chunks it may have queued or
%% bound ahead of its writes.
claim_limit(StoreID, States) ->
    claim_limit(get_state(StoreID, States)).

claim_limit(#store_state{ write_rate = undefined }) ->
    bootstrap_limit(?DEP(chunk_cache):limit());
claim_limit(#store_state{ write_rate = ChunksPerSecond }) ->
    max(?MIN_CLAIM_LIMIT,
        round(ChunksPerSecond * ?WRITE_QUEUE_TARGET_DURATION_MS / 1000)).

bootstrap_limit(ChunkCacheLimit) ->
    max(?MIN_CLAIM_LIMIT, ChunkCacheLimit div 4).

cache_limit(State, FairShare) ->
    min(FairShare, initial_cache_limit(State)).

initial_cache_limit(#store_state{ write_rate = undefined }) ->
    ?MIN_CLAIM_LIMIT;
initial_cache_limit(State) ->
    claim_limit(State).

%% @doc Return the store's pipeline limit, which caps its bound and fetching
%% chunks plus its chunks in the chunk cache.
pipeline_limit(StoreID, FairShare, States) ->
    do_pipeline_limit(get_state(StoreID, States), FairShare).

do_pipeline_limit(State, FairShare) ->
    min(FairShare, write_rate_pipeline_limit(State)).

write_rate_pipeline_limit(#store_state{ write_rate = undefined }) ->
    ?MIN_CLAIM_LIMIT;
write_rate_pipeline_limit(#store_state{ write_rate = ChunksPerSecond }) ->
    max(?MIN_CLAIM_LIMIT, round(ChunksPerSecond
        * ?PIPELINE_TARGET_DURATION_MS / 1000)).

%% @doc Return the most chunks one store may hold in the chunk cache, set so
%% that the stores' demands just fill the cache. A store that needs less than
%% an even split gets only what it needs, and the other stores split the rest,
%% so a store with little or no work does not shrink everyone else's share.
fair_share(States) ->
    Demands = lists:sort(maps:fold(
        fun(_StoreID, State, Acc) ->
            case demand(State) of
                0 -> Acc;
                Demand -> [Demand | Acc]
            end
        end,
        [],
        States)),
    Limit = ?DEP(chunk_cache):limit(),
    share_level(Demands, length(Demands), Limit, Limit).

share_level([], _Count, _Remaining, Limit) ->
    Limit;
share_level([Demand | Demands], Count, Remaining, Limit) ->
    EvenSplit = Remaining div Count,
    case Demand =< EvenSplit of
        true -> share_level(Demands, Count - 1, Remaining - Demand, Limit);
        false -> max(1, EvenSplit)
    end.

%% @doc Return how many chunks the store could keep in its pipeline: the chunks
%% it has claimed, up to the pipeline limit its write rate supports.
demand(#store_state{ claimed_chunks = ClaimedChunks,
        reservation_chunks = ReservationChunks } = State) ->
    min(ClaimedChunks + ReservationChunks, write_rate_pipeline_limit(State)).

%%%===================================================================
%%% Shared helpers.
%%%===================================================================

get_state(StoreID, States) ->
    maps:get(StoreID, States, #store_state{ store_id = StoreID }).

put_state(#store_state{ store_id = StoreID } = State, States) ->
    maps:put(StoreID, State, States).

get_store_plan(StoreID, Plan) ->
    case maps:find(StoreID, Plan) of
        {ok, StorePlan} -> StorePlan;
        error ->
            %% A store with no state has no claims, so its fair share is the
            %% whole cache; the MIN_CLAIM_LIMIT limits of an unmeasured store
            %% keep it small.
            new_store_plan(StoreID, #store_state{ store_id = StoreID },
                ?DEP(chunk_cache):limit())
    end.

put_store_plan(#store_plan{ state = #store_state{ store_id = StoreID } } = StorePlan,
        Plan) ->
    maps:put(StoreID, StorePlan, Plan).

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

%% @doc Return the unit's position in the work queue: its footprint's index
%% within the partition. A task without a footprint takes the index of the
%% matching slice of its partition, so units across partitions interleave.
work_position(#task{ footprint = #footprint{
        footprint = Footprint } }) ->
    Footprint;
work_position(#task{ footprint = none, offset = Offset }) ->
    BlockSize = ?PARTITION_SIZE div
        arweave_lib_constants:get_replica_2_9_footprints_per_partition(),
    (Offset rem ?PARTITION_SIZE) div BlockSize;
work_position(#footprint_reservation{} = Reservation) ->
    arweave_sync_footprint:sort_key(Reservation).

%% @doc Rank the kind of unit, to break ties at the same position:
%% single-chunk tasks, then bound footprints, then queued footprints.
work_kind_rank(#task{}) ->
    0;
work_kind_rank(#footprint_reservation{} = Reservation) ->
    arweave_sync_footprint:queue_rank(Reservation).

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

%%%===================================================================
%%% Test support.
%%%===================================================================

-ifdef(AR_TEST).
%% @doc Latest measured write rate in chunks per second, or undefined.
write_rate(StoreID, States) ->
    (get_state(StoreID, States))#store_state.write_rate.
-endif.

-ifdef(AR_TEST).
%% @doc Build a store dispatch without existing tasks or footprint
%% reservations.
test_plan(States) ->
    snapshot(arweave_sync_footprint:new(), #{}, [], States).
-endif.

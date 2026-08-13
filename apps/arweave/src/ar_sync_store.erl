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
        queues_empty/1, queued_count/2, queued_tasks/2, claims_empty/1,
        peers/1,
        record_write_completed/2, sample_drain_rates/3, drain_rate/2,
        start_dispatch/3, finish_dispatch/1,
        best_store/1, pop_work/2,
        add_reservations/2, start_task/2,
        enqueue_tasks/2, bind_footprint/2,
        unclaimed_intervals/3, has_capacity/2, stores_by_peer/1]).
-export_type([state/0, dispatch/0]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_sync.hrl").

-ifdef(AR_TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% Enough queued work to cover transient write variance without letting a slow
%% store monopolize the chunk cache.
-define(PIPELINE_HORIZON_MS, 4000).

%% A store whose measured rate collapses must retain enough work to make
%% progress and earn a higher rate again.
-define(TASK_FLOOR, 50).

-record(store_state, {
    store_id,
    %% Persistent work retained across dispatch passes.
    work_queue = gb_sets:new(),
    claimed = ar_intervals:new(),
    claimed_chunks = 0,
    queued_peer_counts = #{},
    completed_writes_since_tick = 0,
    drain_rate = undefined,
    write_backlog_at_last_tick = false
}).

-opaque state() :: #{term() => #store_state{}}.

%% One store's mutable snapshot during a scheduler dispatch pass.
-record(store_dispatch, {
    state,
    fetching_count = 0,
    writing_count = 0,
    cached_chunk_count = 0,
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

%% @doc Chunks the store may still claim before reaching its work horizon.
admission_headroom(StoreID, States) ->
    State = get_state(StoreID, States),
    max(0, claim_limit(State) - State#store_state.claimed_chunks).

%% @doc Number of fetched and fetching chunks allowed for the store. An
%% unmeasured store starts from a cache-derived bootstrap limit.
claim_limit(StoreID, States) ->
    claim_limit(get_state(StoreID, States)).

claim_limit(#store_state{ drain_rate = undefined }) ->
    bootstrap_limit(ar_sync_deps:chunk_cache_size_limit());
claim_limit(#store_state{ drain_rate = ChunksPerSecond }) ->
    max(?TASK_FLOOR,
        round(ChunksPerSecond * ?PIPELINE_HORIZON_MS / 1000)).


%% @doc Return the store capacity reserved by a task or reservation. A
%% footprint reservation claims one complete footprint until it is bound.
chunks_in_claim(#task{}) ->
    1;
chunks_in_claim(#footprint_reservation{} = Reservation) ->
    ar_sync_footprint:claim_size(Reservation).

%% @doc Number of concrete chunks and footprint claims currently held.
claimed_chunks(StoreID, States) ->
    (get_state(StoreID, States))#store_state.claimed_chunks.

%% @doc Return whether Offset belongs to an existing concrete chunk claim.
is_claimed(StoreID, Offset, States) ->
    is_claimed(Offset, get_state(StoreID, States)).

is_claimed(Offset, #store_state{ claimed = Claimed }) ->
    ar_intervals:is_inside(Claimed, Offset + 1).

%% @doc Claim and enqueue a candidate when the store has admission headroom.
admit(StoreID, Task, States) ->
    State = get_state(StoreID, States),
    case admit(Task, State) of
        {ok, ClaimedChunks, State2} ->
            {ok, ClaimedChunks, put_state(State2, States)};
        rejected ->
            rejected
    end.

admit(#task{ offset = Offset } = Task, State) ->
    case admission_headroom(State) > 0 andalso not is_claimed(Offset, State) of
        true ->
            {ok, 1, enqueue_state(Task, State)};
        false ->
            rejected
    end;
admit(Reservation, State) ->
    case admission_headroom(State) > 0 of
        true ->
            ClaimedChunks = chunks_in_claim(Reservation),
            {ok, ClaimedChunks, enqueue_state(Reservation, State)};
        false ->
            rejected
    end.

admission_headroom(#store_state{ claimed_chunks = ClaimedChunks } = State) ->
    max(0, claim_limit(State) - ClaimedChunks).

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

%% @doc Queued tasks in dispatch order.
queued_tasks(StoreID, States) ->
    WorkQueue = (get_state(StoreID, States))#store_state.work_queue,
    [Task || {_Priority, Task} <- gb_sets:to_list(WorkQueue)].

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

%% @doc Record one completed store write in the current scheduler interval.
record_write_completed(StoreID, States) ->
    State = get_state(StoreID, States),
    State2 = State#store_state{ completed_writes_since_tick =
        State#store_state.completed_writes_since_tick + 1 },
    put_state(State2, States).

%% @doc Sample drain capacity at a scheduler boundary. A persistent write
%% backlog measures the completed rate. Completing work without retaining a
%% backlog clears a stale low estimate and returns the store to bootstrap.
sample_drain_rates(StoresWithWriteBacklog, TickIntervalMs, States) ->
    maps:map(
        fun(StoreID, State) ->
            HasWriteBacklog = sets:is_element(StoreID, StoresWithWriteBacklog),
            sample_drain_rate(HasWriteBacklog, TickIntervalMs, State)
        end,
        States).

sample_drain_rate(HasWriteBacklog, TickIntervalMs, State) ->
    #store_state{
        completed_writes_since_tick = CompletedWrites,
        drain_rate = DrainRate,
        write_backlog_at_last_tick = HadWriteBacklog
    } = State,
    DrainRate2 = case {HadWriteBacklog, HasWriteBacklog, CompletedWrites} of
        {true, true, _} -> CompletedWrites * 1000 / TickIntervalMs;
        {_, false, Count} when Count > 0 -> undefined;
        _ -> DrainRate
    end,
    State#store_state{
        completed_writes_since_tick = 0,
        drain_rate = DrainRate2,
        write_backlog_at_last_tick = HasWriteBacklog
    }.

%% @doc Latest measured drain rate in chunks per second, or undefined.
drain_rate(StoreID, States) ->
    (get_state(StoreID, States))#store_state.drain_rate.

%%%===================================================================
%%% Dispatch snapshot.
%%%===================================================================

%% @doc Snapshot every indexed store, existing task, and pending footprint
%% footprint work for one scheduler dispatch pass.
start_dispatch(Footprints, Tasks, States) ->
    Dispatches = maps:map(
        fun new_store_dispatch/2,
        States),
    Dispatches2 = add_reservations(Footprints, Dispatches),
    maps:fold(
        fun(_TaskRef, #task{ state = TaskState, store_id = StoreID }, Acc)
                when TaskState =:= fetching; TaskState =:= writing ->
                record_task(StoreID, TaskState, Acc);
            (_TaskRef, _Task, Acc) ->
                Acc
        end,
        Dispatches2,
        Tasks).

-ifdef(AR_TEST).
%% @doc Build a store dispatch without existing tasks or footprint work.
test_dispatch(States) ->
    start_dispatch(ar_sync_footprint:new(), #{}, States).
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
do_record_task(writing, #store_dispatch{ writing_count = Count } = Dispatch) ->
    Dispatch#store_dispatch{ writing_count = Count + 1 }.

%% @doc Return fetching load and total active load for store selection.
load(#store_dispatch{ fetching_count = FetchingCount,
        writing_count = WritingCount }) ->
    {FetchingCount, FetchingCount + WritingCount}.

%% @doc Return whether the store can start another network fetch within its
%% local work horizon. The scheduler enforces the node-wide cache limit.
has_capacity(#store_dispatch{
        state = State,
        fetching_count = FetchingCount,
        cached_chunk_count = CachedChunkCount,
        disk_ready = DiskReady
    }) ->
    ProjectedCachedChunkCount = CachedChunkCount + FetchingCount,
    DiskReady andalso ProjectedCachedChunkCount < claim_limit(State).

%% @doc Return whether one store can start another network fetch this pass.
has_capacity(StoreID, Dispatches) ->
    has_capacity(get_dispatch(StoreID, Dispatches)).

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

%% @doc Remove a selected chunk from persistent work and count its fetch.
start_task(Task, Dispatches) ->
    StoreID = Task#task.store_id,
    Dispatch = get_dispatch(StoreID, Dispatches),
    #store_dispatch{ state = State, fetching_count = FetchingCount } = Dispatch,
    put_dispatch(Dispatch#store_dispatch{
        state = dequeue(Task, State),
        fetching_count = FetchingCount + 1
    }, Dispatches).

%% @doc Enqueue claimed tasks in persistent and dispatch-pass work.
enqueue_tasks(Tasks, Dispatches) ->
    lists:foldl(fun do_enqueue_task/2, Dispatches, Tasks).

do_enqueue_task(Task, Dispatches) ->
    StoreID = Task#task.store_id,
    Dispatch = get_dispatch(StoreID, Dispatches),
    #store_dispatch{ state = State, work_queue = WorkQueue } = Dispatch,
    put_dispatch(Dispatch#store_dispatch{
        state = enqueue_state(Task, State),
        work_queue = gb_sets:add_element(work_element(Task), WorkQueue)
    }, Dispatches).

%% @doc Bind a reservation and release its persistent whole-footprint claim.
bind_footprint(Reservation, Dispatches) ->
    StoreID = ar_sync_footprint:store_id(Reservation),
    Dispatch = get_dispatch(StoreID, Dispatches),
    State = Dispatch#store_dispatch.state,
    State2 = dequeue(Reservation, State),
    put_dispatch(Dispatch#store_dispatch{
        state = State2#store_state{ claimed_chunks = max(0,
            State2#store_state.claimed_chunks - chunks_in_claim(Reservation)) }
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
    max(?TASK_FLOOR, ChunkCacheLimit div 4).

get_state(StoreID, States) ->
    maps:get(StoreID, States, #store_state{ store_id = StoreID }).

put_state(#store_state{ store_id = StoreID } = State, States) ->
    maps:put(StoreID, State, States).

get_dispatch(StoreID, Dispatches) ->
    case maps:find(StoreID, Dispatches) of
        {ok, Dispatch} -> Dispatch;
        error -> new_store_dispatch(StoreID, #store_state{ store_id = StoreID })
    end.

put_dispatch(#store_dispatch{ state = #store_state{ store_id = StoreID } } = Dispatch,
        Dispatches) ->
    maps:put(StoreID, Dispatch, Dispatches).

new_store_dispatch(StoreID, State) ->
    #store_dispatch{
        state = State,
        cached_chunk_count = ar_sync_deps:chunk_cache_size(StoreID),
        disk_ready = ar_sync_deps:is_disk_space_sufficient(StoreID) =:= true,
        work_queue = State#store_state.work_queue,
        peers = sets:from_list(maps:keys(State#store_state.queued_peer_counts))
    }.

claims_empty_for_store(#store_state{
        claimed = Claimed,
        claimed_chunks = ClaimedChunks
    }) ->
    ClaimedChunks =:= 0 andalso ar_intervals:is_empty(Claimed).

enqueue_state(#task{ offset = Offset } = Task, State) ->
    enqueue_task(Task, State#store_state{
        claimed = ar_intervals:add(
            State#store_state.claimed, Offset + ?DATA_CHUNK_SIZE, Offset),
        claimed_chunks = State#store_state.claimed_chunks + 1
    });
enqueue_state(Reservation, State) ->
    enqueue_task(Reservation, State#store_state{
        claimed_chunks = State#store_state.claimed_chunks
            + chunks_in_claim(Reservation)
    }).

dequeue(Task, State) ->
    State#store_state{
        work_queue = gb_sets:delete(
            work_element(Task), State#store_state.work_queue),
        queued_peer_counts = adjust_peer_counts(
            sources(Task), -1, State#store_state.queued_peer_counts)
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
        %% One quarter of a 2000-chunk cache is 500 chunks.
        ?assertEqual(500, bootstrap_limit(2000)),
        %% Four seconds at 100 chunks/s is 400 chunks.
        ?assertEqual(400,
            claim_limit(#store_state{ drain_rate = 100 })),
        %% Four seconds at 105 chunks/s is 420 chunks without footprint rounding.
        ?assertEqual(420,
            claim_limit(#store_state{ drain_rate = 105 })),
        %% A one-chunk/s store retains the 50-chunk progress floor.
        ?assertEqual(50,
            claim_limit(#store_state{ drain_rate = 1 }))
    after
        ar_replica_2_9:reset_all_overrides()
    end.

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
        claimed_chunks = InitialClaimedChunks
    }, new()),
    {ok, ClaimedChunks, State} = admit(store1, Reservation, InitialState),
    FootprintChunks = ar_sync_footprint:claim_size(Reservation),
    ?assertEqual(FootprintChunks, chunks_in_claim(Reservation)),
    ?assertEqual(FootprintChunks, ClaimedChunks),
    ?assertEqual(InitialClaimedChunks + FootprintChunks,
        claimed_chunks(store1, State)),
    %% A reservation may exceed the final positive headroom, but its complete
    %% footprint charge prevents another reservation from being admitted.
    Reservation2 = ar_sync_footprint:new_reservation(
        store1, Footprint#footprint{ footprint = 2 }, Sources),
    ?assertEqual(rejected, admit(store1, Reservation2, State)).

dispatch_best_store_orders_by_load_test() ->
    %% The first dispatch has no active tasks. The next two have one fetch each,
    %% with the writing task making store_c the more heavily loaded tie.
    Task = #task{ offset = 0,
        sources = [#task_source{ peer = peer }] },
    WorkQueue = gb_sets:singleton(work_element(Task)),
    StoreA = #store_dispatch{ state = #store_state{ store_id = store_a },
        disk_ready = true,
        work_queue = WorkQueue },
    StoreB = #store_dispatch{ state = #store_state{ store_id = store_b },
        disk_ready = true,
        work_queue = WorkQueue, fetching_count = 1 },
    StoreC = #store_dispatch{ state = #store_state{ store_id = store_c },
        fetching_count = 1,
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
    Dispatches0 = start_dispatch(Footprints, #{}, States),
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
    %% Leaving rejected work out of the dispatch-local queue exposes the next
    %% item without changing persistent queued work.
    {Second, _Dispatch2} = pop_work(store1, Dispatch1),
    %% A new pass is rebuilt from persistent state, so the first task is ready
    %% again rather than carrying blocked state across scheduler callbacks.
    NewDispatch = test_dispatch(State2),
    {First, _NewDispatch2} = pop_work(store1, NewDispatch).

drain_rate_tick_boundaries_test() ->
    TickIntervalMs = 10_000,
    InitialRate = 100,
    State0 = #store_state{ drain_rate = InitialRate,
        completed_writes_since_tick = 200 },

    %% The first busy boundary starts a complete sample and retains the old rate.
    State1 = sample_drain_rate(true, TickIntervalMs, State0),
    ?assertEqual(InitialRate, State1#store_state.drain_rate),
    ?assertEqual(true, State1#store_state.write_backlog_at_last_tick),
    ?assertEqual(0, State1#store_state.completed_writes_since_tick),

    %% 200 completions over ten seconds is 20 cps and an 80-chunk horizon.
    State2 = sample_drain_rate(true, TickIntervalMs,
        State1#store_state{ completed_writes_since_tick = 200 }),
    ?assertEqual(20.0, State2#store_state.drain_rate),
    ?assertEqual(80, claim_limit(State2)),

    %% A continuously busy interval with no completions reaches the 50-chunk floor.
    StalledState = sample_drain_rate(true, TickIntervalMs, State2),
    ?assertEqual(0.0, StalledState#store_state.drain_rate),
    ?assertEqual(50, claim_limit(StalledState)),

    %% An idle interval with no writes retains the last measured capacity.
    IdleState = sample_drain_rate(false, TickIntervalMs, State2),
    ?assertEqual(20.0, IdleState#store_state.drain_rate),

    %% Completing the offered work with no remaining backlog invalidates the
    %% stalled estimate so the next admission uses the larger bootstrap limit.
    CompletedState = StalledState#store_state{
        completed_writes_since_tick = 1 },
    RecoveredState = sample_drain_rate(false, TickIntervalMs, CompletedState),
    ?assertEqual(undefined, RecoveredState#store_state.drain_rate).

-endif.

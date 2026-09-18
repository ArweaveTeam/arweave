-module(arweave_sync_store_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_sync/include/arweave_sync.hrl").
-include("arweave_sync_store.hrl").
-import(arweave_sync_store, [
    admission_headroom/1,
    admit/3,
    best_store/1,
    bootstrap_limit/1,
    chunks_in_claim/1,
    claim_limit/1,
    claimed_chunks/2,
    do_cache_limit/2,
    do_pipeline_limit/2,
    has_capacity/1,
    new/0,
    pop_work/2,
    put_state/2,
    queued_count/2,
    queued_task_count/2,
    sample_drain_rate/3,
    start_dispatch/4,
    stores_by_peer/1,
    test_dispatch/1,
    work_element/1
]).

suite() -> [{timetrap, {seconds, 60}}].

all() ->
    [
        capacity_limits_are_chunk_granular,
        admission_headroom_counts_only_queued_tasks,
        fetching_tasks_do_not_consume_store_cache_limit,
        unmeasured_store_limits_initial_fetch_probe,
        footprint_claim_reserves_complete_footprint,
        dispatch_best_store_orders_by_load,
        stores_by_peer_includes_task_and_footprint_demand,
        pop_work_returns_queued_task,
        work_removed_from_a_pass_returns_in_the_next_pass,
        drain_rate_uses_tick_samples
    ].

init_per_testcase(_Case, Config) ->
    arweave_sync_test_deps:setup(),
    Config.

end_per_testcase(_Case, _Config) ->
    arweave_sync_test_deps:cleanup().

%%====================================================================
%% Test cases
%%====================================================================

%% @doc Store limits derive from measured drain rates in chunks, with a minimum
%% progress probe.
capacity_limits_are_chunk_granular(_Config) ->
    %% A 100-chunk footprint keeps the limit arithmetic explicit.
    ?assertEqual(
        100 * ?DATA_CHUNK_SIZE,
        (arweave_sync_deps:constants()):get_replica_2_9_footprint_size()
    ),
    %% One quarter of a 2000-chunk cache provides 500 bootstrap chunks.
    ?assertEqual(500, bootstrap_limit(2000)),
    %% An unmeasured store starts with the twenty-five-chunk probe.
    ?assertEqual(25, do_cache_limit(#store_state{}, 1)),
    %% Five seconds at 100 chunks/s is 500 chunks.
    ?assertEqual(
        500,
        claim_limit(#store_state{drain_rate = 100})
    ),
    %% Five seconds at 105 chunks/s is 525 chunks without footprint rounding.
    ?assertEqual(
        525,
        claim_limit(#store_state{drain_rate = 105})
    ),
    %% Five seconds at fifteen chunks/s is a seventy-five-chunk cache limit.
    ?assertEqual(
        75,
        do_cache_limit(#store_state{drain_rate = 15}, 1)
    ),
    %% Cached and fetching stages retain eleven seconds, or 165 chunks.
    ?assertEqual(
        165,
        do_pipeline_limit(#store_state{drain_rate = 15}, 1)
    ),
    %% Five seconds at one chunk/s is below the 25-chunk progress probe.
    ?assertEqual(
        25,
        claim_limit(#store_state{drain_rate = 1})
    ).

%% @doc Fetching and writing claims do not consume headroom for future queued
%% tasks.
admission_headroom_counts_only_queued_tasks(_Config) ->
    %% One chunk/s resolves to the twenty-five-chunk progress floor. Concrete
    %% claims already fetching or writing remain for deduplication but do not
    %% consume future queue capacity.
    State = #store_state{
        drain_rate = 1,
        claimed_chunks = 25,
        queued_task_count = 4
    },
    ?assertEqual(21, admission_headroom(State)).

%% @doc Cached chunks and total pipeline work are bounded independently for
%% measured stores.
fetching_tasks_do_not_consume_store_cache_limit(_Config) ->
    %% Once drain is measured, peer and global limits bound network requests;
    %% the store cache limit applies only after chunks enter its cache.
    Dispatch = #store_dispatch{
        state = #store_state{drain_rate = 1},
        cached_chunk_count = 3,
        fetching_count = 10,
        cache_limit = 4,
        pipeline_limit = 14,
        disk_ready = true
    },
    ?assert(has_capacity(Dispatch)),
    ?assertNot(
        has_capacity(Dispatch#store_dispatch{
            cached_chunk_count = 4
        })
    ),
    ?assertNot(
        has_capacity(Dispatch#store_dispatch{
            fetching_count = 11
        })
    ).

%% @doc A store without a drain measurement cannot exceed the initial fetch
%% probe.
unmeasured_store_limits_initial_fetch_probe(_Config) ->
    Dispatch = #store_dispatch{
        state = #store_state{},
        fetching_count = ?MIN_CLAIM_LIMIT - 1,
        cache_limit = 500,
        pipeline_limit = 500,
        disk_ready = true
    },
    ?assert(has_capacity(Dispatch)),
    ?assertNot(
        has_capacity(Dispatch#store_dispatch{
            fetching_count = ?MIN_CLAIM_LIMIT
        })
    ).

%% @doc Footprint admission charges a complete footprint without using
%% concrete-task queue headroom.
footprint_claim_reserves_complete_footprint(_Config) ->
    Footprint = #footprint{store_id = store1, partition = 0, footprint = 1},
    OneChunk = ar_intervals:from_list([{?DATA_CHUNK_SIZE, 0}]),
    ThreeChunks = ar_intervals:from_list([
        {2 * ?DATA_CHUNK_SIZE + 1, 0}
    ]),
    Sources = [
        #task_source{
            peer = peer1,
            footprint = Footprint,
            intervals = OneChunk
        },
        #task_source{
            peer = peer2,
            footprint = Footprint,
            intervals = ThreeChunks
        }
    ],
    Reservation = arweave_sync_footprint:new_reservation(store1, Footprint, Sources),
    %% Task-source coverage is unknown until dispatch binds the reservation, so admission
    %% reserves the complete footprint rather than either advertised subset.
    ChunkCacheLimit = (arweave_sync_deps:chunk_cache()):limit(),
    InitialClaimedChunks = bootstrap_limit(ChunkCacheLimit) - 1,
    InitialState = put_state(
        #store_state{
            store_id = store1,
            claimed_chunks = InitialClaimedChunks,
            queued_task_count = InitialClaimedChunks
        },
        new()
    ),
    {ok, ClaimedChunks, State} = admit(store1, Reservation, InitialState),
    FootprintChunks = arweave_sync_footprint:claim_size(Reservation),
    ?assertEqual(FootprintChunks, chunks_in_claim(Reservation)),
    ?assertEqual(FootprintChunks, ClaimedChunks),
    ?assertEqual(
        InitialClaimedChunks + FootprintChunks,
        claimed_chunks(store1, State)
    ),
    ?assertEqual(1, queued_count(store1, State)),
    ?assertEqual(InitialClaimedChunks, queued_task_count(store1, State)),
    %% Reservation credit does not consume the one remaining concrete task of
    %% headroom; exact claims arbitrate overlap when the footprint binds.
    Task = #task{
        offset = ?DATA_CHUNK_SIZE,
        sources = [#task_source{peer = peer1}]
    },
    {ok, 1, State2} = admit(store1, Task, State),
    ?assertEqual(
        InitialClaimedChunks + 1,
        queued_task_count(store1, State2)
    ),
    %% A reservation may exceed the final positive headroom, but its complete
    %% footprint charge prevents another reservation from being admitted.
    Reservation2 = arweave_sync_footprint:new_reservation(
        store1, Footprint#footprint{footprint = 2}, Sources
    ),
    ?assertEqual(blocked, admit(store1, Reservation2, State2)).

%% @doc Store selection prefers fewer active fetches, then fewer pending writes.
dispatch_best_store_orders_by_load(_Config) ->
    %% The first dispatch has no active tasks. The next two have one fetch each,
    %% with the writing task making store_c the more heavily loaded tie.
    Task = #task{
        offset = 0,
        sources = [#task_source{peer = peer}]
    },
    WorkQueue = gb_sets:singleton(work_element(Task)),
    StoreA = #store_dispatch{
        state = #store_state{store_id = store_a},
        disk_ready = true,
        cache_limit = 100,
        pipeline_limit = 100,
        work_queue = WorkQueue
    },
    StoreB = #store_dispatch{
        state = #store_state{store_id = store_b},
        disk_ready = true,
        cache_limit = 100,
        pipeline_limit = 100,
        work_queue = WorkQueue,
        fetching_count = 1
    },
    StoreC = #store_dispatch{
        state = #store_state{store_id = store_c},
        fetching_count = 1,
        cache_limit = 100,
        pipeline_limit = 100,
        writing_count = 1,
        disk_ready = true,
        work_queue = WorkQueue
    },
    Dispatches = #{store_c => StoreC, store_b => StoreB, store_a => StoreA},
    {ok, store_a} = best_store(Dispatches),
    {ok, store_b} = best_store(maps:remove(store_a, Dispatches)),
    {ok, store_c} = best_store(
        maps:remove(
            store_b,
            maps:remove(store_a, Dispatches)
        )
    ).

%% @doc Runnable byte and footprint demand expose their peers; disk-blocked
%% stores expose none.
stores_by_peer_includes_task_and_footprint_demand(_Config) ->
    StoreID = store1,
    TaskPeer = task_peer,
    FootprintPeer = footprint_peer,
    Task = #task{
        offset = 0,
        store_id = StoreID,
        sources = [#task_source{peer = TaskPeer}]
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
    Reservation = arweave_sync_footprint:test_reservation(
        StoreID, Footprint, [Source], FootprintPeer, 0, bound
    ),
    Footprints = arweave_sync_footprint:test_state([Reservation]),
    Dispatches0 = start_dispatch(Footprints, #{}, [], States),
    Dispatch = maps:get(StoreID, Dispatches0),
    Dispatches = #{StoreID => Dispatch#store_dispatch{disk_ready = true}},
    ?assertEqual(
        #{
            TaskPeer => [StoreID],
            FootprintPeer => [StoreID]
        },
        stores_by_peer(Dispatches)
    ),
    BlockedDispatches = #{
        StoreID => Dispatch#store_dispatch{
            disk_ready = false
        }
    },
    ?assertEqual(#{}, stores_by_peer(BlockedDispatches)).

%% @doc Popping the only queued task removes its store from the current dispatch
%% pass.
pop_work_returns_queued_task(_Config) ->
    Task = #task{
        offset = 0,
        sources = [#task_source{peer = peer}]
    },
    {ok, 1, State} = admit(store1, Task, new()),
    Dispatch = test_dispatch(State),
    ?assertMatch({#task{}, #{}}, pop_work(store1, Dispatch)).

%% @doc Skipping work in one dispatch pass does not remove it from the
%% persistent queue.
work_removed_from_a_pass_returns_in_the_next_pass(_Config) ->
    First = #task{
        offset = 0,
        sources = [#task_source{peer = peer_a}]
    },
    Second = #task{
        offset = ?DATA_CHUNK_SIZE,
        sources = [#task_source{peer = peer_b}]
    },
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

%% @doc Drain estimates distinguish starved samples from backed-up writes and
%% recover promptly.
drain_rate_uses_tick_samples(_Config) ->
    SampleStartMs = 1_000,
    SampleIntervalMs = 10_000,
    InitialRate = 100,
    State0 = #store_state{
        drain_rate = InitialRate,
        drain_sample_started_ms = SampleStartMs,
        completed_writes_since_sample = 200
    },

    %% Twenty chunks/s without pending writes is only a lower bound, so it
    %% cannot reduce the existing 100-chunk/s estimate.
    State1 = sample_drain_rate(
        0, SampleStartMs + SampleIntervalMs, State0
    ),
    ?assertEqual(100, State1#store_state.drain_rate),

    %% The same twenty-chunk/s sample with pending writes is authoritative.
    %% The half-weight EMA moves the estimate from 100 to 60 chunks/s.
    State2 = sample_drain_rate(
        1,
        SampleStartMs + 2 * SampleIntervalMs,
        State1#store_state{
            completed_writes_since_sample = 200,
            drain_sample_starved = false
        }
    ),
    ?assertEqual(60.0, State2#store_state.drain_rate),
    ?assertEqual(300, claim_limit(State2)),

    %% Eight hundred completions with no pending writes prove at least eighty
    %% chunks/s and raise the lower bound directly.
    State3 = sample_drain_rate(
        0,
        SampleStartMs + 3 * SampleIntervalMs,
        State2#store_state{completed_writes_since_sample = 800}
    ),
    ?assertEqual(80.0, State3#store_state.drain_rate),

    %% A higher authoritative sample takes effect immediately so a recovered
    %% store is not held to its earlier capacity for several long windows.
    IncreasedState = sample_drain_rate(
        1,
        SampleStartMs + 4 * SampleIntervalMs,
        State3#store_state{
            completed_writes_since_sample = 1000,
            drain_sample_starved = false
        }
    ),
    ?assertEqual(100.0, IncreasedState#store_state.drain_rate),

    %% A fully idle interval carries no new capacity evidence.
    State4 = sample_drain_rate(
        0, SampleStartMs + 5 * SampleIntervalMs, IncreasedState
    ),
    ?assertEqual(100.0, State4#store_state.drain_rate),

    %% Pending writes with no completions provide a zero-rate sample, moving
    %% the half-weight EMA down to forty chunks/s.
    State5 = sample_drain_rate(
        1,
        SampleStartMs + 6 * SampleIntervalMs,
        State4#store_state{drain_sample_starved = false}
    ),
    ?assertEqual(50.0, State5#store_state.drain_rate).

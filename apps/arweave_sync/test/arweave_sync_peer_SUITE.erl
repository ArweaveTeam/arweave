-module(arweave_sync_peer_SUITE).
-test_category([fast]).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_sync/include/arweave_sync.hrl").
-include("arweave_sync_peer.hrl").

suite() -> [{timetrap, {seconds, 60}}].

all() ->
    [
        dispatch_tracks_fetch_capacity,
        queued_footprint_tasks_share_peer_queue_with_fetches,
        snapshot_restores_queued_task_limit,
        queue_max_length_preserves_bounded_store_exploration,
        priority_counts_own_fetches_as_available,
        compare_priorities_margins,
        dispatch_splits_peer_capacity_across_stores,
        stores_with_tasks_remain_in_target_split,
        source_capacity_filter_precedes_load,
        add_fetch_result_accumulates_fetch_results,
        sample_spans_ticks,
        failure_pressure_weights_fetch_time,
        queue_max_length_tracks_measured_goodput,
        queue_sized_from_recent_goodput,
        tick_covers_active_peers,
        worker_time_pressure_isolated,
        flat_driven_goodput_bounds_concurrency
    ].

init_per_testcase(_Case, Config) ->
    arweave_sync_test_deps:setup(),
    Config.

end_per_testcase(_Case, _Config) ->
    arweave_sync_test_deps:cleanup().

%%====================================================================
%% Test cases
%%====================================================================

%% @doc A peer's task load and its active fetch capacity are tracked separately.
dispatch_tracks_fetch_capacity(_Config) ->
    StoreID = store,
    %% Two concurrent fetches fill this peer's two-request cap while its task
    %% load remains the two tasks already in its peer queue.
    Plan0 = arweave_sync_peer:new_plan(#{peer => 2}, #{peer => 2}),
    ?assertEqual(0.0, arweave_sync_peer:load(peer, Plan0)),
    Task = #task{state = queued, store_id = StoreID},
    Plan1 = arweave_sync_peer:enqueue_tasks(peer, [Task], Plan0),
    %% The task limit combines two active and two queued tasks.
    ?assertEqual(1 / 4, arweave_sync_peer:load(peer, Plan1)),
    Plan2 = arweave_sync_peer:start_task(peer, Task, Plan1),
    ?assert(arweave_sync_peer:can_start_fetch(peer, Plan1)),
    Plan3 = arweave_sync_peer:enqueue_tasks(peer, [Task], Plan2),
    ?assertEqual(1 / 2, arweave_sync_peer:load(peer, Plan3)),
    Plan4 = arweave_sync_peer:start_task(peer, Task, Plan3),
    ?assertNot(arweave_sync_peer:can_start_fetch(peer, Plan4)).

%% @doc Queued footprint tasks share the peer's task capacity and can start
%% within the active-fetch cap.
queued_footprint_tasks_share_peer_queue_with_fetches(_Config) ->
    Peer = peer,
    StoreID = store,
    Footprint = #footprint{store_id = StoreID},
    Source = #task_source{peer = Peer},
    Task = #task{
        state = queued,
        store_id = StoreID,
        footprint = Footprint,
        sources = [Source]
    },
    %% The two-task queue limit permits two queued replacements independently of the
    %% two active fetches.
    Plan0 = arweave_sync_peer:new_plan(#{Peer => 2}, #{Peer => 2}),
    Plan1 = arweave_sync_peer:enqueue_tasks(Peer, [Task, Task], Plan0),
    ?assertEqual(1 / 2, arweave_sync_peer:load(Peer, Plan1)),
    %% A new reservation cannot claim more peer queue capacity, but an already-queued
    %% task may start a fetch without adding to the peer's task count.
    Reservation = #footprint_reservation{store_id = StoreID},
    ?assertNot(arweave_sync_peer:has_capacity(Reservation, Source, Plan1)),
    ?assert(arweave_sync_peer:can_start_fetch(Peer, Plan1)),
    Plan2 = arweave_sync_peer:start_task(Peer, Task, Plan1),
    ?assertEqual(1 / 2, arweave_sync_peer:load(Peer, Plan2)),
    ?assert(arweave_sync_peer:has_capacity(Task, Source, Plan2)),
    Plan3 = arweave_sync_peer:start_task(Peer, Task, Plan2),
    ?assertNot(arweave_sync_peer:can_start_fetch(Peer, Plan3)).

%% @doc A new dispatch pass restores queued tasks' task load and queue
%% capacity.
snapshot_restores_queued_task_limit(_Config) ->
    Peer = peer,
    StoreID = store,
    Task = #task{
        state = queued,
        peer = Peer,
        store_id = StoreID,
        footprint = #footprint{store_id = StoreID}
    },
    %% One persistent child occupies one place in the eight-task bootstrap
    %% queue of a peer with a cap of two.
    PeerState = active_peer(#cap_control{cap = 2}),
    State = #state{peers = #{Peer => PeerState#peer{queue = queue:from_list([Task])}}},
    Plan = arweave_sync_peer:snapshot(#{}, State),
    ?assertEqual(1 / 10, arweave_sync_peer:load(Peer, Plan)),
    ?assertEqual(7, arweave_sync_peer:queue_capacity(Peer, Plan)).

%% @doc A reservation whose own fetches fill its peer's room for the store is
%% busy, not blocked; one with nothing in flight is blocked.
priority_counts_own_fetches_as_available(_Config) ->
    Peer = peer,
    Source = #task_source{peer = Peer},
    %% A one-request cap and a one-task queue: one task queued for store_a
    %% leaves the peer no room for the store.
    Plan0 = arweave_sync_peer:new_plan(#{Peer => 1}, #{Peer => 1}),
    Task = #task{
        store_id = store_a,
        footprint = #footprint{store_id = store_a},
        sources = [Source]
    },
    Plan = arweave_sync_peer:enqueue_tasks(Peer, [Task], Plan0),
    Busy = arweave_sync_peer:priority(
        #footprint_reservation{store_id = store_a, active_tasks = 1},
        Source,
        Plan
    ),
    Idle = arweave_sync_peer:priority(
        #footprint_reservation{store_id = store_a}, Source, Plan
    ),
    ?assertEqual(better, arweave_sync_peer:compare_priorities(Busy, Idle)),
    ?assertEqual(worse, arweave_sync_peer:compare_priorities(Idle, Busy)),
    %% Before the task was queued the peer had room, so the same idle
    %% reservation was available.
    IdleWithRoom = arweave_sync_peer:priority(
        #footprint_reservation{store_id = store_a}, Source, Plan0
    ),
    ?assertEqual(
        better, arweave_sync_peer:compare_priorities(IdleWithRoom, Idle)
    ).

%% @doc Peer availability decides first, then a load more than ten percent
%% lower, then a concurrency cap more than ten percent higher.
compare_priorities_margins(_Config) ->
    P = fun arweave_sync_peer:peer_priority/3,
    Compare = fun arweave_sync_peer:compare_priorities/2,
    %% An available peer beats an unavailable one whatever their loads.
    ?assertEqual(better, Compare(P(true, 0.9, 1), P(false, 0.0, 100))),
    %% 0.44 is more than ten percent below 0.50, 0.46 is not, and 0.56 is
    %% more than ten percent above it.
    ?assertEqual(better, Compare(P(true, 0.44, 100), P(true, 0.50, 100))),
    ?assertEqual(close, Compare(P(true, 0.46, 100), P(true, 0.50, 100))),
    ?assertEqual(worse, Compare(P(true, 0.56, 100), P(true, 0.50, 100))),
    %% At equal loads a cap of 111 is more than ten percent above 100, and a
    %% cap of 110 is not.
    ?assertEqual(better, Compare(P(true, 0.5, 111), P(true, 0.5, 100))),
    ?assertEqual(close, Compare(P(true, 0.5, 110), P(true, 0.5, 100))).

%% @doc A full peer queue prevents binding more tasks for either an existing
%% or a new store.
queue_max_length_preserves_bounded_store_exploration(_Config) ->
    Peer = peer,
    Source = #task_source{peer = Peer},
    %% The one-task queue limit permits one speculative store, not a second.
    Plan0 = arweave_sync_peer:new_plan(#{Peer => 1}, #{Peer => 1}),
    Task = #task{
        store_id = store_a,
        footprint = #footprint{store_id = store_a},
        sources = [Source]
    },
    ?assert(arweave_sync_peer:has_capacity(Task, Source, Plan0)),
    Plan = arweave_sync_peer:enqueue_tasks(Peer, [Task], Plan0),
    ?assertNot(arweave_sync_peer:has_capacity(Task, Source, Plan)),
    ?assertNot(
        arweave_sync_peer:has_capacity(
            Task#task{store_id = store_b}, Source, Plan
        )
    ).

%% @doc Active and queued peer capacity is shared across eligible stores.
dispatch_splits_peer_capacity_across_stores(_Config) ->
    %% Four active and four queued tasks split into four tasks per
    %% store.
    Plan0 = arweave_sync_peer:set_store_task_targets(
        peer,
        [store_a, store_b],
        arweave_sync_peer:new_plan(#{peer => 4}, #{peer => 4})
    ),
    Task = #task{state = queued, store_id = store_a},
    PlanA = arweave_sync_peer:enqueue_tasks(peer, [Task], Plan0),
    Plan1 = arweave_sync_peer:start_task(peer, Task, PlanA),
    ?assertEqual(1 / 4, arweave_sync_peer:store_load(peer, store_a, Plan1)),
    ?assertEqual(0.0, arweave_sync_peer:store_load(peer, store_b, Plan1)),
    %% One fetching task plus three queued tasks fills store A's share.
    Footprint = #footprint{store_id = store_a},
    FootprintTask = #task{
        state = queued,
        store_id = store_a,
        footprint = Footprint
    },
    Plan2 = arweave_sync_peer:enqueue_tasks(
        peer, [FootprintTask, FootprintTask, FootprintTask], Plan1
    ),
    ?assertNot(arweave_sync_peer:has_capacity(
        #task{store_id = store_a}, #task_source{peer = peer}, Plan2
    )).

%% @doc A store's existing tasks remain counted when another store becomes
%% ready.
stores_with_tasks_remain_in_target_split(_Config) ->
    Peer = peer,
    StoreA = store_a,
    StoreB = store_b,
    %% Four active and four queued tasks fill the peer's task limit while store
    %% A is the only ready store.
    Plan0 = arweave_sync_peer:set_store_task_targets(
        Peer,
        [StoreA],
        arweave_sync_peer:new_plan(#{Peer => 4}, #{Peer => 4})
    ),
    Task = #task{state = queued, store_id = StoreA},
    PlanA = arweave_sync_peer:enqueue_tasks(
        Peer,
        lists:duplicate(4, Task),
        Plan0
    ),
    Plan1 = lists:foldl(
        fun(_, Acc) -> arweave_sync_peer:start_task(Peer, Task, Acc) end,
        PlanA,
        lists:seq(1, 4)
    ),
    FootprintTask = #task{
        state = queued,
        store_id = StoreA,
        footprint = #footprint{store_id = StoreA}
    },
    Plan1A = arweave_sync_peer:enqueue_tasks(
        Peer,
        [FootprintTask, FootprintTask, FootprintTask, FootprintTask],
        Plan1
    ),
    Plan2 = arweave_sync_peer:set_store_task_targets(
        #{Peer => [StoreB]}, Plan1A
    ),
    ?assertNot(arweave_sync_peer:has_capacity(
        #task{store_id = StoreA}, #task_source{peer = Peer}, Plan2
    )),
    ?assertNot(arweave_sync_peer:has_capacity(
        #task{store_id = StoreB}, #task_source{peer = Peer}, Plan2
    )).

%% @doc Sources without fetch capacity are excluded before load-based peer
%% selection.
source_capacity_filter_precedes_load(_Config) ->
    StoreID = store,
    BlockedPeer = blocked_peer,
    ReadyPeer = ready_peer,
    Sources = [
        #task_source{peer = BlockedPeer},
        #task_source{peer = ReadyPeer}
    ],
    %% The first peer has the lower tie-break value but its only slot is full.
    Plan0 = arweave_sync_peer:new_plan(
        #{BlockedPeer => 1, ReadyPeer => 2},
        #{BlockedPeer => 1, ReadyPeer => 2}
    ),
    BlockedTask = #task{state = queued, store_id = StoreID},
    PlanA = arweave_sync_peer:enqueue_tasks(BlockedPeer, [BlockedTask], Plan0),
    Plan = arweave_sync_peer:start_task(BlockedPeer, BlockedTask, PlanA),
    ReadySources = lists:filter(
        fun(#task_source{peer = Peer}) ->
            arweave_sync_peer:can_start_fetch(Peer, Plan)
        end,
        Sources
    ),
    [#task_source{peer = ReadyPeer}] = ReadySources,
    ?assertMatch(
        {ok, #task_source{peer = ReadyPeer}},
        arweave_sync_peer:best_source(StoreID, ReadySources, Plan)
    ).

%% @doc Fetch results accumulate fetched bytes and timings, clearing only
%% timings on each tick.
add_fetch_result_accumulates_fetch_results(_Config) ->
    Peer = {1, 2, 3, 4, 5},
    %% Two outcomes cover every timing category and only one fetches a chunk.
    FirstTiming = #fetch_timing{productive_ms = 100, reject_ms = 20},
    SecondTiming = #fetch_timing{productive_ms = 50, timeout_ms = 30},
    State1 = arweave_sync_peer:add_fetch_result(
        Peer, ?DATA_CHUNK_SIZE, FirstTiming, arweave_sync_peer:new()
    ),
    State2 = arweave_sync_peer:add_fetch_result(Peer, 0, SecondTiming, State1),
    PeerState = maps:get(Peer, State2#state.peers),
    ?assertEqual(?DATA_CHUNK_SIZE, PeerState#peer.fetched_bytes),
    ?assertEqual(
        #fetch_timing{
            productive_ms = 150,
            reject_ms = 20,
            timeout_ms = 30
        },
        PeerState#peer.fetch_timing
    ),
    State3 = arweave_sync_peer:tick([Peer], 1000, State2),
    PeerState2 = maps:get(Peer, State3#state.peers),
    ?assertEqual(?DATA_CHUNK_SIZE, PeerState2#peer.fetched_bytes),
    ?assertEqual(#fetch_timing{}, PeerState2#peer.fetch_timing).

%% @doc A sample reports the bytes fetched and the time since the peer's
%% previous tick snapshot, and nothing without one.
sample_spans_ticks(_Config) ->
    ?assertEqual({200, 2000}, arweave_sync_peer:sample_goodput({100, 1000}, {300, 3000})),
    ?assertEqual(undefined, arweave_sync_peer:sample_goodput(undefined, {50, 3000})).

%% @doc Failure pressure is the share of fetch time spent on failures of any
%% kind.
failure_pressure_weights_fetch_time(_Config) ->
    Timing = #fetch_timing{
        productive_ms = 9000,
        reject_ms = 250,
        timeout_ms = 500,
        client_error_ms = 250
    },
    ?assertEqual({0.1, 1000}, arweave_sync_peer:failure_pressure(Timing)),
    %% Nine two-second successes and one 250 ms 429 yield 1.37% pressure.
    ?assertEqual(
        {250 / 18250, 250},
        arweave_sync_peer:failure_pressure(#fetch_timing{
            productive_ms = 18000, reject_ms = 250
        })
    ).

%% @doc The peer queue limit follows measured goodput while retaining a minimum
%% probe.
queue_max_length_tracks_measured_goodput(_Config) ->
    HundredTaskRate = 100 * ?DATA_CHUNK_SIZE / ?QUEUE_TARGET_DURATION_MS,
    EightyTaskRate = 80 * ?DATA_CHUNK_SIZE / ?QUEUE_TARGET_DURATION_MS,
    ?assertEqual(100, arweave_sync_peer:queue_max_length(HundredTaskRate)),
    ?assertEqual(80, arweave_sync_peer:queue_max_length(EightyTaskRate)),
    ?assertEqual(?CONCURRENCY_CAP_INITIAL, arweave_sync_peer:queue_max_length(0.0)).

%% @doc A peer's queue holds four seconds of its mean goodput over its last six
%% samples with work in flight, driven or not, and falls back to the
%% exploration bootstrap before any is recorded.
queue_sized_from_recent_goodput(_Config) ->
    Tick = fun(Sample, FetchingCount, Goodputs) ->
        tick_peer(
            #peer{recent_goodputs = Goodputs}, Sample, #fetch_timing{}, false, FetchingCount
        )
    end,
    QueueMaxLength = fun(PeerState) ->
        {_Cap, Length} = limits(peer, #state{peers = #{peer => PeerState}}),
        Length
    end,
    RecentGoodputs = fun(#peer{recent_goodputs = Goodputs}) -> Goodputs end,
    Rates = [chunks_per_second(100), chunks_per_second(300)],
    %% 100 and 300 chunks/s average 200 chunks/s: 800 chunks in four seconds.
    ?assertEqual(800, QueueMaxLength(Tick(undefined, 0, Rates))),
    ?assertEqual(?CONCURRENCY_CAP_INITIAL, QueueMaxLength(Tick(undefined, 0, []))),
    %% An undriven sample with work in flight counts.
    ?assertEqual(
        [chunks_per_second(500) | Rates],
        RecentGoodputs(Tick(sample(chunks_per_second(500)), 3, Rates))
    ),
    %% So does a stalled one, as zero; an idle one with nothing in flight
    %% does not.
    ?assertEqual([0.0 | Rates], RecentGoodputs(Tick({0.0, 1000}, 2, Rates))),
    ?assertEqual(Rates, RecentGoodputs(Tick({0.0, 1000}, 0, Rates))),
    %% Only the newest six are kept.
    Six = lists:duplicate(6, chunks_per_second(100)),
    ?assertEqual(
        [chunks_per_second(200) | lists:duplicate(5, chunks_per_second(100))],
        RecentGoodputs(Tick(sample(chunks_per_second(200)), 0, Six))
    ).

%% Caps and queue limits cover exactly the active peers, while cap control is
%% retained when a peer leaves.
tick_covers_active_peers(_Config) ->
    A = {1, 1, 1, 1, 1},
    B = {2, 2, 2, 2, 2},
    %% About 100 MiB/s in bytes/ms derives a 1600-task queue limit:
    %% 400 chunks/s * 4 seconds.
    Rate = 104857.6,
    MeasuredQueueMaxLength = round(
        Rate * ?QUEUE_TARGET_DURATION_MS / ?DATA_CHUNK_SIZE
    ),
    Control = #cap_control{cap = 100},
    State0 = #state{
        peers = #{A => active_peer(Control, [Rate]), B => active_peer(Control, [Rate])}
    },
    Productive = #fetch_timing{productive_ms = 1000},
    %% One sample does not complete a baseline window.
    State1 = fetch_tick([A, B], #{A => Productive, B => Productive}, Rate, 1, State0),
    ?assertEqual({100, MeasuredQueueMaxLength}, limits(A, State1)),
    ?assertEqual({100, MeasuredQueueMaxLength}, limits(B, State1)),
    %% Forty percent failed fetch time reaches the halving clamp for B alone.
    Failed = #fetch_timing{productive_ms = 600, timeout_ms = 400},
    State2 = fetch_tick([A, B], #{A => Productive, B => Failed}, Rate, 2, State1),
    ?assertMatch({100, _}, limits(A, State2)),
    ?assertMatch({50, _}, limits(B, State2)),
    %% B leaves the active set: it has no cap in the dispatch snapshot, but
    %% its cap control remains for when it returns.
    State3 = fetch_tick([A], #{A => Productive}, Rate, 3, State2),
    ?assertEqual({?CONCURRENCY_CAP_MIN, ?CONCURRENCY_CAP_INITIAL}, limits(B, State3)),
    State4 = fetch_tick([A, B], #{A => Productive, B => Productive}, Rate, 4, State3),
    ?assertMatch({50, _}, limits(B, State4)),
    %% Lower goodput contracts queued work without changing an undriven cap.
    LowRate = 5242.88,
    LowRateQueueMaxLength =
        round((Rate + LowRate) / 2 * ?QUEUE_TARGET_DURATION_MS / ?DATA_CHUNK_SIZE),
    Low = tick_peer(
        #peer{control = #cap_control{cap = 200}, recent_goodputs = [Rate]},
        sample(LowRate),
        Productive,
        false,
        0
    ),
    ?assertEqual({200, LowRateQueueMaxLength}, limits(peer, #state{peers = #{peer => Low}})),
    %% Unmeasured peers receive an eight-task queue bootstrap.
    Fresh = arweave_sync_peer:tick([A], 1000, arweave_sync_peer:new()),
    ?assertEqual({?CONCURRENCY_CAP_INITIAL, ?CONCURRENCY_CAP_INITIAL}, limits(A, Fresh)).

%% Worker-time pressure is isolated by peer, and a clean interval after a
%% failed one measures the cut cap before changing it.
worker_time_pressure_isolated(_Config) ->
    Healthy = {1, 1, 1, 1, 1},
    TimedOut = {2, 2, 2, 2, 2},
    Rate = 104857.6,
    Control = #cap_control{cap = 100},
    State0 = #state{
        peers = #{Healthy => active_peer(Control), TimedOut => active_peer(Control)}
    },
    Timings = #{
        Healthy => #fetch_timing{productive_ms = 1000},
        TimedOut => #fetch_timing{timeout_ms = 1000}
    },
    State1 = fetch_tick([Healthy, TimedOut], Timings, Rate, 1, State0),
    ?assertMatch({100, _}, limits(Healthy, State1)),
    ?assertMatch({50, _}, limits(TimedOut, State1)),
    Recovery = #{TimedOut => #fetch_timing{productive_ms = 1000}},
    State2 = fetch_tick([TimedOut], Recovery, Rate, 2, State1),
    ?assertMatch({50, _}, limits(TimedOut, State2)).

%% Flat driven goodput drops the step above the seed after a full window; the
%% seed has no lower cap to test, so the next probe soon steps upward again.
flat_driven_goodput_bounds_concurrency(_Config) ->
    Peer = {5, 5, 5, 5, 5},
    TickMs = 1000,
    RTTMs = 250,
    Caps = caps_after_ticks(Peer, lists:duplicate(20, 400), RTTMs, TickMs),
    %% Six samples measure the seed; the doubled step to sixteen skips
    %% one and measures six, finding nothing; the seed has no lower cap to
    %% test, so the probe ends, and after one baseline window the next probe
    %% steps upward in a base step.
    ?assertEqual(
        lists:duplicate(5, 8) ++ lists:duplicate(7, 16) ++ lists:duplicate(7, 8) ++ [9],
        Caps
    ).

%%====================================================================
%% Helpers
%%====================================================================

%% @doc A one-second sample at Rate bytes/ms.
sample(Rate) ->
    {Rate * 1000, 1000}.

%% @doc Goodput in bytes/ms for a number of chunks per second.
chunks_per_second(Chunks) ->
    Chunks * ?DATA_CHUNK_SIZE / 1000.

%% @doc A peer that was active at a tick at time zero with Control and the
%% recent goodputs Goodputs.
active_peer(Control) ->
    active_peer(Control, []).

active_peer(Control, Goodputs) ->
    #peer{control = Control, last_tick = {0, 0}, recent_goodputs = Goodputs}.

%% @doc Tick PeerState with Sample, Timing, Driven and FetchingCount as its
%% inputs since its previous tick; a peer without a sample was not active.
tick_peer(PeerState, Sample, Timing, Driven, FetchingCount) ->
    {LastTick, Bytes, NowMs} =
        case Sample of
            undefined -> {undefined, 0, 1000};
            {SampleBytes, Ms} -> {{0, 0}, SampleBytes, Ms}
        end,
    arweave_sync_peer:tick_peer({9, 9, 9, 9, 9}, NowMs, PeerState#peer{
        fetched_bytes = Bytes,
        fetch_timing = Timing,
        last_tick = LastTick,
        driven = Driven,
        fetching_count = FetchingCount
    }).

%% @doc Fetch at Rate for one second from each peer in Timings with its timing,
%% mark it driven, then tick the active peers at Second.
fetch_tick(ActivePeers, Timings, Rate, Second, State) ->
    State2 = maps:fold(
        fun(Peer, Timing, Acc) ->
            mark_driven(Peer, arweave_sync_peer:add_fetch_result(Peer, Rate * 1000, Timing, Acc))
        end,
        State,
        Timings
    ),
    arweave_sync_peer:tick(ActivePeers, Second * 1000, State2).

mark_driven(Peer, #state{peers = Peers} = State) ->
    State#state{
        peers = maps:update_with(Peer, fun(P) -> P#peer{driven = true} end, Peers)
    }.

%% @doc Return the cap and queue length a dispatch snapshot gives Peer.
limits(Peer, State) ->
    Plan = arweave_sync_peer:snapshot(#{}, State),
    #peer_plan{concurrency_cap = Cap, queue_max_length = QueueMaxLength} =
        arweave_sync_peer:peer_plan(Peer, Plan),
    {Cap, QueueMaxLength}.

caps_after_ticks(Peer, ChunksPerTick, RTTMs, TickMs) ->
    State0 = arweave_sync_peer:tick([Peer], 0, arweave_sync_peer:new()),
    {_State, _Now, Caps} = lists:foldl(
        fun(Chunks, {State, Now, Acc}) ->
            Now2 = Now + TickMs,
            FetchTiming = #fetch_timing{
                productive_ms = Chunks * RTTMs
            },
            State1 = arweave_sync_peer:add_fetch_result(
                Peer,
                Chunks * ?DATA_CHUNK_SIZE,
                FetchTiming,
                State
            ),
            State2 = arweave_sync_peer:tick([Peer], Now2, mark_driven(Peer, State1)),
            {Cap, _QueueMaxLength} = limits(Peer, State2),
            {State2, Now2, [Cap | Acc]}
        end,
        {State0, 0, []},
        ChunksPerTick
    ),
    lists:reverse(Caps).

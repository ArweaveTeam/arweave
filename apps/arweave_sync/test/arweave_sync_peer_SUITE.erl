-module(arweave_sync_peer_SUITE).
-test_category([fast]).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_sync/include/arweave_sync.hrl").
-include("arweave_sync_peer.hrl").
-import(arweave_sync_peer, [
    aggregate_delivery_entry/4,
    aggregate_goodput/1,
    best_source/3,
    bound_aggregate_goodput/2,
    enqueue_tasks/3,
    evolve_cap_control/5,
    failure_pressure/1,
    goodput_improved/2,
    goodput_rate/1,
    has_capacity/2,
    has_capacity/3,
    load/2,
    new/0,
    peer_dispatch/2,
    queue_has_capacity/2,
    queue_max_length/1,
    recompute/5,
    record_result/4,
    set_store_task_targets/2,
    set_store_task_targets/3,
    start_dispatch/3,
    start_task/3,
    store_capacity/3,
    store_load/3,
    test_dispatch/2,
    tick/5,
    update_delivery/5
]).

suite() -> [{timetrap, {seconds, 60}}].

all() ->
    [
        dispatch_tracks_fetch_capacity,
        queued_footprint_tasks_share_peer_queue_with_fetches,
        start_dispatch_restores_queued_assignment_limit,
        queue_max_length_preserves_bounded_store_exploration,
        dispatch_splits_peer_capacity_across_stores,
        assigned_stores_remain_in_target_split,
        source_capacity_filter_precedes_load,
        record_result_accumulates_observations,
        update_delivery,
        aggregate_goodput_bounds_peer_sum,
        aggregate_delivery_smooths_bucket_boundary,
        goodput_probe,
        small_failure_pressure_preserves_concurrency,
        goodput_backoff_settles_before_retry,
        queue_max_length_tracks_measured_delivery,
        recompute,
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

%% @doc Queued assignment load and active fetch capacity are tracked separately.
dispatch_tracks_fetch_capacity(_Config) ->
    StoreID = store,
    %% Two concurrent fetches fill this peer's two-request cap while assignment
    %% load remains the two tasks already admitted to its runnable queue.
    Dispatches0 = test_dispatch(#{peer => 2}, #{peer => 2}),
    ?assertEqual(0.0, load(peer, Dispatches0)),
    Task = #task{state = queued, store_id = StoreID},
    Dispatches1 = enqueue_tasks(peer, [Task], Dispatches0),
    %% The assignment limit combines two active and two queued tasks.
    ?assertEqual(1 / 4, load(peer, Dispatches1)),
    Dispatches2 = start_task(peer, Task, Dispatches1),
    ?assert(has_capacity(peer, Dispatches1)),
    Dispatches3 = enqueue_tasks(peer, [Task], Dispatches2),
    ?assertEqual(1 / 2, load(peer, Dispatches3)),
    Dispatches4 = start_task(peer, Task, Dispatches3),
    ?assertNot(has_capacity(peer, Dispatches4)).

%% @doc Queued footprint tasks share assignment capacity and can start within
%% the active-fetch cap.
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
    %% two active fetch slots.
    Dispatches0 = test_dispatch(#{Peer => 2}, #{Peer => 2}),
    Dispatches1 = enqueue_tasks(Peer, [Task, Task], Dispatches0),
    ?assertEqual(1 / 2, load(Peer, Dispatches1)),
    %% A new reservation cannot claim more peer queue capacity, but an already-queued
    %% task may consume a fetch slot without increasing assigned work.
    Reservation = #footprint_reservation{store_id = StoreID},
    ?assertNot(has_capacity(Reservation, Source, Dispatches1)),
    ?assert(has_capacity(Peer, Dispatches1)),
    Dispatches2 = start_task(Peer, Task, Dispatches1),
    ?assertEqual(1 / 2, load(Peer, Dispatches2)),
    ?assert(has_capacity(Task, Source, Dispatches2)),
    Dispatches3 = start_task(Peer, Task, Dispatches2),
    ?assertNot(has_capacity(Peer, Dispatches3)).

%% @doc A new dispatch pass restores queued tasks' assignment load and store
%% capacity.
start_dispatch_restores_queued_assignment_limit(_Config) ->
    Peer = peer,
    StoreID = store,
    Task = #task{
        state = queued,
        peer = Peer,
        store_id = StoreID,
        footprint = #footprint{store_id = StoreID}
    },
    %% One persistent child occupies half of the two-task queue limit.
    State = #state{
        caps = #{Peer => 2},
        queue_max_lengths = #{Peer => 2}
    },
    Dispatches = start_dispatch(#{}, [Task], State),
    ?assertEqual(1 / 4, load(Peer, Dispatches)),
    ?assertEqual(1, store_capacity(Peer, StoreID, Dispatches)).

%% @doc A full peer queue prevents speculative assignment to either an existing
%% or a new store.
queue_max_length_preserves_bounded_store_exploration(_Config) ->
    Peer = peer,
    Source = #task_source{peer = Peer},
    %% The one-task queue limit permits one speculative store, not a second.
    Dispatches0 = test_dispatch(#{Peer => 1}, #{Peer => 1}),
    ?assert(queue_has_capacity(store_a, peer_dispatch(Peer, Dispatches0))),
    Task = #task{
        store_id = store_a,
        footprint = #footprint{store_id = store_a},
        sources = [Source]
    },
    Dispatches = enqueue_tasks(Peer, [Task], Dispatches0),
    ?assertNot(
        queue_has_capacity(
            store_a, peer_dispatch(Peer, Dispatches)
        )
    ),
    ?assertNot(
        queue_has_capacity(
            store_b, peer_dispatch(Peer, Dispatches)
        )
    ).

%% @doc Active and queued peer capacity is shared across eligible stores.
dispatch_splits_peer_capacity_across_stores(_Config) ->
    %% Four active and four queued tasks split into four assigned tasks per
    %% store.
    Dispatches0 = set_store_task_targets(
        peer,
        [store_a, store_b],
        test_dispatch(#{peer => 4}, #{peer => 4})
    ),
    Task = #task{state = queued, store_id = store_a},
    DispatchesA = enqueue_tasks(peer, [Task], Dispatches0),
    Dispatches1 = start_task(peer, Task, DispatchesA),
    ?assertEqual(1 / 4, store_load(peer, store_a, Dispatches1)),
    ?assertEqual(0.0, store_load(peer, store_b, Dispatches1)),
    %% One fetching task plus three queued tasks fills store A's share.
    Footprint = #footprint{store_id = store_a},
    FootprintTask = #task{
        state = queued,
        store_id = store_a,
        footprint = Footprint
    },
    Dispatches2 = enqueue_tasks(
        peer, [FootprintTask, FootprintTask, FootprintTask], Dispatches1
    ),
    ?assertEqual(
        0,
        store_capacity(peer, store_a, Dispatches2)
    ).

%% @doc Existing assignments remain counted when another store becomes ready.
assigned_stores_remain_in_target_split(_Config) ->
    Peer = peer,
    StoreA = store_a,
    StoreB = store_b,
    %% Four active and four queued tasks fill the peer's combined assignment
    %% limit while store A is the only ready store.
    Dispatches0 = set_store_task_targets(
        Peer,
        [StoreA],
        test_dispatch(#{Peer => 4}, #{Peer => 4})
    ),
    Task = #task{state = queued, store_id = StoreA},
    DispatchesA = enqueue_tasks(
        Peer,
        lists:duplicate(4, Task),
        Dispatches0
    ),
    Dispatches1 = lists:foldl(
        fun(_, Acc) -> start_task(Peer, Task, Acc) end,
        DispatchesA,
        lists:seq(1, 4)
    ),
    FootprintTask = #task{
        state = queued,
        store_id = StoreA,
        footprint = #footprint{store_id = StoreA}
    },
    Dispatches1A = enqueue_tasks(
        Peer,
        [FootprintTask, FootprintTask, FootprintTask, FootprintTask],
        Dispatches1
    ),
    Dispatches2 = set_store_task_targets(
        #{Peer => [StoreB]}, Dispatches1A
    ),
    ?assertEqual(0, store_capacity(Peer, StoreA, Dispatches2)),
    ?assertEqual(0, store_capacity(Peer, StoreB, Dispatches2)).

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
    Dispatches0 = test_dispatch(
        #{BlockedPeer => 1, ReadyPeer => 2},
        #{BlockedPeer => 1, ReadyPeer => 2}
    ),
    BlockedTask = #task{state = queued, store_id = StoreID},
    DispatchesA = enqueue_tasks(BlockedPeer, [BlockedTask], Dispatches0),
    Dispatches = start_task(BlockedPeer, BlockedTask, DispatchesA),
    ReadySources = lists:filter(
        fun(Source) ->
            has_capacity(Source, Dispatches)
        end,
        Sources
    ),
    [#task_source{peer = ReadyPeer}] = ReadySources,
    ?assertMatch(
        {ok, #task_source{peer = ReadyPeer}},
        best_source(StoreID, ReadySources, Dispatches)
    ).

%% @doc Fetch results accumulate delivered bytes and timings, clearing only
%% timings on each tick.
record_result_accumulates_observations(_Config) ->
    Peer = {1, 2, 3, 4, 5},
    %% Two outcomes cover every timing category and only one delivers a chunk.
    FirstTiming = #fetch_timing{productive_ms = 100, reject_ms = 20},
    SecondTiming = #fetch_timing{productive_ms = 50, timeout_ms = 30},
    State1 = record_result(Peer, ?DATA_CHUNK_SIZE, FirstTiming, new()),
    State2 = record_result(Peer, 0, SecondTiming, State1),
    Observation = maps:get(Peer, State2#state.observations),
    ?assertEqual(?DATA_CHUNK_SIZE, Observation#observation.total_bytes),
    ?assertEqual(
        #fetch_timing{
            productive_ms = 150,
            reject_ms = 20,
            timeout_ms = 30
        },
        Observation#observation.fetch_timing
    ),
    State3 = tick([Peer], #{Peer => 1}, #{Peer => true}, 1000, State2),
    Observation2 = maps:get(Peer, State3#state.observations),
    ?assertEqual(?DATA_CHUNK_SIZE, Observation2#observation.total_bytes),
    ?assertEqual(#fetch_timing{}, Observation2#observation.fetch_timing).

%% @doc Delivery estimates use elapsed-time samples, decay driven idle peers and
%% retain unfed estimates.
update_delivery(_Config) ->
    Fast = {1, 1, 1, 1, 1},
    Slow = {2, 2, 2, 2, 2},
    %% Rates well above the one-chunk-per-tick noise floor so the snap does
    %% not engage: deliveries are in whole chunks.
    CS = float(?DATA_CHUNK_SIZE),
    Driven = #{Fast => 1, Slow => 1},
    D0 = update_delivery(
        [Fast, Slow],
        #{Fast => 0, Slow => 0},
        #{},
        Driven,
        0
    ),
    ?assertMatch({_, 0, undefined}, maps:get(Fast, D0)),
    %% Over 1000 ms Fast delivered 3 chunks, Slow 1 -> realized 3 vs 1 chunk/ms... in bytes/ms.
    D1 = update_delivery(
        [Fast, Slow],
        #{Fast => 3 * ?DATA_CHUNK_SIZE, Slow => ?DATA_CHUNK_SIZE},
        D0,
        Driven,
        1000
    ),
    ?assertEqual({3 * CS, 1000, 3 * CS / 1000}, maps:get(Fast, D1)),
    ?assertEqual({CS, 1000, CS / 1000}, maps:get(Slow, D1)),
    %% DRIVEN and idle (work in flight, no new bytes): real signal, the EWMA
    %% decays toward 0 using the decrease weight.
    D2 = update_delivery(
        [Fast],
        #{Fast => 3 * ?DATA_CHUNK_SIZE},
        D1,
        Driven,
        2000
    ),
    DecreasedRate = arweave_util:ema(3 * CS / 1000, 0.0, ?GOODPUT_DECREASE_ALPHA),
    ?assertEqual(DecreasedRate, element(3, maps:get(Fast, D2))),
    %% Slow left the active set, so its sampling state is removed.
    ?assertNot(maps:is_key(Slow, D2)),
    %% UNFED and idle (nothing in flight, no new bytes): a missing sample,
    %% not a zero one - the snapshot advances, the estimate is retained.
    D3 = update_delivery(
        [Fast],
        #{Fast => 3 * ?DATA_CHUNK_SIZE},
        D2,
        #{},
        3000
    ),
    ?assertEqual({3 * CS, 3000, DecreasedRate}, maps:get(Fast, D3)),
    %% Driven-idle ticks below the noise floor snap to 0.0 ("unmeasured"), so
    %% the peer returns to its exploration cap; the EWMA alone would decay
    %% asymptotically and never reach 0.0.
    D6 = lists:foldl(
        fun(N, Acc) ->
            update_delivery(
                [Fast],
                #{Fast => 3 * ?DATA_CHUNK_SIZE},
                Acc,
                Driven,
                3000 + N * 1000
            )
        end,
        D3,
        lists:seq(1, 12)
    ),
    ?assertMatch({_, _, +0.0}, maps:get(Fast, D6)),
    ?assertEqual(0.0, goodput_rate(undefined)),
    ?assertEqual(1.5, goodput_rate(1.5)).

%% @doc Aggregate goodput bounds peer estimates proportionally without
%% constraining a lone peer.
aggregate_goodput_bounds_peer_sum(_Config) ->
    PeerA = peer_a,
    PeerB = peer_b,
    %% Individual asymmetric estimates sum to five while the aggregate path
    %% measured four. Scaling by four-fifths preserves peer proportions.
    Goodput = bound_aggregate_goodput(#{PeerA => 3.0, PeerB => 2.0}, 4.0),
    ?assertEqual(2.4, maps:get(PeerA, Goodput)),
    ?assertEqual(1.6, maps:get(PeerB, Goodput)),
    ?assertEqual(4.0, lists:sum(maps:values(Goodput))),
    %% A single peer keeps its burst-tolerant per-peer estimate.
    ?assertEqual(
        undefined,
        aggregate_goodput({[PeerA], {0.0, 0, 1.0}})
    ).

%% @doc Aggregate delivery sampling smooths bursts that straddle measurement
%% boundaries.
aggregate_delivery_smooths_bucket_boundary(_Config) ->
    CS = float(?DATA_CHUNK_SIZE),
    Entry0 = aggregate_delivery_entry(0, 1, 0, undefined),
    %% Ten chunks in one second seed a ten-chunk/s aggregate estimate.
    Entry1 = aggregate_delivery_entry(10 * ?DATA_CHUNK_SIZE, 1, 1000, Entry0),
    ?assertEqual(10 * CS / 1000, element(3, Entry1)),
    %% A driven empty second retains four-fifths under the five-sample EWMA.
    Entry2 = aggregate_delivery_entry(
        10 * ?DATA_CHUNK_SIZE, 1, 2000, Entry1
    ),
    ?assertEqual(8 * CS / 1000, element(3, Entry2)).

%% A productive driven peer probes upward, accepts a material goodput gain,
%% and backs off when a larger cap leaves goodput flat.
goodput_probe(_Config) ->
    %% A 0.51% gain clears the 0.5% confirmation floor; a 0.1% gain does not.
    ?assert(goodput_improved(100.51, 100.0)),
    ?assertNot(goodput_improved(100.1, 100.0)),
    Control0 = #cap_control{cap = 100},
    Control1 = evolve_cap_control(100.0, 1000, 0.0, true, Control0),
    ?assertEqual(108, Control1#cap_control.cap),
    ?assertEqual(100, Control1#cap_control.baseline_cap),
    ?assertEqual(probe, Control1#cap_control.phase),
    Control2 = evolve_cap_control(105.0, 1000, 0.0, true, Control1),
    ?assertEqual(116, Control2#cap_control.cap),
    ?assertEqual(108, Control2#cap_control.baseline_cap),
    ?assertEqual(probe, Control2#cap_control.phase),
    Control3 = evolve_cap_control(105.0, 1000, 0.0, true, Control2),
    ?assertEqual(124, Control3#cap_control.cap),
    ?assertEqual(1, Control3#cap_control.observations),
    Control4 = evolve_cap_control(105.0, 1000, 0.0, true, Control3),
    ?assertEqual(132, Control4#cap_control.cap),
    ?assertEqual(2, Control4#cap_control.observations),
    Control5 = evolve_cap_control(105.0, 1000, 0.0, true, Control4),
    ?assertEqual(140, Control5#cap_control.cap),
    ?assertEqual(3, Control5#cap_control.observations),
    Control6 = evolve_cap_control(105.0, 1000, 0.0, true, Control5),
    ?assertEqual(108, Control6#cap_control.cap),
    ?assertEqual(settle, Control6#cap_control.phase),
    %% An idle or undriven peer does not advance its probe.
    ?assertEqual(
        Control6,
        evolve_cap_control(105.0, 0, 0.0, true, Control6)
    ),
    ?assertEqual(
        Control6,
        evolve_cap_control(105.0, 1000, 0.0, false, Control6)
    ),
    %% Ten percent worker-time pressure asks for 15% of the 105-request cap:
    %% remove fifteen whole requests, not the fractional sixteenth.
    PressureCut = evolve_cap_control(
        110.0,
        1000,
        0.1,
        true,
        #cap_control{cap = 105}
    ),
    ?assertEqual(90, PressureCut#cap_control.cap),
    %% One hundred percent failure pressure reaches the 50% cut clamp.
    FullCut = evolve_cap_control(0.0, 0, 1.0, true, Control0),
    ?assertEqual(50, FullCut#cap_control.cap),
    MinimumCut = evolve_cap_control(
        0.0,
        0,
        1.0,
        true,
        #cap_control{cap = ?CONCURRENCY_CAP_MIN}
    ),
    ?assertEqual(?CONCURRENCY_CAP_MIN, MinimumCut#cap_control.cap),
    %% The category split remains diagnostic; pressure uses their total time.
    Timing = #fetch_timing{
        productive_ms = 9000,
        reject_ms = 250,
        timeout_ms = 500,
        client_error_ms = 250
    },
    ?assertEqual({0.1, 1000}, failure_pressure(Timing)),
    %% Nine two-second successes and one 250 ms 429 yield 1.37% worker-time
    %% pressure. Scaling by 1.5 cuts a cap of 100 by 2.05%, rounding to 98.
    FastRejectTiming = #fetch_timing{
        productive_ms = 18000,
        reject_ms = 250
    },
    {FastRejectPressure, 250} = failure_pressure(FastRejectTiming),
    FastRejectControl = evolve_cap_control(
        100.0, 18000, FastRejectPressure, true, Control0
    ),
    ?assertEqual(98, FastRejectControl#cap_control.cap),
    ?assertEqual(cooldown, FastRejectControl#cap_control.phase).

%% @doc Small failure pressure cannot round a fractional cut up to a request.
small_failure_pressure_preserves_concurrency(_Config) ->
    %% One four-second failure among twenty equal-duration completions asks
    %% for a 7.5% cut: 0.6 requests at the eight-request exploration cap.
    Control = #cap_control{cap = 8},
    {Pressure, 4000} = failure_pressure(#fetch_timing{
        productive_ms = 19 * 4000,
        client_error_ms = 4000
    }),
    SmallCut = evolve_cap_control(100.0, 19 * 4000, Pressure, true, Control),
    ?assertEqual(8, SmallCut#cap_control.cap),
    ?assertEqual(cooldown, SmallCut#cap_control.phase),
    %% Ten percent pressure asks for 1.2 requests, so one is removed.
    LargerCut = evolve_cap_control(100.0, 1000, 0.1, true, Control),
    ?assertEqual(7, LargerCut#cap_control.cap),
    %% An all-failure interval must still halve this small cap.
    FullCut = evolve_cap_control(0.0, 0, 1.0, true, Control),
    ?assertEqual(4, FullCut#cap_control.cap).

%% A rejected probe holds the lower cap while old higher-cap completions and
%% their decreasing-rate EWMA contribution settle before the next baseline.
goodput_backoff_settles_before_retry(_Config) ->
    Control0 = #cap_control{
        cap = 121,
        phase = probe,
        baseline_rate = 110.0,
        baseline_cap = 105
    },
    %% Four non-improving observations explore another twenty-four requests,
    %% then discard the entire unconfirmed window and return to cap 105.
    Control1 = evolve_cap_control(90.0, 1000, 0.0, true, Control0),
    ?assertEqual(129, Control1#cap_control.cap),
    Control2 = evolve_cap_control(90.0, 1000, 0.0, true, Control1),
    ?assertEqual(137, Control2#cap_control.cap),
    Control3 = evolve_cap_control(90.0, 1000, 0.0, true, Control2),
    ?assertEqual(145, Control3#cap_control.cap),
    Control4 = evolve_cap_control(90.0, 1000, 0.0, true, Control3),
    ?assertEqual(105, Control4#cap_control.cap),
    ?assertEqual(settle, Control4#cap_control.phase),
    %% Three observations retain the cap; the fourth admits one eight-request
    %% additive probe.
    SettlingControl = lists:foldl(
        fun(_, Control) ->
            evolve_cap_control(90.0, 1000, 0.0, true, Control)
        end,
        Control4,
        lists:seq(1, 3)
    ),
    ?assertEqual(105, SettlingControl#cap_control.cap),
    ControlAfterSettle = evolve_cap_control(
        90.0, 1000, 0.0, true, SettlingControl
    ),
    ?assertEqual(113, ControlAfterSettle#cap_control.cap),
    ?assertEqual(105, ControlAfterSettle#cap_control.baseline_cap),
    ?assertEqual(90.0, ControlAfterSettle#cap_control.baseline_rate),
    ?assertEqual(probe, ControlAfterSettle#cap_control.phase).

%% @doc The peer queue limit follows measured delivery while retaining a minimum
%% probe.
queue_max_length_tracks_measured_delivery(_Config) ->
    HundredTaskRate = 100 * ?DATA_CHUNK_SIZE / ?QUEUE_TARGET_DURATION_MS,
    EightyTaskRate = 80 * ?DATA_CHUNK_SIZE / ?QUEUE_TARGET_DURATION_MS,
    ?assertEqual(100, queue_max_length(HundredTaskRate)),
    ?assertEqual(80, queue_max_length(EightyTaskRate)),
    ?assertEqual(?CONCURRENCY_CAP_INITIAL, queue_max_length(0.0)).

%% Caps and queue limits cover exactly the active peers, while cap memory is
%% retained when a peer leaves.
recompute(_Config) ->
    A = {1, 1, 1, 1, 1},
    B = {2, 2, 2, 2, 2},
    No = fun(_) -> false end,
    Yes = fun(_) -> true end,
    %% About 100 MiB/s in bytes/ms derives a 1600-task queue limit:
    %% 400 chunks/s * 4 seconds.
    Rate = 104857.6,
    MeasuredQueueMaxLength = round(
        Rate * ?QUEUE_TARGET_DURATION_MS / ?DATA_CHUNK_SIZE
    ),
    G = #{A => Rate, B => Rate},
    InitialState = #{
        A => #cap_control{cap = 100},
        B => #cap_control{cap = 100}
    },
    Productive = #fetch_timing{productive_ms = 1000},
    Timings0 = #{A => Productive, B => Productive},
    {Caps0, QueueMaxLengths0, S0} = recompute(
        [A, B], G, Timings0, Yes, InitialState
    ),
    ?assertEqual(108, maps:get(A, Caps0)),
    ?assertEqual(108, maps:get(B, Caps0)),
    ?assertEqual(MeasuredQueueMaxLength, maps:get(A, QueueMaxLengths0)),
    ?assertEqual(MeasuredQueueMaxLength, maps:get(B, QueueMaxLengths0)),
    %% Forty percent failed worker time reaches the halving clamp, reducing B's
    %% 108-request probe to 54 while A continues its eight-request probe.
    Failed = #fetch_timing{productive_ms = 600, timeout_ms = 400},
    {Caps1, _QueueMaxLengths1, S1} = recompute(
        [A, B],
        G,
        #{A => Productive, B => Failed},
        Yes,
        S0
    ),
    ?assertEqual(116, maps:get(A, Caps1)),
    ?assertEqual(54, maps:get(B, Caps1)),
    ?assertEqual(54, (maps:get(B, S1))#cap_control.cap),
    %% B leaves the active-peer set: its caps entry disappears, but its
    %% last cap remains available if it returns.
    {Caps2, QueueMaxLengths2, S2} = recompute(
        [A], G, #{A => Productive}, Yes, S1
    ),
    ?assertEqual(false, maps:is_key(B, Caps2)),
    ?assertEqual(false, maps:is_key(B, QueueMaxLengths2)),
    ?assertEqual(54, (maps:get(B, S2))#cap_control.cap),
    {Caps3, _QueueMaxLengths3, _S3} = recompute(
        [A, B], G, Timings0, Yes, S2
    ),
    ?assertEqual(54, maps:get(B, Caps3)),
    %% Lower delivery contracts queued work without changing an undriven cap.
    LowRateQueueMaxLength =
        round(5242.88 * ?QUEUE_TARGET_DURATION_MS / ?DATA_CHUNK_SIZE),
    {Caps4, QueueMaxLengths4, _} = recompute(
        [A],
        #{A => 5242.88},
        #{A => Productive},
        No,
        #{A => #cap_control{cap = 200}}
    ),
    ?assertEqual(200, maps:get(A, Caps4)),
    ?assertEqual(LowRateQueueMaxLength, maps:get(A, QueueMaxLengths4)),
    %% Unmeasured peers receive an eight-task queue bootstrap.
    {Caps5, QueueMaxLengths5, _} = recompute([A], #{}, #{}, No, #{}),
    ?assertEqual(?CONCURRENCY_CAP_INITIAL, maps:get(A, Caps5)),
    ?assertEqual(
        ?CONCURRENCY_CAP_INITIAL,
        maps:get(A, QueueMaxLengths5)
    ),
    ok.

%% Worker-time pressure is isolated by peer and clean productive queued work
%% resumes growth immediately after a failed interval.
worker_time_pressure_isolated(_Config) ->
    Healthy = {1, 1, 1, 1, 1},
    TimedOut = {2, 2, 2, 2, 2},
    Yes = fun(_) -> true end,
    Rate = 104857.6,
    Goodput = #{Healthy => Rate, TimedOut => Rate},
    State0 = #{
        Healthy => #cap_control{cap = 100},
        TimedOut => #cap_control{cap = 100}
    },
    Timings = #{
        Healthy => #fetch_timing{productive_ms = 1000},
        TimedOut => #fetch_timing{timeout_ms = 1000}
    },
    {Caps1, _QueueMaxLengths1, State1} = recompute(
        [Healthy, TimedOut],
        Goodput,
        Timings,
        Yes,
        State0
    ),
    ?assertEqual(108, maps:get(Healthy, Caps1)),
    ?assertEqual(50, maps:get(TimedOut, Caps1)),
    Recovery = #{TimedOut => #fetch_timing{productive_ms = 1000}},
    {Caps2, _QueueMaxLengths2, _State2} = recompute(
        [TimedOut],
        Goodput,
        Recovery,
        Yes,
        State1
    ),
    ?assertEqual(50, maps:get(TimedOut, Caps2)).

%% Flat driven goodput tests one eight-request window, then holds the lower cap
%% long enough to establish a baseline uncontaminated by old completions.
flat_driven_goodput_bounds_concurrency(_Config) ->
    Peer = {5, 5, 5, 5, 5},
    TickMs = 1000,
    RTTMs = 250,
    %% The initial observation starts at sixteen. Three more observations reach
    %% forty; the fourth flat comparison restores eight. Four settling
    %% observations retain eight before the ninth observation probes again.
    Caps = delivery_caps(
        Peer,
        [400, 400, 400, 400, 400, 400, 400, 400, 400],
        RTTMs,
        TickMs
    ),
    ?assertEqual([16, 24, 32, 40, 8, 8, 8, 8, 16], Caps).

%%====================================================================
%% Helpers
%%====================================================================

delivery_caps(Peer, ChunksPerTick, RTTMs, TickMs) ->
    State0 = tick([Peer], #{}, #{}, 0, new()),
    {_State, _Now, Caps} = lists:foldl(
        fun(Chunks, {State, Now, Acc}) ->
            Now2 = Now + TickMs,
            FetchTiming = #fetch_timing{
                productive_ms = Chunks * RTTMs
            },
            State1 = record_result(
                Peer,
                Chunks * ?DATA_CHUNK_SIZE,
                FetchTiming,
                State
            ),
            State2 = tick([Peer], #{}, #{Peer => true}, Now2, State1),
            Cap = maps:get(Peer, State2#state.caps),
            {State2, Now2, [Cap | Acc]}
        end,
        {State0, 0, []},
        ChunksPerTick
    ),
    lists:reverse(Caps).

-module(arweave_sync_footprint_SUITE).
-test_category([fast]).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_sync/include/arweave_sync.hrl").
-include("arweave_sync_footprint.hrl").

suite() -> [{timetrap, {seconds, 60}}].

all() ->
    [
        admit_replaces_unbound_source_snapshot,
        queued_reservation_can_use_available_global_slot,
        queued_reservation_reaches_full_cache_competition,
        build_batches,
        byte_source_respects_batch_limit_without_binding,
        build_batch_uses_available_intervals,
        task_completion,
        bound_candidates_include_fully_queued_footprint,
        bound_count_by_store,
        competition_releases_idle_incumbent,
        competition_renewal_margin,
        competition_prefers_store_with_fewer_footprints,
        competition_releases_excess_slot_before_blocked_store,
        competition_prefers_stronger_waiter,
        competition_drains_multiple_incumbents
    ].

init_per_testcase(_Case, Config) ->
    arweave_sync_test_deps:setup(),
    Config.

end_per_testcase(_Case, _Config) ->
    arweave_sync_test_deps:cleanup().

%%====================================================================
%% Test cases
%%====================================================================

%% @doc Re-admitting an unbound footprint refreshes its sources without charging
%% another claim.
admit_replaces_unbound_source_snapshot(_Config) ->
    StoreID = store,
    Footprint = #footprint{store_id = StoreID, partition = 1, footprint = 2},
    Original = arweave_sync_footprint:new_reservation(
        StoreID,
        Footprint,
        [#task_source{peer = old_peer, footprint = Footprint}]
    ),
    {ok, _ClaimedChunks, Reservations} = arweave_sync_footprint:admit(Original, arweave_sync_footprint:new()),
    Updated = arweave_sync_footprint:new_reservation(
        StoreID,
        Footprint,
        [#task_source{peer = new_peer, footprint = Footprint}]
    ),
    {ok, 0, Reservations2} = arweave_sync_footprint:admit(Updated, Reservations),
    ?assertEqual(
        arweave_sync_footprint:sources(Updated),
        arweave_sync_footprint:sources(maps:get(Footprint, Reservations2))
    ).

%% @doc Queued footprints can bind when global entropy capacity remains
%% available.
queued_reservation_can_use_available_global_slot(_Config) ->
    StoreID = store,
    Peer = peer,
    BoundFootprint = footprint(StoreID, 1),
    QueuedFootprint = footprint(StoreID, 2),
    Bound = bound_reservation(Peer, BoundFootprint, 1),
    Queued = queued_reservation(Peer, QueuedFootprint),
    %% One bound footprint leaves the second global entropy slot available.
    Dispatch = arweave_sync_footprint:test_dispatch(arweave_sync_footprint:test_state([Bound, Queued]), 2),
    [Source] = arweave_sync_footprint:sources(Queued),
    ?assert(arweave_sync_footprint:is_source_compatible(Queued, Source, Dispatch)),
    ?assert(arweave_sync_footprint:has_entropy_capacity(QueuedFootprint, Source, Dispatch)).

%% @doc A full entropy cache does not exclude queued footprints from capacity
%% competition.
queued_reservation_reaches_full_cache_competition(_Config) ->
    StoreID = store,
    Peer = peer,
    BoundFootprint = footprint(StoreID, 1),
    QueuedFootprint = footprint(StoreID, 2),
    Bound = bound_reservation(Peer, BoundFootprint, 1),
    Queued = queued_reservation(Peer, QueuedFootprint),
    %% The one-slot cache is full, but compatibility must let the queued
    %% footprint reach the scheduler's bounded displacement decision.
    Dispatch = arweave_sync_footprint:test_dispatch(arweave_sync_footprint:test_state([Bound, Queued]), 1),
    [Source] = arweave_sync_footprint:sources(Queued),
    ?assert(arweave_sync_footprint:is_source_compatible(Queued, Source, Dispatch)),
    ?assertNot(arweave_sync_footprint:has_entropy_capacity(QueuedFootprint, Source, Dispatch)).

%% @doc Bounded batches preserve remaining intervals and accumulate active
%% footprint tasks.
build_batches(_Config) ->
    Peer = peer,
    Footprint = #footprint{store_id = store, partition = 1, footprint = 2},
    %% Three chunks with a two-chunk batch leave one interval for refill.
    Intervals = ar_intervals:from_list([{3 * ?DATA_CHUNK_SIZE, 0}]),
    Source = #task_source{
        peer = Peer,
        footprint = Footprint,
        intervals = Intervals
    },
    Reservation = #footprint_reservation{
        store_id = store,
        footprint = Footprint,
        sources = [Source],
        state = queued
    },
    Dispatch = arweave_sync_footprint:test_dispatch(#{Footprint => Reservation}, 1),
    {Dispatch2, Tasks, BoundReservation} = arweave_sync_footprint:build_batch(
        Reservation, Source, Intervals, 2, Dispatch
    ),
    ?assertEqual(2, length(Tasks)),
    ?assertEqual(BoundReservation, arweave_sync_footprint:lookup(Footprint, Dispatch2)),
    ?assertEqual(2, BoundReservation#footprint_reservation.active_tasks),
    [
        #task_source{
            peer = Peer,
            footprint = Footprint,
            intervals = Remaining
        } = RemainingSource
    ] =
        BoundReservation#footprint_reservation.sources,
    ?assertEqual(?DATA_CHUNK_SIZE, ar_intervals:sum(Remaining)),
    {Dispatch3, RefillTasks, BoundReservation2} = arweave_sync_footprint:build_batch(
        BoundReservation, RemainingSource, Remaining, 1, Dispatch2
    ),
    ?assertEqual(1, length(RefillTasks)),
    ?assertEqual(BoundReservation2, arweave_sync_footprint:lookup(Footprint, Dispatch3)),
    ?assertEqual(3, BoundReservation2#footprint_reservation.active_tasks).

%% @doc Byte-source batches honor task limits without binding entropy capacity.
byte_source_respects_batch_limit_without_binding(_Config) ->
    Peer = peer,
    Footprint = #footprint{store_id = store, partition = 1, footprint = 2},
    %% A byte source does not consume entropy, but it still respects the
    %% peer/store batch limit.
    Intervals = ar_intervals:from_list([{2 * ?DATA_CHUNK_SIZE, 0}]),
    Source = #task_source{peer = Peer, intervals = Intervals},
    Reservation = #footprint_reservation{
        store_id = store,
        footprint = Footprint,
        sources = [Source],
        state = queued
    },
    Dispatch = arweave_sync_footprint:test_dispatch(#{Footprint => Reservation}, 1),
    {Dispatch2, [Task], none} = arweave_sync_footprint:build_batch(
        Reservation, Source, Intervals, 1, Dispatch
    ),
    ?assertEqual(not_found, arweave_sync_footprint:lookup(Footprint, Dispatch2)),
    ?assertEqual(none, Task#task.footprint).

%% @doc Footprint batches materialize only intervals still available after store
%% filtering.
build_batch_uses_available_intervals(_Config) ->
    Peer = {6, 6, 6, 6, 9},
    StoreID = store1,
    Footprint = footprint(StoreID, 1),
    %% Two retained candidates let the first be claimed independently while
    %% the second remains available for this reservation's next batch.
    RemainingIntervals = ar_intervals:from_list([
        {2 * ?DATA_CHUNK_SIZE, 0}
    ]),
    BoundReservation = #footprint_reservation{
        store_id = StoreID,
        peer = Peer,
        footprint = Footprint,
        sources = [
            #task_source{
                peer = Peer,
                footprint = Footprint,
                intervals = RemainingIntervals
            }
        ],
        state = bound
    },
    %% Store filtering excludes the first retained chunk before footprint
    %% batching, so only the second chunk becomes a task.
    AvailableIntervals = ar_intervals:from_list([
        {2 * ?DATA_CHUNK_SIZE, ?DATA_CHUNK_SIZE}
    ]),
    FootprintDispatch = arweave_sync_footprint:test_dispatch(#{Footprint => BoundReservation}, 1),
    [Source] = BoundReservation#footprint_reservation.sources,
    {FootprintDispatch2, [Task], Reservation2} = arweave_sync_footprint:build_batch(
        BoundReservation, Source, AvailableIntervals, 2, FootprintDispatch
    ),
    ?assertEqual(?DATA_CHUNK_SIZE, Task#task.offset),
    ?assertEqual(Reservation2, arweave_sync_footprint:lookup(Footprint, FootprintDispatch2)),
    ?assertEqual(1, Reservation2#footprint_reservation.active_tasks).

%% @doc A footprint with no remaining intervals is released after its final task
%% completes.
task_completion(_Config) ->
    Footprint = #footprint{store_id = store, partition = 1, footprint = 2},
    Reservation = #footprint_reservation{
        store_id = store,
        footprint = Footprint,
        peer = peer,
        sources = [
            #task_source{
                peer = peer,
                footprint = Footprint,
                intervals = ar_intervals:new()
            }
        ],
        active_tasks = 2,
        state = bound
    },
    Reservation2 = arweave_sync_footprint:task_completed(Reservation),
    ?assertEqual(1, Reservation2#footprint_reservation.active_tasks),
    ?assertEqual(release, arweave_sync_footprint:task_completed(Reservation2)).

%% @doc A bound footprint remains a candidate while its already-queued tasks are
%% active.
bound_candidates_include_fully_queued_footprint(_Config) ->
    Peer = peer,
    Footprint = footprint(store, 1),
    %% One active child with no remaining source intervals represents a
    %% footprint whose complete batch has already entered the peer queue.
    Reservation = #footprint_reservation{
        store_id = store,
        footprint = Footprint,
        peer = Peer,
        sources = [
            #task_source{
                peer = Peer,
                footprint = Footprint,
                intervals = ar_intervals:new()
            }
        ],
        active_tasks = 1,
        state = bound
    },
    Dispatch = arweave_sync_footprint:test_dispatch(#{Footprint => Reservation}, 1),
    ?assertEqual([{Footprint, Peer, store}], arweave_sync_footprint:bound_candidates(Dispatch)).

%% @doc Bound and draining footprints count toward global and per-store entropy
%% usage.
bound_count_by_store(_Config) ->
    StoreAFootprint = footprint(store_a, 1),
    StoreBFootprint = footprint(store_b, 2),
    %% One bound slot and one draining slot both consume entropy, while each
    %% destination store owns only its corresponding slot.
    StoreAReservation = bound_reservation(peer, StoreAFootprint, 1),
    StoreBReservation = (bound_reservation(peer, StoreBFootprint, 1))#footprint_reservation{
        state = draining
    },
    Dispatch = arweave_sync_footprint:test_dispatch(
        arweave_sync_footprint:test_state(
            [StoreAReservation, StoreBReservation]
        ),
        2
    ),
    ?assertEqual(2, arweave_sync_footprint:bound_count(Dispatch)),
    ?assertEqual(1, arweave_sync_footprint:bound_count(store_a, Dispatch)),
    ?assertEqual(1, arweave_sync_footprint:bound_count(store_b, Dispatch)).

%% @doc A stronger waiting footprint immediately replaces an idle incumbent.
competition_releases_idle_incumbent(_Config) ->
    IdlePeer = idle,
    WaitingPeer = waiting,
    IdleFootprint = footprint(store_a, 1),
    WaitingFootprint = footprint(store_b, 2),
    Reservations = #{
        IdleFootprint => bound_reservation(IdlePeer, IdleFootprint, 0),
        WaitingFootprint => queued_reservation(WaitingPeer, WaitingFootprint)
    },
    %% One entropy slot is occupied by an idle one-request peer while a
    %% 100-request peer waits, so the waiting footprint wins that slot.
    CandidatePriority = {0, 0, 0.0, -100},
    BoundPriorities = [{{0, 0, 0.0, -1}, IdleFootprint}],
    {released, Reservations2} = arweave_sync_footprint:compete_with_weakest(
        CandidatePriority, BoundPriorities, Reservations
    ),
    ?assertNot(maps:is_key(IdleFootprint, Reservations2)),
    ?assert(maps:is_key(WaitingFootprint, Reservations2)).

%% @doc An incumbent drains only after a sufficient priority improvement, then
%% releases on completion.
competition_renewal_margin(_Config) ->
    IncumbentPeer = incumbent,
    WaitingPeer = waiting,
    IncumbentFootprint = footprint(store_a, 1),
    WaitingFootprint = footprint(store_b, 2),
    Reservations = #{
        IncumbentFootprint => bound_reservation(
            IncumbentPeer, IncumbentFootprint, 1
        ),
        WaitingFootprint => queued_reservation(WaitingPeer, WaitingFootprint)
    },
    %% Loads of 50% and 46% differ by less than the ten-percent renewal margin,
    %% so the incumbent retains entropy.
    IncumbentPriority = {0, 0, 0.50, -100},
    BoundPriorities = [{IncumbentPriority, IncumbentFootprint}],
    lost = arweave_sync_footprint:compete_with_weakest(
        {0, 0, 0.46, -100}, BoundPriorities, Reservations
    ),
    ?assertMatch(
        #footprint_reservation{state = bound},
        maps:get(IncumbentFootprint, Reservations)
    ),
    %% A 44% waiting load is more than ten percent below the incumbent's 50%,
    %% so the waiting footprint wins the occupied slot.
    {draining, DrainingReservations} = arweave_sync_footprint:compete_with_weakest(
        {0, 0, 0.44, -100}, BoundPriorities, Reservations
    ),
    ?assertMatch(
        #footprint_reservation{state = draining},
        maps:get(IncumbentFootprint, DrainingReservations)
    ),
    %% An active draining reservation remains until its child task completes.
    ?assert(maps:is_key(IncumbentFootprint, DrainingReservations)),
    CompletedReservations = arweave_sync_footprint:task_completed(
        #task{footprint = IncumbentFootprint},
        DrainingReservations
    ),
    ?assertNot(maps:is_key(IncumbentFootprint, CompletedReservations)).

%% @doc Equal-load competition favors a store with fewer bound footprints.
competition_prefers_store_with_fewer_footprints(_Config) ->
    Peer = peer,
    IncumbentFootprint = footprint(store_a, 1),
    WaitingFootprint = footprint(store_b, 2),
    Reservations = #{
        IncumbentFootprint => bound_reservation(
            Peer, IncumbentFootprint, 1
        ),
        WaitingFootprint => queued_reservation(Peer, WaitingFootprint)
    },
    %% Equal 50% peer loads leave destination-store entropy ownership as the
    %% tie-breaker. A store with no slot beats an incumbent store with one.
    CandidatePriority = {0, 0, 0.5, -100},
    BoundPriorities = [{{1, 0, 0.5, -100}, IncumbentFootprint}],
    {draining, DrainingReservations} = arweave_sync_footprint:compete_with_weakest(
        CandidatePriority, BoundPriorities, Reservations
    ),
    ?assertMatch(
        #footprint_reservation{state = draining},
        maps:get(IncumbentFootprint, DrainingReservations)
    ).

%% @doc Competition reclaims excess ownership before taking a blocked store's
%% only footprint.
competition_releases_excess_slot_before_blocked_store(_Config) ->
    BlockedFootprint = footprint(store_a, 1),
    ExcessFootprint = footprint(store_b, 2),
    Reservations = #{
        BlockedFootprint => bound_reservation(
            blocked_peer, BlockedFootprint, 0
        ),
        ExcessFootprint => bound_reservation(
            ready_peer, ExcessFootprint, 0
        )
    },
    %% Store A owns one temporarily blocked slot while store B owns two slots.
    %% A zero-slot candidate must reclaim store B's excess instead of starving
    %% store A because its current work is momentarily unrunnable.
    BoundPriorities = [
        {{1, 1, 0.0, -100}, BlockedFootprint},
        {{2, 0, 0.0, -100}, ExcessFootprint}
    ],
    {released, Reservations2} = arweave_sync_footprint:compete_with_weakest(
        {0, 0, 0.0, -100}, BoundPriorities, Reservations
    ),
    ?assert(maps:is_key(BlockedFootprint, Reservations2)),
    ?assertNot(maps:is_key(ExcessFootprint, Reservations2)).

%% @doc A higher-capacity waiter can displace an incumbent even if it owns
%% another footprint.
competition_prefers_stronger_waiter(_Config) ->
    IncumbentPeer = incumbent,
    WaitingPeer = waiting,
    IncumbentFootprint = footprint(store_a, 1),
    WaitingFootprint = footprint(store_b, 2),
    Reservations = #{
        IncumbentFootprint => bound_reservation(
            IncumbentPeer, IncumbentFootprint, 1
        ),
        WaitingFootprint => queued_reservation(WaitingPeer, WaitingFootprint)
    },
    %% Equal zero load leaves the caps as the strength tie-breaker.
    CandidatePriority = {0, 0, 0.0, -100},
    BoundPriorities = [{{0, 0, 0.0, -1}, IncumbentFootprint}],
    {draining, DrainingReservations} = arweave_sync_footprint:compete_with_weakest(
        CandidatePriority, BoundPriorities, Reservations
    ),
    ?assertMatch(
        #footprint_reservation{state = draining},
        maps:get(IncumbentFootprint, DrainingReservations)
    ),
    OwnedFootprint = footprint(store_c, 3),
    ReservationsWithOwned = maps:put(
        OwnedFootprint,
        bound_reservation(WaitingPeer, OwnedFootprint, 1),
        Reservations
    ),
    %% An existing footprint in store_c does not exclude the waiting store_b
    %% footprint from competing for the occupied slot in store_a.
    {draining, DrainingReservations2} = arweave_sync_footprint:compete_with_weakest(
        CandidatePriority, BoundPriorities, ReservationsWithOwned
    ),
    ?assertMatch(
        #footprint_reservation{state = draining},
        maps:get(IncumbentFootprint, DrainingReservations2)
    ).

%% @doc Independent stronger waiters can drain multiple weaker incumbents.
competition_drains_multiple_incumbents(_Config) ->
    SlowPeerA = slow_a,
    SlowPeerB = slow_b,
    FastPeerA = fast_a,
    FastPeerB = fast_b,
    BoundFootprintA = footprint(store_a, 1),
    BoundFootprintB = footprint(store_b, 2),
    QueuedFootprintA = footprint(store_c, 3),
    QueuedFootprintB = footprint(store_d, 4),
    Reservations = #{
        BoundFootprintA => bound_reservation(SlowPeerA, BoundFootprintA, 1),
        BoundFootprintB => bound_reservation(SlowPeerB, BoundFootprintB, 1),
        QueuedFootprintA => queued_reservation(FastPeerA, QueuedFootprintA),
        QueuedFootprintB => queued_reservation(FastPeerB, QueuedFootprintB)
    },
    %% Both slots contain one-request peers and both queued footprints have
    %% 100-request peers, so each queued footprint drains one incumbent.
    {draining, DrainingReservations1} = arweave_sync_footprint:compete_with_weakest(
        {0, 0, 0.0, -100},
        [{{0, 0, 0.0, -1}, BoundFootprintA}],
        Reservations
    ),
    {draining, DrainingReservations} = arweave_sync_footprint:compete_with_weakest(
        {0, 0, 0.0, -100},
        [{{0, 0, 0.0, -1}, BoundFootprintB}],
        DrainingReservations1
    ),
    ?assertMatch(
        #footprint_reservation{state = draining},
        maps:get(BoundFootprintA, DrainingReservations)
    ),
    ?assertMatch(
        #footprint_reservation{state = draining},
        maps:get(BoundFootprintB, DrainingReservations)
    ).

%%====================================================================
%% Helpers
%%====================================================================

footprint(StoreID, Index) ->
    #footprint{store_id = StoreID, partition = 1, footprint = Index}.

bound_reservation(Peer, Footprint, ActiveTasks) ->
    #footprint{store_id = StoreID} = Footprint,
    Intervals = ar_intervals:from_list([{?DATA_CHUNK_SIZE, 0}]),
    #footprint_reservation{
        store_id = StoreID,
        peer = Peer,
        footprint = Footprint,
        sources = [
            #task_source{
                peer = Peer,
                footprint = Footprint,
                intervals = Intervals
            }
        ],
        active_tasks = ActiveTasks,
        state = bound
    }.

queued_reservation(Peer, Footprint) ->
    #footprint{store_id = StoreID} = Footprint,
    Intervals = ar_intervals:from_list([{?DATA_CHUNK_SIZE, 0}]),
    #footprint_reservation{
        store_id = StoreID,
        footprint = Footprint,
        sources = [
            #task_source{
                peer = Peer,
                footprint = Footprint,
                intervals = Intervals
            }
        ],
        state = queued
    }.

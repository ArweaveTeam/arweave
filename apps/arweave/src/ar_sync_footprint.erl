%%% @doc Lifecycle operations for a footprint reservation.
%%%
%%% The scheduler supplies selected peers, store availability, and batch
%%% capacity. This module owns reservation state, builds finite task batches,
%%% retains intervals not yet enqueued, and tracks when active tasks finish.
-module(ar_sync_footprint).
-test_category([fast]).

-export([new/0, start_dispatch/1, finish_dispatch/1, emit_metrics/1,
        new_reservation/3, key/1, store_id/1,
        sources/1, sort_key/1, queue_rank/1,
        claim_size/1,
        admit/2, reservation/2, build_batch/5,
        has_entropy_capacity/3,
        compete_for_entropy_capacity/4, bound_candidates/1,
        is_source_compatible/3,
        pending_reservations/1,
        has_bound_work/1, is_empty/1, task_completed/2,
        bound_count/1, bound_count/2,
        peers/1, queued_claim_chunks/1]).
-export_type([state/0, dispatch/0, reservation/0]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_sync.hrl").

-ifdef(AR_TEST).
-include_lib("eunit/include/eunit.hrl").
-export([test_reservation/6, reservation_state/1,
        reservation_peer/1, active_tasks/1,
        test_state/1, test_get/2, test_dispatch/2,
        set_max_active/2]).
-endif.

-opaque reservation() :: #footprint_reservation{}.
-opaque state() :: map().

%% Mutable footprint snapshot for one scheduler dispatch pass. Deferred
%% reservations and the draining-footprint count are reset each pass.
-record(dispatch, {
    reservations = #{},
    max_active,
    deferred = sets:new(),
    footprints_draining = 0
}).

-opaque dispatch() :: #dispatch{}.

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Return empty persistent footprint state.
new() ->
    #{}.

%% @doc Snapshot footprint state for one scheduler dispatch pass.
start_dispatch(Reservations) ->
    new_dispatch(Reservations, max_active()).

new_dispatch(Reservations, MaxActive) ->
    #dispatch{ reservations = Reservations, max_active = MaxActive }.

max_active() ->
    EntropyCacheSizeMiB = arweave_config:get([packing, entropy, cache_size]),
    FootprintSize = ar_block:get_replica_2_9_footprint_size(),
    max(1, (EntropyCacheSizeMiB * ?MiB) div FootprintSize).

-ifdef(AR_TEST).
%% @doc Build a dispatch with an explicit entropy-slot limit for focused tests.
test_dispatch(Reservations, MaxActive) ->
    new_dispatch(Reservations, MaxActive).
-endif.

%% @doc Return persistent footprint state after a dispatch pass.
finish_dispatch(#dispatch{ reservations = Reservations }) ->
    Reservations.

%% @doc Publish footprint reservation and entropy-slot accounting.
emit_metrics(Reservations) ->
    arweave_metrics:gauge_set(sync_active_footprints,
        bound_count(Reservations)),
    arweave_metrics:gauge_set(sync_max_active_footprints, max_active()),
    arweave_metrics:gauge_set(sync_footprint_queued_credits,
        queued_claim_chunks(Reservations)).

%% @doc Build a queued reservation from one picker snapshot.
new_reservation(StoreID, Footprint, Sources) ->
    #footprint_reservation{
        store_id = StoreID,
        footprint = Footprint,
        sources = Sources
    }.

%% @doc Return the footprint identifying Reservation.
key(#footprint_reservation{ footprint = Footprint }) ->
    Footprint.

%% @doc Return the destination storage module.
store_id(#footprint_reservation{ store_id = StoreID }) ->
    StoreID.

%% @doc Return the sources captured when the reservation was created.
sources(#footprint_reservation{ sources = Sources }) ->
    Sources.

%% @doc Return the reservation's store queue ordering key.
sort_key(#footprint_reservation{
        footprint = #footprint{ footprint = FootprintIndex } }) ->
    FootprintIndex.

%% @doc Return dispatch precedence after concrete tasks. Bound
%% reservations refill before queued reservations at the same position.
queue_rank(#footprint_reservation{ state = bound }) ->
    1;
queue_rank(#footprint_reservation{}) ->
    2.

%% @doc Return the whole-footprint claim held before a reservation is bound.
claim_size(#footprint_reservation{}) ->
    footprint_chunks().

%%%===================================================================
%%% Binding and batching.
%%%===================================================================

%% @doc Add a reservation or refresh the same unbound footprint. Store
%% admission is coordinated separately by the scheduler. A zero claim means
%% the reservation refreshed or matched an existing footprint.
admit(Reservation, Reservations) ->
    Footprint = key(Reservation),
    case maps:get(Footprint, Reservations, none) of
        #footprint_reservation{ state = queued } ->
            {ok, 0, maps:put(Footprint, Reservation, Reservations)};
        #footprint_reservation{} ->
            {ok, 0, Reservations};
        none ->
            {ok, claim_size(Reservation),
                maps:put(Footprint, Reservation, Reservations)}
    end.

%% @doc Return a reservation that remains eligible in this dispatch pass.
reservation(Footprint, Dispatch) ->
    #dispatch{ reservations = Reservations, deferred = Deferred } = Dispatch,
    case sets:is_element(Footprint, Deferred) of
        true -> none;
        false -> maps:get(Footprint, Reservations, none)
    end.

%% @doc Return the authoritative reservation for Footprint.
lookup(Footprint, Dispatch) ->
    maps:get(Footprint, reservations(Dispatch), not_found).

%% @doc Build one finite task batch from an already-selected source and the
%% source intervals not currently claimed by the store. Return any bound
%% reservation with intervals remaining.
build_batch(Reservation, Source, AvailableIntervals, Limit, Dispatch) ->
    #task_source{ peer = Peer, footprint = Footprint } = Source,
    {BatchIntervals, RemainingIntervals} = split_intervals(
        AvailableIntervals, Limit),
    Tasks = build_tasks(Reservation, Peer, Footprint, BatchIntervals),
    EnqueuedCount = length(Tasks),
    ActiveTasks = Reservation#footprint_reservation.active_tasks
        + EnqueuedCount,
    BoundReservation = case Footprint of
        none ->
            none;
        #footprint{} ->
            case ActiveTasks =:= 0
                    andalso ar_intervals:is_empty(RemainingIntervals) of
                true ->
                    none;
                false ->
                    Reservation#footprint_reservation{
                        sources = [Source#task_source{
                            intervals = RemainingIntervals }],
                        peer = Peer,
                        active_tasks = ActiveTasks,
                        state = bound
                    }
            end
    end,
    FootprintKey = key(Reservation),
    Dispatch2 = put_reservation(FootprintKey, BoundReservation, Dispatch),
    {Dispatch2, Tasks, BoundReservation}.

split_intervals(Intervals, Limit) ->
    {_Remaining, Batch, Rest} = ar_intervals:fold(
        fun({End, Start}, {0, BatchAcc, RestAcc}) ->
                {0, BatchAcc, ar_intervals:add(RestAcc, End, Start)};
            ({End, Start}, {Remaining, BatchAcc, RestAcc}) ->
                ChunkCount = (End - Start + ?DATA_CHUNK_SIZE - 1)
                    div ?DATA_CHUNK_SIZE,
                case ChunkCount =< Remaining of
                    true ->
                        {Remaining - ChunkCount,
                            ar_intervals:add(BatchAcc, End, Start), RestAcc};
                    false ->
                        BatchEnd = Start + Remaining * ?DATA_CHUNK_SIZE,
                        {0,
                            ar_intervals:add(BatchAcc, BatchEnd, Start),
                            ar_intervals:add(RestAcc, End, BatchEnd)}
                end
        end,
        {Limit, ar_intervals:new(), ar_intervals:new()},
        Intervals),
    {Batch, Rest}.

build_tasks(Reservation, Peer, Footprint, Intervals) ->
    Tasks = ar_intervals:fold(
        fun({End, Start}, Acc) ->
            build_interval_tasks(Start, End, Reservation, Peer, Footprint, Acc)
        end,
        [],
        Intervals),
    lists:reverse(Tasks).

build_interval_tasks(Start, End, _Reservation, _Peer, _Footprint, Tasks)
        when Start >= End ->
    Tasks;
build_interval_tasks(Offset, End, Reservation, Peer, Footprint, Tasks) ->
    TaskFootprint = case Footprint of
        none -> none;
        #footprint{} -> Reservation#footprint_reservation.footprint
    end,
    Task = #task{
        offset = Offset,
        sources = [#task_source{ peer = Peer, footprint = Footprint }],
        peer = undefined,
        store_id = Reservation#footprint_reservation.store_id,
        footprint = TaskFootprint,
        state = queued
    },
    build_interval_tasks(Offset + ?DATA_CHUNK_SIZE, End,
        Reservation, Peer, Footprint, [Task | Tasks]).

%%%===================================================================
%%% Task completion.
%%%===================================================================

%% @doc Account for a completed task and release an exhausted reservation.
task_completed(#task{ footprint = none }, Reservations) ->
    Reservations;
task_completed(#task{ footprint = Footprint }, Reservations) ->
    case maps:get(Footprint, Reservations, undefined) of
        undefined ->
            Reservations;
        #footprint_reservation{} = Reservation ->
            case task_completed(Reservation) of
                release ->
                    release(Footprint, Reservations);
                #footprint_reservation{} = Reservation2 ->
                    maps:put(Footprint, Reservation2, Reservations)
            end
    end.

task_completed(Reservation) ->
    #footprint_reservation{
        active_tasks = ActiveTasks,
        sources = [#task_source{ intervals = RemainingIntervals }]
    } = Reservation,
    ActiveTasks2 = ActiveTasks - 1,
    ShouldRelease = ActiveTasks2 =:= 0
        andalso (Reservation#footprint_reservation.state =:= draining
            orelse ar_intervals:is_empty(RemainingIntervals)),
    case ShouldRelease of
        true -> release;
        false ->
            Reservation#footprint_reservation{ active_tasks = ActiveTasks2 }
    end.

%%%===================================================================
%%% Release competition.
%%%===================================================================

%% @doc Return whether Source can bind without displacing a footprint that
%% already occupies an entropy slot.
has_entropy_capacity(Footprint,
        #task_source{ footprint = #footprint{} }, Dispatch) ->
    #dispatch{ max_active = MaxActive } = Dispatch,
    case lookup(Footprint, Dispatch) of
        #footprint_reservation{ state = queued } ->
            bound_count(Dispatch) < MaxActive;
        _ ->
            true
    end;
has_entropy_capacity(_Footprint, _Source, _Dispatch) ->
    true.

%% @doc Let a queued footprint compete for full entropy capacity. The caller
%% has already established that no unused entropy slot is available.
compete_for_entropy_capacity(Footprint, CandidatePriority, BoundPriorities,
        Dispatch) ->
    #dispatch{
        reservations = Reservations,
        footprints_draining = FootprintsDraining
    } = Dispatch,
    case FootprintsDraining < draining_count(Reservations) of
        true ->
            defer(Footprint, Dispatch#dispatch{
                footprints_draining = FootprintsDraining + 1
            });
        false ->
            case compete_with_weakest(
                    CandidatePriority, BoundPriorities, Reservations) of
                {released, Reservations2} ->
                    {ready, Dispatch#dispatch{
                        reservations = Reservations2 }};
                {draining, Reservations2} ->
                    defer(Footprint, Dispatch#dispatch{
                        reservations = Reservations2,
                        footprints_draining = FootprintsDraining + 1
                    });
                lost ->
                    defer(Footprint, Dispatch)
            end
    end.

defer(Footprint, #dispatch{ deferred = Deferred } = Dispatch) ->
    {deferred, Dispatch#dispatch{ deferred =
        sets:add_element(Footprint, Deferred) }}.

%% @doc Return every bound reservation occupying an entropy slot. A footprint
%% remains eligible for displacement after all its work has entered peer queues;
%% otherwise queued work can hide the slot from a newly available store.
bound_candidates(Dispatch) ->
    maps:fold(
        fun(Footprint, #footprint_reservation{ state = bound,
                peer = Peer, store_id = StoreID }, Acc) ->
                [{Footprint, Peer, StoreID} | Acc];
            (_Footprint, _Reservation, Acc) ->
                Acc
        end,
        [],
        reservations(Dispatch)).

compete_with_weakest(CandidatePriority, BoundPriorities, Reservations) ->
    SortedPriorities = lists:reverse(lists:sort(BoundPriorities)),
    case SortedPriorities of
        [] ->
            lost;
        [{IncumbentPriority, IncumbentFootprint} | _] ->
            case candidate_wins(CandidatePriority, IncumbentPriority) of
                true ->
                    Incumbent = maps:get(IncumbentFootprint, Reservations),
                    ActiveTasks = Incumbent#footprint_reservation.active_tasks,
                    case ActiveTasks of
                        0 ->
                            {released,
                                release(IncumbentFootprint, Reservations)};
                        _ ->
                            {draining,
                                maps:put(IncumbentFootprint,
                                    Incumbent#footprint_reservation{
                                        state = draining }, Reservations)}
                    end;
                false ->
                    lost
            end
    end.

candidate_wins({CandidateStoreCount, _CandidateAvailability,
        _CandidatePeerLoad, _CandidateCap},
        {IncumbentStoreCount, _IncumbentAvailability,
            _IncumbentPeerLoad, _IncumbentCap})
        when CandidateStoreCount < IncumbentStoreCount ->
    true;
candidate_wins({CandidateStoreCount, _CandidateAvailability,
        _CandidatePeerLoad, _CandidateCap},
        {IncumbentStoreCount, _IncumbentAvailability,
            _IncumbentPeerLoad, _IncumbentCap})
        when CandidateStoreCount > IncumbentStoreCount ->
    false;
candidate_wins({StoreCount, 0, _CandidatePeerLoad, _CandidateCap},
        {StoreCount, 1, _IncumbentPeerLoad, _IncumbentCap}) ->
    true;
candidate_wins({StoreCount, 1, _CandidatePeerLoad, _CandidateCap},
        {StoreCount, 0, _IncumbentPeerLoad, _IncumbentCap}) ->
    false;
candidate_wins({StoreCount, Availability, CandidatePeerLoad,
        NegativeCandidateCap},
        {StoreCount, Availability, IncumbentPeerLoad,
            NegativeIncumbentCap}) ->
    case compare_load_with_margin(CandidatePeerLoad, IncumbentPeerLoad) of
        better -> true;
        worse -> false;
        close ->
            CandidateCap = -NegativeCandidateCap,
            IncumbentCap = -NegativeIncumbentCap,
            CandidateCap > IncumbentCap * 1.1
    end.

%% The incumbent retains its entropy while it is within ten percent of the
%% challenger, avoiding churn between otherwise equivalent footprints.
compare_load_with_margin(Candidate, Incumbent)
        when Incumbent > Candidate * 1.1 ->
    better;
compare_load_with_margin(Candidate, Incumbent)
        when Candidate > Incumbent * 1.1 ->
    worse;
compare_load_with_margin(_Candidate, _Incumbent) ->
    close.

%%%===================================================================
%%% Release.
%%%===================================================================

%% @doc Release one footprint reservation. Any remaining local need is found
%% by the store sweeper's next complete pass.
release(Footprint, Reservations) ->
    maps:remove(Footprint, Reservations).

%%%===================================================================
%%% Queries.
%%%===================================================================

%% @doc Return whether a source is compatible with current footprint state.
is_source_compatible(#footprint_reservation{ state = bound, peer = Peer },
        #task_source{ peer = Peer }, _Dispatch) ->
    true;
is_source_compatible(#footprint_reservation{ state = queued },
        #task_source{}, _Dispatch) ->
    true;
is_source_compatible(#footprint_reservation{ state = draining },
        _Source, _Dispatch) ->
    false;
is_source_compatible(#task{ footprint = none },
        #task_source{ footprint = none }, _Dispatch) ->
    true;
is_source_compatible(#task{ footprint = none }, _Source, _Dispatch) ->
    false;
is_source_compatible(#task{ footprint = Footprint },
        #task_source{ peer = Peer,
            footprint = SourceFootprint }, Dispatch) ->
    case maps:get(Footprint, reservations(Dispatch), undefined) of
        #footprint_reservation{ state = State, peer = Peer,
                sources = [#task_source{
                    footprint = SourceFootprint }] }
                when State =:= bound; State =:= draining ->
            true;
        _ ->
            false
    end.

%% @doc Return bound reservations that retain source intervals to enqueue in
%% this dispatch pass.
pending_reservations(#dispatch{} = Dispatch) ->
    pending_reservations(maps:values(reservations(Dispatch)));
pending_reservations(Reservations) when is_map(Reservations) ->
    pending_reservations(maps:values(Reservations));
pending_reservations(Reservations) when is_list(Reservations) ->
    lists:filtermap(
        fun(Reservation) ->
            case pending_reservation(Reservation) of
                none -> false;
                PendingReservation -> {true, PendingReservation}
            end
        end,
        Reservations).

pending_reservation(#footprint_reservation{ state = bound,
        sources = [#task_source{ intervals = Intervals }] } = Reservation) ->
    case ar_intervals:is_empty(Intervals) of
        true -> none;
        false -> Reservation
    end;
pending_reservation(_Reservation) ->
    none.

%% @doc Return whether any bound reservation retains intervals to enqueue.
has_bound_work(Footprints) ->
    lists:any(
        fun(Reservation) ->
                case Reservation of
                    #footprint_reservation{ state = bound,
                            sources = [#task_source{
                                intervals = Intervals }] } ->
                        not ar_intervals:is_empty(Intervals);
                    _ ->
                        false
                end
        end,
        maps:values(reservations(Footprints))).

%% @doc Return whether no footprint reservations are tracked.
is_empty(Footprints) ->
    map_size(reservations(Footprints)) =:= 0.

%% @doc Count footprints currently bound to a peer and consuming entropy.
%% Draining footprints remain bound until their active tasks finish.
bound_count(Footprints) ->
    maps:fold(
        fun(_Footprint, Reservation, Count) ->
                State = Reservation#footprint_reservation.state,
                case State =:= bound orelse State =:= draining of
                    true -> Count + 1;
                    false -> Count
                end
        end,
        0,
        reservations(Footprints)).

%% @doc Count entropy slots currently owned by one destination store.
bound_count(StoreID, Footprints) ->
    maps:fold(
        fun(_Footprint, #footprint_reservation{ store_id = OwnerStoreID,
                state = State }, Count)
                when OwnerStoreID =:= StoreID,
                    (State =:= bound orelse State =:= draining) ->
                Count + 1;
            (_Footprint, _Reservation, Count) ->
                Count
        end,
        0,
        reservations(Footprints)).

draining_count(Reservations) ->
    maps:fold(
        fun(_Footprint, Reservation, Count) ->
                case Reservation#footprint_reservation.state of
                    draining -> Count + 1;
                    _ -> Count
                end
        end,
        0,
        Reservations).

%% @doc Return peers owning bound or draining footprints.
peers(Footprints) ->
    maps:fold(
        fun(_Footprint, Reservation, Acc) ->
            State = Reservation#footprint_reservation.state,
            case State =:= bound orelse State =:= draining of
                true -> [Reservation#footprint_reservation.peer | Acc];
                false -> Acc
            end
        end,
        [],
        reservations(Footprints)).

%% @doc Return the chunks claimed by queued whole-footprint reservations.
queued_claim_chunks(Footprints) ->
    maps:fold(
        fun(_Footprint, #footprint_reservation{ state = queued }, Acc) ->
                Acc + footprint_chunks();
            (_Footprint, _Reservation, Acc) ->
                Acc
        end,
        0,
        reservations(Footprints)).

footprint_chunks() ->
    ar_block:get_replica_2_9_footprint_size() div ?DATA_CHUNK_SIZE.

reservations(#dispatch{ reservations = Reservations }) ->
    Reservations;
reservations(Reservations) when is_map(Reservations) ->
    Reservations.

put_reservation(Footprint, none, #dispatch{ reservations = Reservations } = Dispatch) ->
    Dispatch#dispatch{ reservations = release(Footprint, Reservations) };
put_reservation(Footprint, Reservation,
        #dispatch{ reservations = Reservations } = Dispatch) ->
    Dispatch#dispatch{ reservations =
        maps:put(Footprint, Reservation, Reservations) }.

%%%===================================================================
%%% Tests.
%%%===================================================================

-ifdef(AR_TEST).

test_reservation(StoreID, Footprint, Sources, Peer, ActiveTasks, State) ->
    #footprint_reservation{
        store_id = StoreID,
        footprint = Footprint,
        sources = Sources,
        peer = Peer,
        active_tasks = ActiveTasks,
        state = State
    }.

reservation_state(#footprint_reservation{ state = State }) ->
    State.

reservation_peer(#footprint_reservation{ peer = Peer }) ->
    Peer.

active_tasks(#footprint_reservation{ active_tasks = ActiveTasks }) ->
    ActiveTasks.

test_state(Reservations) ->
    maps:from_list([{key(Reservation), Reservation}
        || Reservation <- Reservations]).

test_get(Footprint, Footprints) ->
    maps:get(Footprint, reservations(Footprints)).

set_max_active(MaxActive, Dispatch) ->
    Dispatch#dispatch{ max_active = MaxActive }.

admit_replaces_unbound_source_snapshot_test() ->
    StoreID = store,
    Footprint = #footprint{ store_id = StoreID, partition = 1, footprint = 2 },
    Original = new_reservation(StoreID, Footprint,
        [#task_source{ peer = old_peer, footprint = Footprint }]),
    {ok, _ClaimedChunks, Reservations} = admit(Original, new()),
    Updated = new_reservation(StoreID, Footprint,
        [#task_source{ peer = new_peer, footprint = Footprint }]),
    {ok, 0, Reservations2} = admit(Updated, Reservations),
    ?assertEqual(sources(Updated),
        sources(maps:get(Footprint, Reservations2))).

queued_reservation_can_use_available_global_slot_test() ->
    StoreID = store,
    Peer = peer,
    BoundFootprint = footprint(StoreID, 1),
    QueuedFootprint = footprint(StoreID, 2),
    Bound = bound_reservation(Peer, BoundFootprint, 1),
    Queued = queued_reservation(Peer, QueuedFootprint),
    %% One bound footprint leaves the second global entropy slot available.
    Dispatch = test_dispatch(test_state([Bound, Queued]), 2),
    [Source] = sources(Queued),
    ?assert(is_source_compatible(Queued, Source, Dispatch)),
    ?assert(has_entropy_capacity(QueuedFootprint, Source, Dispatch)).

queued_reservation_reaches_full_cache_competition_test() ->
    StoreID = store,
    Peer = peer,
    BoundFootprint = footprint(StoreID, 1),
    QueuedFootprint = footprint(StoreID, 2),
    Bound = bound_reservation(Peer, BoundFootprint, 1),
    Queued = queued_reservation(Peer, QueuedFootprint),
    %% The one-slot cache is full, but compatibility must let the queued
    %% footprint reach the scheduler's bounded displacement decision.
    Dispatch = test_dispatch(test_state([Bound, Queued]), 1),
    [Source] = sources(Queued),
    ?assert(is_source_compatible(Queued, Source, Dispatch)),
    ?assertNot(has_entropy_capacity(QueuedFootprint, Source, Dispatch)).

build_batches_test() ->
    Peer = peer,
    Footprint = #footprint{ store_id = store, partition = 1, footprint = 2 },
    %% Three chunks with a two-chunk batch leave one interval for refill.
    Intervals = ar_intervals:from_list([{3 * ?DATA_CHUNK_SIZE, 0}]),
    Source = #task_source{ peer = Peer,
        footprint = Footprint, intervals = Intervals },
    Reservation = #footprint_reservation{ store_id = store,
        footprint = Footprint, sources = [Source],
        state = queued },
    Dispatch = test_dispatch(#{Footprint => Reservation}, 1),
    {Dispatch2, Tasks, BoundReservation} = build_batch(
        Reservation, Source, Intervals, 2, Dispatch),
    ?assertEqual(2, length(Tasks)),
    ?assertEqual(BoundReservation, lookup(Footprint, Dispatch2)),
    ?assertEqual(2, BoundReservation#footprint_reservation.active_tasks),
    [#task_source{ peer = Peer, footprint = Footprint,
        intervals = Remaining } = RemainingSource] =
            BoundReservation#footprint_reservation.sources,
    ?assertEqual(?DATA_CHUNK_SIZE, ar_intervals:sum(Remaining)),
    {Dispatch3, RefillTasks, BoundReservation2} = build_batch(
        BoundReservation, RemainingSource, Remaining, 1, Dispatch2),
    ?assertEqual(1, length(RefillTasks)),
    ?assertEqual(BoundReservation2, lookup(Footprint, Dispatch3)),
    ?assertEqual(3, BoundReservation2#footprint_reservation.active_tasks).

byte_source_respects_batch_limit_without_binding_test() ->
    Peer = peer,
    Footprint = #footprint{ store_id = store, partition = 1, footprint = 2 },
    %% A byte source does not consume entropy, but it still respects the
    %% peer/store batch limit.
    Intervals = ar_intervals:from_list([{2 * ?DATA_CHUNK_SIZE, 0}]),
    Source = #task_source{ peer = Peer, intervals = Intervals },
    Reservation = #footprint_reservation{ store_id = store,
        footprint = Footprint, sources = [Source],
        state = queued },
    Dispatch = test_dispatch(#{Footprint => Reservation}, 1),
    {Dispatch2, [Task], none} = build_batch(
        Reservation, Source, Intervals, 1, Dispatch),
    ?assertEqual(not_found, lookup(Footprint, Dispatch2)),
    ?assertEqual(none, Task#task.footprint).

build_batch_uses_available_intervals_test() ->
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
        sources = [#task_source{ peer = Peer, footprint = Footprint,
            intervals = RemainingIntervals }],
        state = bound
    },
    %% Store filtering excludes the first retained chunk before footprint
    %% batching, so only the second chunk becomes a task.
    AvailableIntervals = ar_intervals:from_list([
        {2 * ?DATA_CHUNK_SIZE, ?DATA_CHUNK_SIZE}
    ]),
    FootprintDispatch = test_dispatch(#{Footprint => BoundReservation}, 1),
    [Source] = BoundReservation#footprint_reservation.sources,
    {FootprintDispatch2, [Task], Reservation2} = build_batch(
        BoundReservation, Source, AvailableIntervals, 2, FootprintDispatch),
    ?assertEqual(?DATA_CHUNK_SIZE, Task#task.offset),
    ?assertEqual(Reservation2, lookup(Footprint, FootprintDispatch2)),
    ?assertEqual(1, Reservation2#footprint_reservation.active_tasks).

task_completed_test() ->
    Footprint = #footprint{ store_id = store, partition = 1, footprint = 2 },
    Reservation = #footprint_reservation{ store_id = store,
        footprint = Footprint, peer = peer,
        sources = [#task_source{ peer = peer, footprint = Footprint,
            intervals = ar_intervals:new() }],
        active_tasks = 2, state = bound },
    Reservation2 = task_completed(Reservation),
    ?assertEqual(1, Reservation2#footprint_reservation.active_tasks),
    ?assertEqual(release, task_completed(Reservation2)).

bound_candidates_include_fully_queued_footprint_test() ->
    Peer = peer,
    Footprint = footprint(store, 1),
    %% One active child with no remaining source intervals represents a
    %% footprint whose complete batch has already entered the peer queue.
    Reservation = #footprint_reservation{ store_id = store,
        footprint = Footprint, peer = Peer,
        sources = [#task_source{ peer = Peer, footprint = Footprint,
            intervals = ar_intervals:new() }],
        active_tasks = 1, state = bound },
    Dispatch = test_dispatch(#{Footprint => Reservation}, 1),
    ?assertEqual([{Footprint, Peer, store}], bound_candidates(Dispatch)).

bound_count_by_store_test() ->
    StoreAFootprint = footprint(store_a, 1),
    StoreBFootprint = footprint(store_b, 2),
    %% One bound slot and one draining slot both consume entropy, while each
    %% destination store owns only its corresponding slot.
    StoreAReservation = bound_reservation(peer, StoreAFootprint, 1),
    StoreBReservation = (bound_reservation(peer, StoreBFootprint, 1))
        #footprint_reservation{ state = draining },
    Dispatch = test_dispatch(test_state(
        [StoreAReservation, StoreBReservation]), 2),
    ?assertEqual(2, bound_count(Dispatch)),
    ?assertEqual(1, bound_count(store_a, Dispatch)),
    ?assertEqual(1, bound_count(store_b, Dispatch)).

competition_releases_idle_incumbent_test() ->
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
    {released, Reservations2} = compete_with_weakest(
        CandidatePriority, BoundPriorities, Reservations),
    ?assertNot(maps:is_key(IdleFootprint, Reservations2)),
    ?assert(maps:is_key(WaitingFootprint, Reservations2)).

competition_renewal_margin_test() ->
    IncumbentPeer = incumbent,
    WaitingPeer = waiting,
    IncumbentFootprint = footprint(store_a, 1),
    WaitingFootprint = footprint(store_b, 2),
    Reservations = #{
        IncumbentFootprint => bound_reservation(
            IncumbentPeer, IncumbentFootprint, 1),
        WaitingFootprint => queued_reservation(WaitingPeer, WaitingFootprint)
    },
    %% Loads of 50% and 46% differ by less than the ten-percent renewal margin,
    %% so the incumbent retains entropy.
    IncumbentPriority = {0, 0, 0.50, -100},
    BoundPriorities = [{IncumbentPriority, IncumbentFootprint}],
    lost = compete_with_weakest(
        {0, 0, 0.46, -100}, BoundPriorities, Reservations),
    ?assertMatch(#footprint_reservation{ state = bound },
        maps:get(IncumbentFootprint, Reservations)),
    %% A 44% waiting load is more than ten percent below the incumbent's 50%,
    %% so the waiting footprint wins the occupied slot.
    {draining, DrainingReservations} = compete_with_weakest(
        {0, 0, 0.44, -100}, BoundPriorities, Reservations),
    ?assertMatch(#footprint_reservation{ state = draining },
        maps:get(IncumbentFootprint, DrainingReservations)),
    %% An active draining reservation remains until its child task completes.
    ?assert(maps:is_key(IncumbentFootprint, DrainingReservations)),
    CompletedReservations = task_completed(
        #task{ footprint = IncumbentFootprint },
        DrainingReservations),
    ?assertNot(maps:is_key(IncumbentFootprint, CompletedReservations)).

competition_prefers_store_with_fewer_footprints_test() ->
    Peer = peer,
    IncumbentFootprint = footprint(store_a, 1),
    WaitingFootprint = footprint(store_b, 2),
    Reservations = #{
        IncumbentFootprint => bound_reservation(
            Peer, IncumbentFootprint, 1),
        WaitingFootprint => queued_reservation(Peer, WaitingFootprint)
    },
    %% Equal 50% peer loads leave destination-store entropy ownership as the
    %% tie-breaker. A store with no slot beats an incumbent store with one.
    CandidatePriority = {0, 0, 0.5, -100},
    BoundPriorities = [{{1, 0, 0.5, -100}, IncumbentFootprint}],
    {draining, DrainingReservations} = compete_with_weakest(
        CandidatePriority, BoundPriorities, Reservations),
    ?assertMatch(#footprint_reservation{ state = draining },
        maps:get(IncumbentFootprint, DrainingReservations)).

competition_releases_excess_slot_before_blocked_store_test() ->
    BlockedFootprint = footprint(store_a, 1),
    ExcessFootprint = footprint(store_b, 2),
    Reservations = #{
        BlockedFootprint => bound_reservation(
            blocked_peer, BlockedFootprint, 0),
        ExcessFootprint => bound_reservation(
            ready_peer, ExcessFootprint, 0)
    },
    %% Store A owns one temporarily blocked slot while store B owns two slots.
    %% A zero-slot candidate must reclaim store B's excess instead of starving
    %% store A because its current work is momentarily unrunnable.
    BoundPriorities = [
        {{1, 1, 0.0, -100}, BlockedFootprint},
        {{2, 0, 0.0, -100}, ExcessFootprint}
    ],
    {released, Reservations2} = compete_with_weakest(
        {0, 0, 0.0, -100}, BoundPriorities, Reservations),
    ?assert(maps:is_key(BlockedFootprint, Reservations2)),
    ?assertNot(maps:is_key(ExcessFootprint, Reservations2)).

competition_prefers_stronger_waiter_test() ->
    IncumbentPeer = incumbent,
    WaitingPeer = waiting,
    IncumbentFootprint = footprint(store_a, 1),
    WaitingFootprint = footprint(store_b, 2),
    Reservations = #{
        IncumbentFootprint => bound_reservation(
            IncumbentPeer, IncumbentFootprint, 1),
        WaitingFootprint => queued_reservation(WaitingPeer, WaitingFootprint)
    },
    %% Equal zero load leaves the caps as the strength tie-breaker.
    CandidatePriority = {0, 0, 0.0, -100},
    BoundPriorities = [{{0, 0, 0.0, -1}, IncumbentFootprint}],
    {draining, DrainingReservations} = compete_with_weakest(
        CandidatePriority, BoundPriorities, Reservations),
    ?assertMatch(#footprint_reservation{ state = draining },
        maps:get(IncumbentFootprint, DrainingReservations)),
    OwnedFootprint = footprint(store_c, 3),
    ReservationsWithOwned = maps:put(OwnedFootprint,
        bound_reservation(WaitingPeer, OwnedFootprint, 1), Reservations),
    %% An existing footprint in store_c does not exclude the waiting store_b
    %% footprint from competing for the occupied slot in store_a.
    {draining, DrainingReservations2} = compete_with_weakest(
        CandidatePriority, BoundPriorities, ReservationsWithOwned),
    ?assertMatch(#footprint_reservation{ state = draining },
        maps:get(IncumbentFootprint, DrainingReservations2)).

competition_drains_multiple_incumbents_test() ->
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
    {draining, DrainingReservations1} = compete_with_weakest(
        {0, 0, 0.0, -100},
        [{{0, 0, 0.0, -1}, BoundFootprintA}], Reservations),
    {draining, DrainingReservations} = compete_with_weakest(
        {0, 0, 0.0, -100}, [{{0, 0, 0.0, -1}, BoundFootprintB}],
        DrainingReservations1),
    ?assertMatch(#footprint_reservation{ state = draining },
        maps:get(BoundFootprintA, DrainingReservations)),
    ?assertMatch(#footprint_reservation{ state = draining },
        maps:get(BoundFootprintB, DrainingReservations)).

footprint(StoreID, Index) ->
    #footprint{ store_id = StoreID, partition = 1, footprint = Index }.

bound_reservation(Peer, Footprint, ActiveTasks) ->
    #footprint{ store_id = StoreID } = Footprint,
    Intervals = ar_intervals:from_list([{?DATA_CHUNK_SIZE, 0}]),
    #footprint_reservation{ store_id = StoreID, peer = Peer,
        footprint = Footprint, sources = [#task_source{ peer = Peer,
            footprint = Footprint, intervals = Intervals }],
        active_tasks = ActiveTasks, state = bound }.

queued_reservation(Peer, Footprint) ->
    #footprint{ store_id = StoreID } = Footprint,
    Intervals = ar_intervals:from_list([{?DATA_CHUNK_SIZE, 0}]),
    #footprint_reservation{ store_id = StoreID,
        footprint = Footprint, sources = [#task_source{ peer = Peer,
            footprint = Footprint, intervals = Intervals }],
        state = queued }.

-endif.

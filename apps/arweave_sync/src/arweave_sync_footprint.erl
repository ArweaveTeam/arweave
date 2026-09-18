%%% @doc Lifecycle operations for a footprint reservation.
%%%
%%% The scheduler supplies selected peers, store availability, and batch
%%% capacity. This module owns reservation state, builds finite task batches,
%%% retains intervals not yet enqueued, and tracks when active tasks finish.
-module(arweave_sync_footprint).

-ifdef(AR_TEST).
%% Focused tests live outside the production module.
-export([
    compete_with_weakest/3,
    lookup/2,
    task_completed/1
]).
-endif.

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
-include_lib("arweave_sync/include/arweave_sync.hrl").

-ifdef(AR_TEST).
-export([test_reservation/6, reservation_state/1,
        reservation_peer/1, active_tasks/1,
        test_state/1, test_get/2, test_dispatch/2,
        set_max_active/2]).
-endif.

-opaque reservation() :: #footprint_reservation{}.
-opaque state() :: map().

-include("arweave_sync_footprint.hrl").

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
    FootprintSize = (arweave_sync_deps:constants()):get_replica_2_9_footprint_size(),
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

%% @doc Return the destination store ID.
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
    (arweave_sync_deps:constants()):get_replica_2_9_footprint_size() div ?DATA_CHUNK_SIZE.

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
%%% Test support.
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

-endif.

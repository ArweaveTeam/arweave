%%% @doc Footprint reservations and their entropy slots. A footprint
%%% reservation is the work to fetch the unsynced chunks of one footprint.
%%% This node unpacks those chunks with entropy, so a reservation first
%%% takes an entropy slot (room for one footprint's entropy in the entropy
%%% cache) and then binds to one peer. A reservation is:
%%% - queued: in the store's work queue, with the sources the sweeper last
%%%   offered;
%%% - bound: holding an entropy slot, with the chosen peer's source and the
%%%   chunks not yet turned into tasks;
%%% - draining: giving up its entropy slot to another footprint; its current
%%%   tasks finish, but it creates no new ones.
%%%
%%% arweave_sync_scheduler drives this module in a cycle:
%%% 1. Admission (admit/2), when the sweeper offers a reservation: a new
%%%    reservation is added, and arweave_sync_store claims a whole footprint
%%%    for it; a queued one gets the new sources.
%%% 2. Entropy competition (has_entropy_capacity/3,
%%%    compete_for_entropy_capacity/4), for each queued reservation the
%%%    scheduler takes from a work queue: the reservation takes a free entropy
%%%    slot, or competes for one that a weaker footprint holds. A reservation
%%%    that gets no slot waits for a later pass.
%%% 3. Batching (build_batch/5), once the reservation holds a slot: bind the
%%%    reservation to a peer and turn some of its unsynced chunks into tasks
%%%    for that peer. The reservation keeps the rest for later batches, so its
%%%    chunks do not all become tasks at once: arweave_sync_store puts the
%%%    bound reservations that still have chunks (pending_reservations/1) back
%%%    into the work queues, to be batched again like other work.
%%% 4. Task completion (task_completed/2), after this node unpacks a task's
%%%    chunk or the task fails: remove the task from the reservation's active
%%%    count. Once no task is active and the reservation is draining or has no
%%%    chunks left, release the reservation and its entropy slot.
%%%
%%% Phases 2 and 3 run during a dispatch pass (snapshot/1 to
%%% commit_plan/1).
%%%
%%% Entropy slots (max_active/0): packing.entropy.cache_size divided by the
%%% footprint size, at least one.
%%%
%%% Any chunks still unsynced when a reservation is released are picked up by
%%% the sweeper's next pass.
-module(arweave_sync_footprint).

-ifdef(AR_TEST).
-export([
    compete_with_weakest/3,
    lookup/2,
    task_completed/1
]).
-endif.

-export([new/0, snapshot/1, commit_plan/1, max_active/0,
        new_reservation/3, key/1, store_id/1,
        sources/1, sort_key/1, queue_rank/1, is_busy/1,
        claim_size/1,
        admit/2, reservation/2, build_batch/5,
        has_entropy_capacity/3,
        compete_for_entropy_capacity/4, bound_candidates/1, slot_priority/3,
        is_source_compatible/3,
        pending_reservations/1,
        has_bound_work/1, is_empty/1, task_completed/2,
        bound_count/1, bound_count/2,
        peers/1]).
-export_type([state/0, plan/0, reservation/0]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_sync/include/arweave_sync.hrl").
-include_lib("arweave_sync/include/arweave_sync_deps.hrl").

-ifdef(AR_TEST).
-export([test_reservation/6, test_state/1, test_plan/2]).
-endif.

-opaque reservation() :: #footprint_reservation{}.
-opaque state() :: map().

%% The footprint part of a dispatch plan. The deferred and
%% footprints_draining fields are reset on each pass.
-record(plan, {
    reservations = #{},
    max_active,
    deferred = sets:new(),
    %% The number of footprints this pass holds back to wait for a draining
    %% slot: at most one for each draining footprint, as counted by
    %% draining_count/1.
    footprints_draining = 0
}).

-opaque plan() :: #plan{}.

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Return empty persistent footprint state.
new() ->
    #{}.

new_reservation(StoreID, Footprint, Sources) ->
    #footprint_reservation{
        store_id = StoreID,
        footprint = Footprint,
        sources = Sources
    }.

key(#footprint_reservation{ footprint = Footprint }) ->
    Footprint.

store_id(#footprint_reservation{ store_id = StoreID }) ->
    StoreID.

sources(#footprint_reservation{ sources = Sources }) ->
    Sources.

sort_key(#footprint_reservation{
        footprint = #footprint{ footprint = FootprintIndex } }) ->
    FootprintIndex.

%% @doc At the same queue position, bound reservations refill before queued
%% ones.
queue_rank(#footprint_reservation{ state = bound }) ->
    1;
queue_rank(#footprint_reservation{}) ->
    2.

is_busy(#footprint_reservation{ active_tasks = ActiveTasks }) ->
    ActiveTasks > 0.

%% @doc Return the claim a reservation holds before binding: a whole
%% footprint's worth of chunks.
claim_size(#footprint_reservation{}) ->
    footprint_chunks().

%% @doc Return whether any bound reservation still has intervals to enqueue.
has_bound_work(Footprints) ->
    lists:any(
        fun(Reservation) ->
            case Reservation of
                #footprint_reservation{ state = bound,
                        sources = [#task_source{
                            intervals = Intervals }] } ->
                    not arweave_lib_intervals:is_empty(Intervals);
                _ ->
                    false
            end
        end,
        maps:values(reservations(Footprints))).

is_empty(Footprints) ->
    map_size(reservations(Footprints)) =:= 0.

%% @doc Return the peers of all bound and draining footprints.
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

%%%===================================================================
%%% Admission.
%%%===================================================================

%% @doc Add a new reservation, or refresh a queued one for the same footprint.
%% A claim of zero means the footprint was already tracked.
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

%%%===================================================================
%%% Dispatch: pass.
%%%===================================================================

%% @doc Snapshot footprint state for one scheduler dispatch pass.
snapshot(Reservations) ->
    new_plan(Reservations, max_active()).

new_plan(Reservations, MaxActive) ->
    #plan{ reservations = Reservations, max_active = MaxActive }.

%% @doc Return persistent footprint state after a dispatch pass.
commit_plan(#plan{ reservations = Reservations }) ->
    Reservations.

%% @doc Return the reservation for Footprint, or none if it is missing or
%% deferred in this dispatch pass.
reservation(Footprint, Plan) ->
    #plan{ reservations = Reservations, deferred = Deferred } = Plan,
    case sets:is_element(Footprint, Deferred) of
        true -> none;
        false -> maps:get(Footprint, Reservations, none)
    end.

%% @doc Return the reservation for Footprint, including one deferred in this
%% pass.
lookup(Footprint, Plan) ->
    maps:get(Footprint, reservations(Plan), not_found).

%% @doc Return whether a task or reservation can use a source, given the
%% current footprint state.
is_source_compatible(#footprint_reservation{ state = bound, peer = Peer },
        #task_source{ peer = Peer }, _Plan) ->
    true;
is_source_compatible(#footprint_reservation{ state = queued },
        #task_source{}, _Plan) ->
    true;
is_source_compatible(#footprint_reservation{ state = draining },
        _Source, _Plan) ->
    false;
is_source_compatible(#task{ footprint = none },
        #task_source{ footprint = none }, _Plan) ->
    true;
is_source_compatible(#task{ footprint = none }, _Source, _Plan) ->
    false;
is_source_compatible(#task{ footprint = Footprint },
        #task_source{ peer = Peer,
            footprint = SourceFootprint }, Plan) ->
    case maps:get(Footprint, reservations(Plan), undefined) of
        #footprint_reservation{ state = State, peer = Peer,
                sources = [#task_source{
                    footprint = SourceFootprint }] }
                when State =:= bound; State =:= draining ->
            true;
        _ ->
            false
    end.

%%%===================================================================
%%% Entropy competition.
%%%===================================================================

%% @doc Return how many entropy slots footprints may hold at once.
max_active() ->
    EntropyCacheSizeMiB = ?DEP(config):get([packing, entropy, cache_size]),
    FootprintSize = arweave_lib_constants:get_replica_2_9_footprint_size(),
    max(1, (EntropyCacheSizeMiB * ?MiB) div FootprintSize).

%% @doc Return whether Source can bind without displacing a footprint that
%% already occupies an entropy slot. The scheduler dispatches only queued and
%% bound reservations; a draining or missing one never gets here.
has_entropy_capacity(Footprint,
        #task_source{ footprint = #footprint{} }, Plan) ->
    #plan{ max_active = MaxActive } = Plan,
    case lookup(Footprint, Plan) of
        #footprint_reservation{ state = queued } ->
            bound_count(Plan) < MaxActive;
        #footprint_reservation{ state = bound } ->
            %% A bound reservation already holds a slot.
            true
    end.

%% @doc Count the footprints that hold an entropy slot, including draining
%% ones, which keep their slot until their active tasks finish.
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

%% @doc Count the entropy slots held by one destination store.
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

%% @doc Let a queued footprint compete for an entropy slot when none is free.
%%
%% The footprint competes with the weakest bound footprint, by slot_priority/3.
%% If the queued footprint wins, the incumbent is released when it has no
%% active tasks, or starts draining otherwise. A footprint that loses, or that
%% has to wait for a draining slot, sits out the rest of the pass.
compete_for_entropy_capacity(Footprint, CandidatePriority, BoundPriorities,
        Plan) ->
    #plan{
        reservations = Reservations,
        footprints_draining = FootprintsDraining
    } = Plan,
    case FootprintsDraining < draining_count(Reservations) of
        true ->
            defer(Footprint, Plan#plan{
                footprints_draining = FootprintsDraining + 1
            });
        false ->
            case compete_with_weakest(
                    CandidatePriority, BoundPriorities, Reservations) of
                {released, Reservations2} ->
                    {ready, Plan#plan{
                        reservations = Reservations2 }};
                {draining, Reservations2} ->
                    defer(Footprint, Plan#plan{
                        reservations = Reservations2,
                        footprints_draining = FootprintsDraining + 1
                    });
                lost ->
                    defer(Footprint, Plan)
            end
    end.

defer(Footprint, #plan{ deferred = Deferred } = Plan) ->
    {deferred, Plan#plan{ deferred =
        sets:add_element(Footprint, Deferred) }}.

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

compete_with_weakest(CandidatePriority, BoundPriorities, Reservations) ->
    maybe
        [_ | _] ?= BoundPriorities,
        %% The weakest priority sorts last.
        {IncumbentPriority, Footprint} = lists:max(BoundPriorities),
        true ?= candidate_wins(CandidatePriority, IncumbentPriority),
        Incumbent = maps:get(Footprint, Reservations),
        case Incumbent#footprint_reservation.active_tasks of
            0 ->
                {released, release(Footprint, Reservations)};
            _ ->
                Draining = Incumbent#footprint_reservation{ state = draining },
                {draining, Reservations#{Footprint := Draining}}
        end
    else
        [] -> lost;
        false -> lost
    end.

%% @doc Decide whether a queued footprint takes an incumbent's entropy slot.
%% Store counts decide on their own only when the move evens out the stores'
%% shares; taking a store's one extra footprint would just swap which store
%% waits, and waste the entropy already generated for the unfinished
%% footprint. Otherwise, a store that can take the work beats one that cannot,
%% and peer priority decides the rest.
candidate_wins({CandidateStoreCount, _CandidateStoreAvailability,
        _CandidatePeerPriority},
        {IncumbentStoreCount, _IncumbentStoreAvailability,
            _IncumbentPeerPriority})
        when CandidateStoreCount + 1 < IncumbentStoreCount ->
    true;
candidate_wins({CandidateStoreCount, _CandidateStoreAvailability,
        _CandidatePeerPriority},
        {IncumbentStoreCount, _IncumbentStoreAvailability,
            _IncumbentPeerPriority})
        when CandidateStoreCount > IncumbentStoreCount ->
    false;
candidate_wins({_CandidateStoreCount, 0, _CandidatePeerPriority},
        {_IncumbentStoreCount, 1, _IncumbentPeerPriority}) ->
    true;
candidate_wins({_CandidateStoreCount, 1, _CandidatePeerPriority},
        {_IncumbentStoreCount, 0, _IncumbentPeerPriority}) ->
    false;
candidate_wins({_CandidateStoreCount, _CandidateStoreAvailability,
        CandidatePeerPriority},
        {_IncumbentStoreCount, _IncumbentStoreAvailability,
            IncumbentPeerPriority}) ->
    arweave_sync_peer:compare_priorities(
        CandidatePeerPriority, IncumbentPeerPriority
    ) =:= better.

%% @doc Return every bound reservation, paired with the source it is bound to.
%% A footprint can still be displaced after all its work is in peer queues;
%% otherwise that queued work could keep a newly available store from getting
%% the slot.
bound_candidates(Plan) ->
    maps:fold(
        fun(_Footprint, #footprint_reservation{ state = bound,
                    sources = [Source] } = Reservation, Acc) ->
                [{Reservation, Source} | Acc];
           (_Footprint, _Reservation, Acc) ->
                Acc
        end,
        [],
        reservations(Plan)).

%% @doc Build a footprint's priority for an entropy slot; the lowest priority
%% wins. Footprints compare by, in order:
%% - the entropy slots their store already holds (StoreCount), fewest first;
%% - whether their store can take the work (StoreAvailable), able first;
%% - their peer's priority (PeerPriority), from arweave_sync_peer:priority/3.
slot_priority(StoreCount, StoreAvailable, PeerPriority) ->
    StoreAvailability =
        case StoreAvailable of
            true -> 0;
            false -> 1
        end,
    {StoreCount, StoreAvailability, PeerPriority}.

%%%===================================================================
%%% Batching.
%%%===================================================================

%% @doc Turn up to Limit chunks of AvailableIntervals into tasks for the
%% source's peer, and return the reservation bound to that peer. If no task is
%% active and no chunk is left, the reservation is released and none is
%% returned.
build_batch(Reservation, Source, AvailableIntervals, Limit, Plan) ->
    #task_source{ peer = Peer, footprint = Footprint } = Source,
    {BatchIntervals, RemainingIntervals} = split_intervals(
        AvailableIntervals, Limit),
    Tasks = build_tasks(Reservation, Peer, Footprint, BatchIntervals),
    EnqueuedCount = length(Tasks),
    ActiveTasks = Reservation#footprint_reservation.active_tasks
        + EnqueuedCount,
    BoundReservation =
        case ActiveTasks =:= 0
                andalso arweave_lib_intervals:is_empty(RemainingIntervals) of
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
        end,
    FootprintKey = key(Reservation),
    Plan2 = put_reservation(FootprintKey, BoundReservation, Plan),
    {Plan2, Tasks, BoundReservation}.

%% @doc Split Intervals into a batch of up to Limit chunks, taken from the
%% lowest offsets, and the rest. An interval that crosses the limit is cut at a
%% chunk boundary, and a partial chunk counts as a whole one.
split_intervals(Intervals, Limit) ->
    {_Remaining, Batch, Rest} = arweave_lib_intervals:fold(
        fun({End, Start}, {0, BatchAcc, RestAcc}) ->
                {0, BatchAcc, arweave_lib_intervals:add(RestAcc, End, Start)};
           ({End, Start}, {Remaining, BatchAcc, RestAcc}) ->
                ChunkCount = (End - Start + ?DATA_CHUNK_SIZE - 1)
                    div ?DATA_CHUNK_SIZE,
                case ChunkCount =< Remaining of
                    true ->
                        {Remaining - ChunkCount,
                            arweave_lib_intervals:add(BatchAcc, End, Start),
                            RestAcc};
                    false ->
                        BatchEnd = Start + Remaining * ?DATA_CHUNK_SIZE,
                        {0,
                            arweave_lib_intervals:add(
                                BatchAcc, BatchEnd, Start),
                            arweave_lib_intervals:add(RestAcc, End, BatchEnd)}
                end
        end,
        {Limit, arweave_lib_intervals:new(), arweave_lib_intervals:new()},
        Intervals),
    {Batch, Rest}.

build_tasks(Reservation, Peer, Footprint, Intervals) ->
    Tasks = arweave_lib_intervals:fold(
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
    Task = #task{
        offset = Offset,
        sources = [#task_source{ peer = Peer, footprint = Footprint }],
        peer = undefined,
        store_id = Reservation#footprint_reservation.store_id,
        footprint = Reservation#footprint_reservation.footprint,
        state = queued
    },
    build_interval_tasks(Offset + ?DATA_CHUNK_SIZE, End,
        Reservation, Peer, Footprint, [Task | Tasks]).

%% @doc Return the bound reservations that still have source intervals to
%% enqueue in this dispatch pass.
pending_reservations(#plan{} = Plan) ->
    pending_reservations(maps:values(reservations(Plan)));
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
    case arweave_lib_intervals:is_empty(Intervals) of
        true -> none;
        false -> Reservation
    end;
pending_reservation(_Reservation) ->
    none.

%%%===================================================================
%%% Task completion.
%%%===================================================================

%% @doc Count a completed task out of its reservation, and release the
%% reservation once it has no work left.
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
            orelse arweave_lib_intervals:is_empty(RemainingIntervals)),
    case ShouldRelease of
        true -> release;
        false ->
            Reservation#footprint_reservation{ active_tasks = ActiveTasks2 }
    end.

%%%===================================================================
%%% Shared helpers.
%%%===================================================================

release(Footprint, Reservations) ->
    maps:remove(Footprint, Reservations).

footprint_chunks() ->
    arweave_lib_constants:get_replica_2_9_footprint_size() div ?DATA_CHUNK_SIZE.

reservations(#plan{ reservations = Reservations }) ->
    Reservations;
reservations(Reservations) when is_map(Reservations) ->
    Reservations.

put_reservation(Footprint, none, #plan{ reservations = Reservations } = Plan) ->
    Plan#plan{ reservations = release(Footprint, Reservations) };
put_reservation(Footprint, Reservation,
        #plan{ reservations = Reservations } = Plan) ->
    Plan#plan{ reservations =
        maps:put(Footprint, Reservation, Reservations) }.

%%%===================================================================
%%% Test support.
%%%===================================================================

-ifdef(AR_TEST).
test_plan(Reservations, MaxActive) ->
    new_plan(Reservations, MaxActive).

test_reservation(StoreID, Footprint, Sources, Peer, ActiveTasks, State) ->
    #footprint_reservation{
        store_id = StoreID,
        footprint = Footprint,
        sources = Sources,
        peer = Peer,
        active_tasks = ActiveTasks,
        state = State
    }.

test_state(Reservations) ->
    maps:from_list([{key(Reservation), Reservation}
        || Reservation <- Reservations]).

-endif.

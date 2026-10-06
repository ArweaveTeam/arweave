%%% @doc Per-peer queues, limits and source selection for sync. For each peer,
%%% this node keeps a peer queue of tasks bound to the peer, and two limits on
%%% its own work:
%%% - the concurrency cap: how many fetches this node runs against the peer at
%%%   once;
%%% - the queue length: how many more tasks may wait in the peer queue to be
%%%   fetched.
%%%
%%% arweave_sync_scheduler drives this module in a cycle:
%%% 1. Tick (tick/3), once per scheduler tick: update each peer's concurrency
%%%    cap, through arweave_sync_peer_cap, and its queue length from the inputs
%%%    gathered since the last tick, then reset those inputs for the next one:
%%%    - the goodput sample: the bytes add_fetch_result/4 added;
%%%    - the failure pressure: the fetch times add_fetch_result/4 added;
%%%    - whether the peer was driven, as update_driven/3 marked it.
%%% 2. Dispatch (snapshot/2 to commit_plan/2), any number of times until the
%%%    next tick: copy each active peer's queue and limits into a plan, bind
%%%    tasks to peer queues and take tasks to fetch within those limits, then
%%%    commit the queues. update_driven/3 marks the peers that were driven.
%%% 3. Fetch results (add_fetch_result/4), after each fetch: add the bytes it
%%%    fetched and its fetch time to the peer's totals for the next tick.
%%%
%%% A plan is this module's part of one dispatch pass: a copy of each active
%%% peer's queue and limits, and the count of tasks it holds, which the pass
%%% changes as it binds tasks and takes tasks to fetch.
%%%
%%% Queue length (queue_max_length): about four seconds of the peer's recent
%%% goodput, the mean goodput of its last GOODPUT_WINDOW_SAMPLES goodput
%%% samples.
%%%
%%% arweave_sync_peer_cap.hrl defines the terms goodput sample and driven.
%%%
%%% This node keeps a peer's cap control state when the peer leaves the active
%%% set, so a returning peer resumes from its previous cap.
-module(arweave_sync_peer).

-ifdef(AR_TEST).
-export([
    can_start_fetch/2,
    clear_queues/1,
    concurrency_cap/2,
    sample_goodput/2,
    driven_peers/1,
    failure_pressure/1,
    fetching_count/2,
    load/2,
    new_plan/2,
    peer_plan/2,
    peer_priority/3,
    queued_tasks/2,
    queue_max_length/1,
    set_store_task_targets/3,
    start_task/3,
    store_load/3,
    take_startable_task/2,
    tick_peer/3
]).
-endif.

-export([
    new/0,
    %% Tick.
    tick/3,
    %% Dispatch.
    snapshot/2, update_driven/3, commit_plan/2,
    enqueue_tasks/3, take_fetch/2,
    set_store_task_targets/2,
    has_capacity/3, queue_capacity/2,
    priority/3, compare_priorities/2, best_source/3,
    %% Peer queues.
    queued_tasks/1, queue_lengths/1, has_queued_tasks/1,
    %% Fetch results.
    add_fetch_result/4
]).
-export_type([state/0, plan/0]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_sync/include/arweave_sync.hrl").

-include("arweave_sync_peer.hrl").

-opaque state() :: #state{}.

-opaque plan() :: #plan{}.

%%%===================================================================
%%% State.
%%%===================================================================

%% @doc Return fresh peer state that tracks no peers.
new() ->
    #state{}.

%%%===================================================================
%%% Tick: measure, update, publish.
%%%===================================================================

%% @doc Run one control tick over the active peers: measure each peer's goodput
%% and failure pressure from the fetch results since the previous tick, update
%% its cap, and size its peer queue.
%%
%% ActivePeers: the peers with work. Every other peer keeps its cap control but
%% has no cap in the plan until it is active again.
tick(ActivePeers, NowMs, #state{ peers = Peers } = State) ->
    Active = sets:from_list(ActivePeers),
    %% An active peer may have no entry yet, for example one that is only a
    %% source of queued work; it starts from a fresh #peer{}.
    Peers2 = maps:map(
        fun(Peer, PeerState) ->
            case sets:is_element(Peer, Active) of
                true -> tick_peer(Peer, NowMs, PeerState);
                false -> idle_peer(PeerState)
            end
        end,
        maps:merge(maps:from_keys(ActivePeers, #peer{}), Peers)),
    State2 = State#state{ peers = Peers2 },
    publish(State2),
    State2.

-ifdef(AR_TEST).
%% @doc Return the peers that were driven in any dispatch pass since the last
%% tick.
driven_peers(#state{ peers = Peers }) ->
    maps:keys(maps:filter(fun(_Peer, #peer{ driven = Driven }) -> Driven end,
        Peers)).
-endif.

%% @doc Update one active peer's cap from its fetch results since the previous
%% tick, and start the peer's next interval.
tick_peer(Peer, NowMs, PeerState) ->
    #peer{
        control = Control,
        fetched_bytes = FetchedBytes,
        fetch_timing = FetchTiming,
        last_tick = LastTick,
        driven = Driven,
        fetching_count = FetchingCount
    } = PeerState,
    Tick = {FetchedBytes, NowMs},
    Sample = sample_goodput(LastTick, Tick),
    {FailurePressure, FailureMs} = failure_pressure(FetchTiming),
    Control2 = arweave_sync_peer_cap:update(Sample, FailurePressure, Driven,
        Control),
    PeerState2 = add_recent_goodput(Sample, FetchingCount,
        PeerState#peer{ control = Control2 }),
    log_cap_decision(Peer, arweave_sync_peer_cap:cap(Control), PeerState2,
        goodput(Sample), FailurePressure, FailureMs, Driven, FetchTiming),
    %% Start the next interval with fresh inputs.
    PeerState2#peer{
        fetch_timing = #fetch_timing{},
        last_tick = Tick,
        driven = false
    }.

%% @doc Start the next interval for a peer that is not active.
idle_peer(PeerState) ->
    PeerState#peer{
        fetch_timing = #fetch_timing{},
        last_tick = undefined,
        driven = false
    }.

%%%===================================================================
%%% Tick: measurement.
%%%===================================================================

%% @doc Return the goodput sample between two tick snapshots,
%% {FetchedBytes, ElapsedMs}, or undefined if there is no previous snapshot.
sample_goodput({PrevBytes, PrevTime}, {Bytes, Time})
        when Time > PrevTime andalso Bytes >= PrevBytes ->
    {Bytes - PrevBytes, Time - PrevTime};
sample_goodput(_Previous, _Tick) ->
    undefined.

%% @doc Convert a goodput sample to bytes per millisecond.
goodput({Bytes, Ms}) -> Bytes / Ms;
goodput(undefined) -> 0.0.

%% @doc Return the share of the peer's fetch time spent on 429s, 503s, timeouts
%% and client errors, and that failure time in milliseconds. Client errors
%% include requests that this node's own HTTP client drops when it has too
%% many open connections.
failure_pressure(FetchTiming) ->
    #fetch_timing{
        productive_ms = ProductiveMs,
        reject_ms = RejectMs,
        timeout_ms = TimeoutMs,
        client_error_ms = ClientErrorMs
    } = FetchTiming,
    FailureMs = RejectMs + TimeoutMs + ClientErrorMs,
    TotalMs = ProductiveMs + FailureMs,
    FailurePressure = case TotalMs of
        0 -> 0.0;
        _ -> FailureMs / TotalMs
    end,
    {FailurePressure, FailureMs}.

%%%===================================================================
%%% Tick: peer queue.
%%%===================================================================

%% @doc Add a goodput sample to the peer's recent goodputs, which size its
%% queue, whether or not the peer was driven.
%%
%% FetchingCount: if nothing was fetching and nothing was fetched, the sample
%% is skipped rather than counted as zero; the peer was idle because the
%% scheduler gave it no work, not because it is slow.
add_recent_goodput(undefined, _FetchingCount, PeerState) ->
    PeerState;
add_recent_goodput({Bytes, _Ms}, 0, PeerState) when Bytes == 0 ->
    PeerState;
add_recent_goodput(Sample, _FetchingCount,
        #peer{ recent_goodputs = Goodputs } = PeerState) ->
    PeerState#peer{ recent_goodputs = lists:sublist(
        [goodput(Sample) | Goodputs], ?GOODPUT_WINDOW_SAMPLES) }.

%% @doc Return the peer's mean goodput over its last ?GOODPUT_WINDOW_SAMPLES
%% goodput samples, used to size its queue. The mean smooths over one bursty
%% tick but still follows a real change within a few ticks.
recent_goodput(#peer{ recent_goodputs = [] }) ->
    0.0;
recent_goodput(#peer{ recent_goodputs = Goodputs }) ->
    lists:sum(Goodputs) / length(Goodputs).

%% @doc Return how many tasks may wait behind active fetches: about
%% ?QUEUE_TARGET_DURATION_MS of the peer's goodput.
queue_max_length(Goodput) when Goodput > 0.0 ->
    max(?CONCURRENCY_CAP_INITIAL, round(
        Goodput * ?QUEUE_TARGET_DURATION_MS / ?DATA_CHUNK_SIZE));
queue_max_length(_Goodput) ->
    ?CONCURRENCY_CAP_INITIAL.

%%%===================================================================
%%% Tick: publication.
%%%===================================================================

%% @doc Publish and log the inputs and limits behind one peer's cap decision.
log_cap_decision(Peer, PrevCap, PeerState, Goodput, FailurePressure, FailureMs,
        Driven, FetchTiming) ->
    #peer{ control = Control } = PeerState,
    Cap = arweave_sync_peer_cap:cap(Control),
    Phase = arweave_sync_peer_cap:phase(Control),
    QueueMaxLength = queue_max_length(recent_goodput(PeerState)),
    arweave_sync_metrics:publish_peer(Peer, Cap, Goodput, FailurePressure),
    ?LOG_DEBUG([{event, sync_peer_cap_decision},
        {peer, arweave_lib_util:format_peer(Peer)},
        {previous_cap, PrevCap},
        {cap, Cap},
        {phase, Phase},
        {goodput_mib_per_second, Goodput * 1000 / ?MiB},
        {queue_max_length, QueueMaxLength},
        {failure_pressure, FailurePressure},
        {productive_worker_ms, FetchTiming#fetch_timing.productive_ms},
        {failure_worker_ms, FailureMs},
        {reject_worker_ms, FetchTiming#fetch_timing.reject_ms},
        {timeout_worker_ms, FetchTiming#fetch_timing.timeout_ms},
        {client_error_worker_ms, FetchTiming#fetch_timing.client_error_ms},
        {driven, Driven}]).

publish(#state{ peers = Peers }) ->
    Active = maps:filter(fun(_Peer, PeerState) -> is_active(PeerState) end,
        Peers),
    TotalCap = maps:fold(
        fun(_Peer, #peer{ control = Control }, Acc) ->
            Acc + arweave_sync_peer_cap:cap(Control)
        end,
        0,
        Active),
    arweave_sync_metrics:publish_peer_caps(maps:keys(Active), TotalCap).

is_active(#peer{ last_tick = LastTick }) ->
    LastTick =/= undefined.

%%%===================================================================
%%% Dispatch: snapshot.
%%%===================================================================

%% @doc Return a plan for one dispatch pass: each active peer's cap and queue
%% length from the last tick, with the peer's queue and current fetches.
%%
%% Tasks: the scheduler's tasks; each fetching task counts against its peer's
%% concurrency cap.
snapshot(Tasks, #state{ peers = Peers }) ->
    PeerPlans = maps:filtermap(
        fun(_Peer, PeerState) ->
            case is_active(PeerState) of
                true -> {true, new_peer_plan(PeerState)};
                false -> false
            end
        end,
        Peers),
    Plan = count_fetching_tasks(Tasks, #plan{ peers = PeerPlans }),
    maps:fold(
        fun(Peer, #peer{ queue = Queue }, Acc) ->
            enqueue_tasks(Peer, queue:to_list(Queue), Acc)
        end,
        Plan,
        Peers).

new_peer_plan(#peer{ control = Control } = PeerState) ->
    #peer_plan{
        concurrency_cap = max(?CONCURRENCY_CAP_MIN,
            arweave_sync_peer_cap:cap(Control)),
        queue_max_length = queue_max_length(recent_goodput(PeerState))
    }.

%% @doc Update each peer's fetching count and driven flag from the plan.
%%
%% GatesOpen: whether the shared gates (download limit, chunk cache) are open;
%% while they are closed, no peer is driven.
update_driven(#plan{ peers = PeerPlans }, GatesOpen,
        #state{ peers = Peers } = State) ->
    Peers2 = maps:map(
        fun(Peer, PeerState) ->
            update_peer_driven(
                maps:get(Peer, PeerPlans, #peer_plan{}),
                GatesOpen, PeerState)
        end,
        maps:merge(maps:from_keys(maps:keys(PeerPlans), #peer{}), Peers)),
    State#state{ peers = Peers2 }.

update_peer_driven(#peer_plan{ fetching_count = FetchingCount,
        task_count = TaskCount,
        concurrency_cap = ConcurrencyCap }, GatesOpen,
        #peer{ driven = Driven } = PeerState) ->
    Queued = TaskCount > FetchingCount,
    PeerState#peer{
        fetching_count = FetchingCount,
        driven = Driven orelse (GatesOpen andalso Queued
            andalso FetchingCount >= ConcurrencyCap)
    }.

%% @doc Write each peer's queue from the plan back to the peer state.
commit_plan(#plan{ peers = PeerPlans },
        #state{ peers = Peers } = State) ->
    Peers2 = maps:fold(
        fun(Peer, #peer_plan{ queue = Queue }, Acc) ->
            PeerState = maps:get(Peer, Acc, #peer{}),
            maps:put(Peer, PeerState#peer{ queue = Queue }, Acc)
        end,
        Peers,
        PeerPlans),
    State#state{ peers = Peers2 }.

count_fetching_tasks(Tasks, Plan) ->
    maps:fold(
        fun(_TaskRef, #task{ state = fetching, peer = Peer } = Task, Acc) ->
                count_fetching_task(Peer, Task, Acc);
           (_TaskRef, _Task, Acc) ->
                Acc
        end,
        Plan,
        Tasks).

-ifdef(AR_TEST).
new_plan(ConcurrencyCaps, QueueMaxLengths) ->
    Peers = maps:map(
        fun(Peer, ConcurrencyCap) ->
            #peer_plan{
                concurrency_cap = max(
                    ?CONCURRENCY_CAP_MIN, ConcurrencyCap),
                queue_max_length = maps:get(
                    Peer, QueueMaxLengths, ?CONCURRENCY_CAP_INITIAL)
            }
        end,
        ConcurrencyCaps),
    #plan{ peers = Peers }.
-endif.

%% @doc Add tasks to the end of the peer's queue.
enqueue_tasks(_Peer, [], Plan) ->
    Plan;
enqueue_tasks(Peer, Tasks, Plan) ->
    PeerPlan = lists:foldl(
        fun(Task, #peer_plan{ queue = Queue } = Acc) ->
            count_task(Task, Acc#peer_plan{ queue = queue:in(Task, Queue) })
        end,
        peer_plan(Peer, Plan),
        Tasks),
    put_peer_plan(Peer, PeerPlan, Plan).

%% @doc Take a task to fetch from the queue of the peer with the fewest fetches
%% in flight among the peers below their cap, and count it as fetching.
%%
%% StoreRank: fun(StoreID) returning false while the store cannot start a
%% fetch, and {true, Rank} otherwise. Within a peer's queue, the task whose
%% store has the lowest rank starts first; ties go to the oldest task.
take_fetch(StoreRank, #plan{ peers = Peers } = Plan) ->
    Candidates = maps:fold(
        fun(Peer, #peer_plan{ queue = Queue,
                fetching_count = FetchingCount } = PeerPlan, Acc) ->
            case can_start(PeerPlan)
                    andalso take_startable_task(Queue, StoreRank) of
                {Task, Queue2} ->
                    [{{FetchingCount, Peer}, Task, Queue2} | Acc];
                _ ->
                    Acc
            end
        end,
        [],
        Peers),
    case Candidates of
        [] ->
            none;
        _ ->
            {{_FetchingCount, Peer}, Task, Queue} = lists:min(Candidates),
            PeerPlan = peer_plan(Peer, Plan),
            {Task, count_fetch(Peer, put_peer_plan(Peer,
                PeerPlan#peer_plan{ queue = Queue }, Plan))}
    end.

take_startable_task(Queue, StoreRank) ->
    Tasks = queue:to_list(Queue),
    case best_startable_task(Tasks, StoreRank, 0, none) of
        none ->
            none;
        {_Rank, Index} ->
            {Before, [Task | After]} = lists:split(Index, Tasks),
            {Task, queue:from_list(Before ++ After)}
    end.

best_startable_task([], _StoreRank, _Index, Best) ->
    Best;
best_startable_task([#task{ store_id = StoreID } | Rest], StoreRank, Index,
        Best) ->
    Best2 =
        case StoreRank(StoreID) of
            false -> Best;
            {true, Rank} when Best =:= none -> {Rank, Index};
            {true, Rank} -> min(Best, {Rank, Index})
        end,
    best_startable_task(Rest, StoreRank, Index + 1, Best2).

-ifdef(AR_TEST).
%% @doc Count a started fetch for a task already counted in the peer queue.
start_task(Peer, #task{}, Plan) ->
    count_fetch(Peer, Plan).
-endif.

count_fetching_task(Peer, Task, Plan) ->
    count_fetch(Peer, count_peer_task(Peer, Task, Plan)).

count_fetch(Peer, Plan) ->
    PeerPlan = peer_plan(Peer, Plan),
    PeerPlan2 = PeerPlan#peer_plan{
        fetching_count = PeerPlan#peer_plan.fetching_count + 1
    },
    put_peer_plan(Peer, PeerPlan2, Plan).

count_peer_task(Peer, Task, Plan) ->
    put_peer_plan(Peer, count_task(Task, peer_plan(Peer, Plan)),
        Plan).

%% @doc Count a queued or fetching task against the peer and its store.
count_task(#task{ store_id = StoreID }, #peer_plan{
        task_count = TaskCount,
        stores = Stores } = PeerPlan) ->
    StoreLoad = maps:get(StoreID, Stores, #store_load{}),
    StoreLoad2 = StoreLoad#store_load{
        task_count = StoreLoad#store_load.task_count + 1
    },
    PeerPlan#peer_plan{
        task_count = TaskCount + 1,
        stores = maps:put(StoreID, StoreLoad2, Stores)
    }.

peer_plan(Peer, #plan{ peers = Peers }) ->
    maps:get(Peer, Peers, #peer_plan{}).

put_peer_plan(Peer, PeerPlan,
        #plan{ peers = Peers } = Plan) ->
    Plan#plan{ peers = maps:put(Peer, PeerPlan, Peers) }.

store_load(StoreID, #peer_plan{ stores = Stores }) ->
    maps:get(StoreID, Stores, #store_load{}).

%%%===================================================================
%%% Dispatch: store targets.
%%%===================================================================

%% @doc Set each local store's task target on every peer with work.
set_store_task_targets(StoresByPeer, #plan{ peers = Peers } = Plan) ->
    Plan#plan{ peers = maps:map(
        fun(Peer, PeerPlan) ->
            do_set_store_task_targets(
                maps:get(Peer, StoresByPeer, []), PeerPlan)
        end,
        Peers) }.

-ifdef(AR_TEST).
%% @doc Split one peer's task limit across the supplied stores.
set_store_task_targets(Peer, StoreIDs, Plan) ->
    PeerPlan = peer_plan(Peer, Plan),
    PeerPlan2 = do_set_store_task_targets(StoreIDs, PeerPlan),
    put_peer_plan(Peer, PeerPlan2, Plan).
-endif.

do_set_store_task_targets(StoreIDs, PeerPlan) ->
    StoreIDsWithTasks = stores_with_tasks(PeerPlan),
    set_store_task_targets_for_stores(
        lists:usort(StoreIDs ++ StoreIDsWithTasks), PeerPlan).

stores_with_tasks(#peer_plan{ stores = Stores }) ->
    maps:fold(
        fun(StoreID, #store_load{ task_count = Count }, Acc)
                    when Count > 0 ->
                [StoreID | Acc];
           (_StoreID, _StoreLoad, Acc) ->
                Acc
        end,
        [],
        Stores).

set_store_task_targets_for_stores([], PeerPlan) ->
    clear_store_task_targets(PeerPlan);
set_store_task_targets_for_stores(StoreIDs, PeerPlan) ->
    TaskLimit = peer_task_limit(PeerPlan),
    Target = erlang:ceil(TaskLimit / length(StoreIDs)),
    PeerPlan2 = clear_store_task_targets(PeerPlan),
    lists:foldl(
        fun(StoreID, Acc) -> set_store_task_target(StoreID, Target, Acc) end,
        PeerPlan2,
        StoreIDs).

clear_store_task_targets(#peer_plan{ stores = Stores } = PeerPlan) ->
    PeerPlan#peer_plan{ stores = maps:map(
        fun(_StoreID, StoreLoad) ->
            StoreLoad#store_load{ target_task_count = 0 }
        end,
        Stores) }.

set_store_task_target(StoreID, Target,
        #peer_plan{ stores = Stores } = PeerPlan) ->
    StoreLoad = maps:get(StoreID, Stores, #store_load{}),
    PeerPlan#peer_plan{ stores = maps:put(StoreID,
        StoreLoad#store_load{ target_task_count = Target }, Stores) }.

%%%===================================================================
%%% Dispatch: capacity.
%%%===================================================================

-ifdef(AR_TEST).
concurrency_cap(Peer, Plan) ->
    peer_concurrency_cap(peer_plan(Peer, Plan)).
-endif.

peer_concurrency_cap(#peer_plan{ concurrency_cap = ConcurrencyCap }) ->
    ConcurrencyCap.

%% @doc Return the fraction of this peer's task limit in use.
load(Peer, Plan) ->
    peer_load(peer_plan(Peer, Plan)).

peer_load(#peer_plan{ task_count = TaskCount } =
        PeerPlan) ->
    case peer_task_limit(PeerPlan) of
        Limit when Limit > 0 -> TaskCount / Limit;
        _ -> 1.0
    end.

-ifdef(AR_TEST).
fetching_count(Peer, Plan) ->
    (peer_plan(Peer, Plan))#peer_plan.fetching_count.
-endif.

%% @doc Return the fraction of the store's target on this peer that is taken
%% by queued or fetching tasks.
store_load(Peer, StoreID, Plan) ->
    StoreLoad = store_load(StoreID, peer_plan(Peer, Plan)),
    case StoreLoad#store_load.target_task_count of
        Target when Target > 0 ->
            StoreLoad#store_load.task_count / Target;
        _ ->
            1.0
    end.

%% @doc Return how many more tasks may wait in the peer queue.
queue_capacity(Peer, Plan) ->
    peer_queue_capacity(peer_plan(Peer, Plan)).

-ifdef(AR_TEST).
can_start_fetch(Peer, Plan) ->
    can_start(peer_plan(Peer, Plan)).

queued_tasks(Peer, Plan) ->
    queue:to_list((peer_plan(Peer, Plan))#peer_plan.queue).

clear_queues(#state{ peers = Peers } = State) ->
    State#state{ peers = maps:map(
        fun(_Peer, PeerState) -> PeerState#peer{ queue = queue:new() } end,
        Peers) }.
-endif.

can_start(#peer_plan{ fetching_count = FetchingCount,
        concurrency_cap = ConcurrencyCap }) ->
    FetchingCount < ConcurrencyCap.

%% @doc Return whether a task or footprint may take another slot in its source
%% peer's queue, within its store's share of that peer.
has_capacity(#task{ store_id = StoreID },
        #task_source{ peer = Peer }, Plan) ->
    has_store_capacity(StoreID, peer_plan(Peer, Plan));
has_capacity(#footprint_reservation{ store_id = StoreID },
        #task_source{ peer = Peer }, Plan) ->
    has_store_capacity(StoreID, peer_plan(Peer, Plan)).

has_store_capacity(StoreID, PeerPlan) ->
    peer_queue_capacity(PeerPlan) > 0
        andalso has_task_capacity(StoreID, PeerPlan).

has_task_capacity(StoreID, PeerPlan) ->
    peer_task_capacity(PeerPlan) > 0
        andalso store_task_capacity(StoreID, PeerPlan) > 0.

store_task_capacity(StoreID, PeerPlan) ->
    case store_is_explored(StoreID, PeerPlan)
            orelse store_exploration_available(PeerPlan) of
        true -> min(peer_task_capacity(PeerPlan),
            store_queue_capacity(StoreID, PeerPlan));
        false -> 0
    end.

store_queue_capacity(StoreID, PeerPlan) ->
    StoreLoad = store_load(StoreID, PeerPlan),
    max(0, store_target(StoreLoad, PeerPlan)
        - StoreLoad#store_load.task_count).

store_target(#store_load{ target_task_count = 0 }, PeerPlan) ->
    peer_task_limit(PeerPlan);
store_target(#store_load{ target_task_count = Target }, _PeerPlan) ->
    Target.

store_is_explored(StoreID, #peer_plan{ stores = Stores }) ->
    StoreLoad = maps:get(StoreID, Stores, #store_load{}),
    StoreLoad#store_load.task_count > 0.

%% @doc Return whether this node may bind work for one more store to the peer.
%% A peer may serve as many stores as its cap, but never fewer than
%% ?CONCURRENCY_CAP_INITIAL, so a fast peer can show that it serves several
%% stores in parallel before its cap has grown.
store_exploration_available(#peer_plan{
        concurrency_cap = ConcurrencyCap,
        stores = Stores }) ->
    ExploredStoreCount = maps:fold(
        fun(_ID, #store_load{ task_count = Count }, Acc)
                    when Count > 0 ->
                Acc + 1;
           (_ID, #store_load{}, Acc) ->
                Acc
        end,
        0,
        Stores),
    ExploredStoreCount < max(?CONCURRENCY_CAP_INITIAL, ConcurrencyCap).

peer_task_capacity(#peer_plan{
        task_count = TaskCount } = PeerPlan) ->
    max(0, peer_task_limit(PeerPlan) - TaskCount).

peer_task_limit(#peer_plan{
        concurrency_cap = ConcurrencyCap,
        queue_max_length = QueueMaxLength
    }) ->
    ConcurrencyCap + QueueMaxLength.

peer_queue_capacity(#peer_plan{
        fetching_count = FetchingCount,
        task_count = TaskCount,
        queue_max_length = QueueMaxLength
    }) ->
    QueuedTaskCount = TaskCount - FetchingCount,
    max(0, QueueMaxLength - QueuedTaskCount).

%%%===================================================================
%%% Dispatch: source selection.
%%%===================================================================

%% @doc Rank the source's peer for a footprint's store; the lowest rank wins.
%% Peers compare by, in order:
%% - whether the peer is available, available first. It is available if it
%%   has room for the store, or if the footprint already has tasks on it,
%%   since those tasks may be what fills the room;
%% - the peer's load, lowest first;
%% - the peer's concurrency cap, highest first.
%% compare_priorities/2 compares two of these ranks.
priority(#footprint_reservation{ store_id = StoreID,
        active_tasks = ActiveTasks },
        #task_source{ peer = Peer }, Plan) ->
    PeerPlan = peer_plan(Peer, Plan),
    Available = ActiveTasks > 0
        orelse has_store_capacity(StoreID, PeerPlan),
    peer_priority(
        Available, peer_load(PeerPlan), peer_concurrency_cap(PeerPlan)
    ).

peer_priority(Available, Load, ConcurrencyCap) ->
    AvailabilityRank =
        case Available of
            true -> 0;
            false -> 1
        end,
    {AvailabilityRank, Load, -ConcurrencyCap}.

%% @doc Compare a candidate's peer priority with an incumbent's. The checks
%% run in order, and the first that decides wins:
%% - only one peer is available: better or worse;
%% - one peer's load is more than 10% below the other's: better or worse;
%% - the candidate's cap is more than 10% above the incumbent's: better;
%% - otherwise close, and the incumbent keeps its footprint.
compare_priorities({0, _, _}, {1, _, _}) ->
    better;
compare_priorities({1, _, _}, {0, _, _}) ->
    worse;
compare_priorities({_, CandidateLoad, NegativeCandidateCap},
        {_, IncumbentLoad, NegativeIncumbentCap}) ->
    case compare_load_with_margin(CandidateLoad, IncumbentLoad) of
        close ->
            case -NegativeCandidateCap > -NegativeIncumbentCap * 1.1 of
                true -> better;
                false -> close
            end;
        Result ->
            Result
    end.

%% The incumbent keeps its footprint while its peer's load is within ten
%% percent of the candidate's, which avoids churn between footprints that are
%% otherwise equivalent.
compare_load_with_margin(Candidate, Incumbent)
        when Incumbent > Candidate * 1.1 ->
    better;
compare_load_with_margin(Candidate, Incumbent)
        when Candidate > Incumbent * 1.1 ->
    worse;
compare_load_with_margin(_Candidate, _Incumbent) ->
    close.

%% @doc Return the source whose peer is least loaded.
best_source(_StoreID, [], _Plan) ->
    none;
best_source(StoreID, Sources, Plan) ->
    Queue = lists:foldl(
        fun(#task_source{ peer = Peer } = Source, Acc) ->
            Priority = #source_priority{
                peer_load = load(Peer, Plan),
                store_load = store_load(Peer, StoreID, Plan),
                peer = Peer
            },
            gb_sets:add_element({Priority, Source}, Acc)
        end,
        gb_sets:new(),
        Sources),
    {_Priority, Source} = gb_sets:smallest(Queue),
    {ok, Source}.

%%%===================================================================
%%% Peer queues.
%%%===================================================================

%% @doc Return the tasks in every peer queue.
queued_tasks(#state{ peers = Peers }) ->
    maps:fold(
        fun(_Peer, #peer{ queue = Queue }, Acc) ->
            queue:to_list(Queue) ++ Acc
        end,
        [],
        Peers).

%% @doc Return the length of every non-empty peer queue.
queue_lengths(#state{ peers = Peers }) ->
    maps:filtermap(
        fun(_Peer, #peer{ queue = Queue }) ->
            case queue:is_empty(Queue) of
                true -> false;
                false -> {true, queue:len(Queue)}
            end
        end,
        Peers).

%% @doc Return whether any peer queue holds a task.
has_queued_tasks(#state{ peers = Peers }) ->
    lists:any(fun(#peer{ queue = Queue }) -> not queue:is_empty(Queue) end,
        maps:values(Peers)).

%%%===================================================================
%%% Fetch results.
%%%===================================================================

%% @doc Add one completed fetch attempt to the peer's fetch results.
add_fetch_result(Peer, BytesFetched, FetchTiming,
        #state{ peers = Peers } = State) ->
    PeerState = maps:get(Peer, Peers, #peer{}),
    PeerState2 = PeerState#peer{
        fetched_bytes = PeerState#peer.fetched_bytes + BytesFetched,
        fetch_timing = merge_fetch_timing(
            PeerState#peer.fetch_timing, FetchTiming)
    },
    State#state{ peers = maps:put(Peer, PeerState2, Peers) }.

merge_fetch_timing(#fetch_timing{} = A, #fetch_timing{} = B) ->
    #fetch_timing{
        productive_ms = A#fetch_timing.productive_ms
            + B#fetch_timing.productive_ms,
        reject_ms = A#fetch_timing.reject_ms + B#fetch_timing.reject_ms,
        timeout_ms = A#fetch_timing.timeout_ms + B#fetch_timing.timeout_ms,
        client_error_ms = A#fetch_timing.client_error_ms
            + B#fetch_timing.client_error_ms
    }.

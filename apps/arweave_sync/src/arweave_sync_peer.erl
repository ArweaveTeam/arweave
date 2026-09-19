%%% @doc The sync pipeline's per-peer control plane: measures each active
%%% peer's realized delivery, accounts the worker time consumed by fetch
%%% outcomes, and sizes its concurrency cap. arweave_sync_scheduler reports fetch
%%% results and identifies peers whose active cap is fully driven on each
%%% scheduler tick. Throughout this module "cap" means how many fetch workers a
%%% peer may run at once.
%%%
%%% The cap rule:
%%%
%%% Each peer's cap is LOCAL — grown and cut from that peer's own observed
%%% behavior, never allocated from a shared pool. This mirrors BitTorrent's
%%% download side, which sizes each peer's request pipeline from that peer's
%%% own delivery and has no global proportional share-out (there is no
%%% global concurrency allocation at all: the download-rate budget and the
%%% chunk cache are the only shared bounds).
%%% The predecessor design water-filled the worker pool across peers by
%%% goodput; because those shares were zero-sum, high-delivery peers hoarded
%%% cap budget they were not using while willing peers sat pinned at small
%%% caps and most of the physical pool ran idle.
%%%
%%% Each tick evolves two independent limits:
%%%
%%% - concurrency_cap: while queued work waits behind a full active cap, a
%%%   productive peer adds eight requests per scheduler observation. A goodput
%%%   improvement confirms the current cap. Four observations with no gain
%%%   discard the probe; a shallow gain or recovery from an initial dip may
%%%   accumulate for up to eight observations before the unconfirmed increases
%%%   are discarded.
%%%   Independently, failures cut the cap in proportion to the share of worker
%%%   time consumed by 429s, timeouts, and client errors.
%%%   Weighting by occupied worker time prevents a fast rejection from counting
%%%   like a long successful request while still reacting strongly to slow
%%%   failures that monopolize concurrency.
%%% - queue_max_length: at most four seconds of the peer's measured delivery may
%%%   wait for a fetch slot. It bounds speculative peer-bound work without
%%%   limiting active fetch concurrency.
%%% - ?CONCURRENCY_CAP_INITIAL seed: unmeasured peers start with a modest
%%%   exploration pipeline. A collapsed peer may be cut to the one-worker
%%%   minimum and re-grows once it delivers again.
%%%
%%% State memory is retained for peers that leave the active-peer set (the map is
%%% bounded by distinct peers seen, a few hundred), so a peer that flaps out
%%% and back does not have to rediscover its budget through another storm.
%%%
%%% ↔ Prior art. The peer-local request pipeline and rate-sized waiting queue
%%% follow the same principles as BitTorrent request pipelines. Arweave also
%%% cuts active concurrency according to worker time consumed by 429s, timeouts,
%%% and client errors because sources can reject work and the local HTTP layer
%%% can shed requests under connection overload.
-module(arweave_sync_peer).

-ifdef(AR_TEST).
%% Focused tests live outside the production module.
-export([
    aggregate_delivery_entry/4,
    aggregate_goodput/1,
    bound_aggregate_goodput/2,
    evolve_cap_control/5,
    failure_pressure/1,
    goodput_improved/2,
    goodput_rate/1,
    peer_dispatch/2,
    queue_has_capacity/2,
    queue_max_length/1,
    recompute/5,
    update_delivery/5
]).
-endif.

-export([create_ets/0, reset_rows/0, new/0, record_result/4, tick/5,
    start_dispatch/3, enqueue_tasks/3, start_task/3,
        set_store_task_targets/2, set_store_task_targets/3,
        concurrency_cap/2,
        load/2, fetching_count/2, store_load/3, store_capacity/3,
        has_capacity/2, has_capacity/3,
        priority/3,
        best_source/3]).
-export_type([state/0, dispatch/0]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_sync/include/arweave_sync.hrl").

-ifdef(AR_TEST).
-export([test_dispatch/1, test_dispatch/2]).
-endif.

-include("arweave_sync_peer.hrl").

%% There is deliberately no absolute cap: the download-rate limit and chunk
%% cache remain the global bounds, while peer failures and unproductive probes
%% push an individual cap down.

-opaque state() :: #state{}.

-opaque dispatch() :: #dispatch{}.

-ifdef(AR_TEST).
%% Sim introspection: publication stashes cap memory for scenario debugging.
stash_cap_memory(State) ->
    catch ets:insert(?MODULE, {cap_memory, State#state.cap_memory}),
    ok.
-else.
stash_cap_memory(_State) ->
    ok.
-endif.

%%%===================================================================
%%% Setup and state.
%%%===================================================================

%% @doc Create the published-cap table (idempotent).
create_ets() ->
    catch ets:new(?MODULE, [named_table, public, set,
        {read_concurrency, true}]),
    ok.

%% @doc Clear the published rows (a freshly booted control plane).
reset_rows() ->
    catch ets:delete(?MODULE, cap_memory),
    ok.

%% @doc Fresh peer state with no peers tracked.
new() ->
    #state{}.

%% @doc Snapshot peer limits and restore fetching and queued assignment load
%% for one scheduler dispatch.
start_dispatch(Tasks, QueuedTasks, #state{
        caps = ConcurrencyCaps,
        queue_max_lengths = QueueMaxLengths
    }) ->
    Dispatches = record_tasks(
        Tasks, new_dispatch(ConcurrencyCaps, QueueMaxLengths)),
    record_queued_tasks(QueuedTasks, Dispatches).

record_tasks(Tasks, Dispatches) ->
    maps:fold(
        fun(_TaskRef, #task{ state = fetching, peer = Peer } = Task, Acc) ->
                record_fetching_assignment(Peer, Task, Acc);
            (_TaskRef, _Task, Acc) ->
                Acc
        end,
        Dispatches,
        Tasks).

record_queued_tasks(Tasks, Dispatches) ->
    lists:foldl(
        fun(#task{ peer = Peer } = Task, Acc) when Peer =/= undefined ->
                record_assignment(Peer, Task, Acc);
            (_Task, Acc) ->
                Acc
        end,
        Dispatches,
        Tasks).

new_dispatch(ConcurrencyCaps, QueueMaxLengths) ->
    Peers = maps:map(
        fun(Peer, ConcurrencyCap) ->
            #peer_dispatch{
                concurrency_cap = max(
                    ?CONCURRENCY_CAP_MIN, ConcurrencyCap),
                queue_max_length = maps:get(
                    Peer, QueueMaxLengths, ?CONCURRENCY_CAP_INITIAL)
            }
        end,
        ConcurrencyCaps),
    #dispatch{ peers = Peers }.

-ifdef(AR_TEST).
%% @doc Build a dispatch with explicit caps for focused scheduler tests.
test_dispatch(ConcurrencyCaps) ->
    QueueMaxLengths = maps:map(
        fun(_Peer, _ConcurrencyCap) -> ?CONCURRENCY_CAP_INITIAL end,
        ConcurrencyCaps),
    test_dispatch(ConcurrencyCaps, QueueMaxLengths).

%% @doc Build a dispatch with explicit active and queued limits.
test_dispatch(ConcurrencyCaps, QueueMaxLengths) ->
    new_dispatch(ConcurrencyCaps, QueueMaxLengths).
-endif.

%% @doc Account newly materialized footprint tasks against the peer queue.
enqueue_tasks(Peer, Tasks, Dispatches) ->
    lists:foldl(
        fun(Task, Acc) -> record_assignment(Peer, Task, Acc) end,
        Dispatches,
        Tasks).

%% @doc Start one task already accounted in the peer's runnable queue.
start_task(Peer, #task{}, Dispatches) ->
    record_fetch(Peer, Dispatches).

record_fetching_assignment(Peer, Task, Dispatches) ->
    record_fetch(Peer, record_assignment(Peer, Task, Dispatches)).

record_fetch(Peer, Dispatches) ->
    PeerDispatch = peer_dispatch(Peer, Dispatches),
    PeerDispatch2 = PeerDispatch#peer_dispatch{
        fetching_count = PeerDispatch#peer_dispatch.fetching_count + 1
    },
    put_peer_dispatch(Peer, PeerDispatch2, Dispatches).

record_assignment(Peer, Task, Dispatches) ->
    PeerDispatch = peer_dispatch(Peer, Dispatches),
    PeerDispatch2 = do_record_assignment(Task, PeerDispatch#peer_dispatch{
        assigned_task_count = PeerDispatch#peer_dispatch.assigned_task_count + 1
    }),
    put_peer_dispatch(Peer, PeerDispatch2, Dispatches).

do_record_assignment(#task{ store_id = StoreID },
        #peer_dispatch{ stores = Stores } = PeerDispatch) ->
    StoreLoad = maps:get(StoreID, Stores, #store_load{}),
    StoreLoad2 = StoreLoad#store_load{
        assigned_task_count = StoreLoad#store_load.assigned_task_count + 1
    },
    PeerDispatch#peer_dispatch{ stores = maps:put(StoreID, StoreLoad2, Stores) }.

%% @doc Set local destination-store task targets for every peer with work.
set_store_task_targets(StoresByPeer, #dispatch{ peers = Peers } = Dispatches) ->
    Dispatches#dispatch{ peers = maps:map(
        fun(Peer, PeerDispatch) ->
            do_set_store_task_targets(
                maps:get(Peer, StoresByPeer, []), PeerDispatch)
        end,
        Peers) }.

%% @doc Split one peer's assignment limit across the supplied stores.
set_store_task_targets(Peer, StoreIDs, Dispatches) ->
    PeerDispatch = peer_dispatch(Peer, Dispatches),
    PeerDispatch2 = do_set_store_task_targets(StoreIDs, PeerDispatch),
    put_peer_dispatch(Peer, PeerDispatch2, Dispatches).

do_set_store_task_targets(StoreIDs, PeerDispatch) ->
    AssignedStoreIDs = assigned_store_ids(PeerDispatch),
    set_store_task_targets_for_stores(
        lists:usort(StoreIDs ++ AssignedStoreIDs), PeerDispatch).

assigned_store_ids(#peer_dispatch{ stores = Stores }) ->
    maps:fold(
        fun(StoreID, #store_load{ assigned_task_count = Count }, Acc)
                when Count > 0 ->
                [StoreID | Acc];
            (_StoreID, _StoreLoad, Acc) ->
                Acc
        end,
        [],
        Stores).

set_store_task_targets_for_stores([], PeerDispatch) ->
    clear_store_task_targets(PeerDispatch);
set_store_task_targets_for_stores(StoreIDs, PeerDispatch) ->
    AssignmentLimit = peer_assignment_limit(PeerDispatch),
    Target = erlang:ceil(AssignmentLimit / length(StoreIDs)),
    PeerDispatch2 = clear_store_task_targets(PeerDispatch),
    lists:foldl(
        fun(StoreID, Acc) -> set_store_task_target(StoreID, Target, Acc) end,
        PeerDispatch2,
        StoreIDs).

clear_store_task_targets(#peer_dispatch{ stores = Stores } = PeerDispatch) ->
    PeerDispatch#peer_dispatch{ stores = maps:map(
        fun(_StoreID, StoreLoad) ->
            StoreLoad#store_load{ target_task_count = 0 }
        end,
        Stores) }.

set_store_task_target(StoreID, Target,
        #peer_dispatch{ stores = Stores } = PeerDispatch) ->
    StoreLoad = maps:get(StoreID, Stores, #store_load{}),
    PeerDispatch#peer_dispatch{ stores = maps:put(StoreID,
        StoreLoad#store_load{ target_task_count = Target }, Stores) }.

%% @doc Return this peer's concurrency cap.
concurrency_cap(Peer, Dispatches) ->
    peer_concurrency_cap(peer_dispatch(Peer, Dispatches)).

peer_concurrency_cap(#peer_dispatch{ concurrency_cap = ConcurrencyCap }) ->
    ConcurrencyCap.

%% @doc Return the fraction of this peer's assignment limit in use.
load(Peer, Dispatches) ->
    peer_load(peer_dispatch(Peer, Dispatches)).

peer_load(#peer_dispatch{ assigned_task_count = AssignedTaskCount } =
        PeerDispatch) ->
    case peer_assignment_limit(PeerDispatch) of
        Limit when Limit > 0 -> AssignedTaskCount / Limit;
        _ -> 1.0
    end.

%% @doc Return this peer's active fetch count during the dispatch.
fetching_count(Peer, Dispatches) ->
    (peer_dispatch(Peer, Dispatches))#peer_dispatch.fetching_count.

%% @doc Return the fraction of this peer's store target occupied by peer-bound
%% queued or fetching tasks.
store_load(Peer, StoreID, Dispatches) ->
    StoreLoad = store_load(StoreID, peer_dispatch(Peer, Dispatches)),
    case StoreLoad#store_load.target_task_count of
        Target when Target > 0 ->
            StoreLoad#store_load.assigned_task_count / Target;
        _ ->
            1.0
    end.

%% @doc Number of additional tasks allowed by this peer/store queue target. The
%% peer's global fetch concurrency is enforced separately by has_capacity/2.
store_capacity(Peer, StoreID, Dispatches) ->
    store_capacity_for_dispatch(
        StoreID, peer_dispatch(Peer, Dispatches)).

%% @doc Return whether a peer or source may start another fetch.
has_capacity(#task_source{ peer = Peer }, Dispatches) ->
    has_capacity(Peer, Dispatches);
has_capacity(Peer, Dispatches) ->
    has_capacity(peer_dispatch(Peer, Dispatches)).

has_capacity(#peer_dispatch{ fetching_count = FetchingCount,
        concurrency_cap = ConcurrencyCap }) ->
    FetchingCount < ConcurrencyCap.

%% @doc Return whether Item may consume another peer-queue assignment.
has_capacity(#task{ store_id = StoreID },
        #task_source{ peer = Peer }, Dispatches) ->
    queue_has_capacity(StoreID, peer_dispatch(Peer, Dispatches));
has_capacity(#footprint_reservation{ store_id = StoreID },
        #task_source{ peer = Peer }, Dispatches) ->
    queue_has_capacity(StoreID, peer_dispatch(Peer, Dispatches)).

queue_has_capacity(StoreID, PeerDispatch) ->
    peer_queue_capacity(PeerDispatch) > 0
        andalso assignment_has_capacity(StoreID, PeerDispatch).

assignment_has_capacity(StoreID, PeerDispatch) ->
    peer_assignment_capacity(PeerDispatch) > 0
        andalso store_assignment_capacity(StoreID, PeerDispatch) > 0.

store_capacity_for_dispatch(StoreID, PeerDispatch) ->
    min(peer_queue_capacity(PeerDispatch),
        store_assignment_capacity(StoreID, PeerDispatch)).

store_assignment_capacity(StoreID, PeerDispatch) ->
    case store_is_explored(StoreID, PeerDispatch)
            orelse store_exploration_available(PeerDispatch) of
        true -> min(peer_assignment_capacity(PeerDispatch),
            store_queue_capacity_for_dispatch(StoreID, PeerDispatch));
        false -> 0
    end.

store_queue_capacity_for_dispatch(StoreID, PeerDispatch) ->
    StoreLoad = store_load(StoreID, PeerDispatch),
    max(0, store_target(StoreLoad, PeerDispatch)
        - StoreLoad#store_load.assigned_task_count).

store_target(#store_load{ target_task_count = 0 }, PeerDispatch) ->
    peer_assignment_limit(PeerDispatch);
store_target(#store_load{ target_task_count = Target }, _PeerDispatch) ->
    Target.

store_is_explored(StoreID, #peer_dispatch{ stores = Stores }) ->
    (maps:get(StoreID, Stores, #store_load{}))#store_load.assigned_task_count > 0.

%% @doc Bound speculative store breadth while letting a fast peer reveal
%% independent source-store capacity before its measured cap has grown.
store_exploration_available(#peer_dispatch{
        concurrency_cap = ConcurrencyCap,
        stores = Stores }) ->
    ExploredStoreCount = maps:fold(
        fun(_ID, #store_load{ assigned_task_count = Count }, Acc)
                when Count > 0 ->
                Acc + 1;
            (_ID, #store_load{}, Acc) ->
                Acc
        end,
        0,
        Stores),
    ExploredStoreCount < max(?CONCURRENCY_CAP_INITIAL, ConcurrencyCap).

peer_assignment_capacity(#peer_dispatch{
        assigned_task_count = AssignedTaskCount } = PeerDispatch) ->
    max(0, peer_assignment_limit(PeerDispatch) - AssignedTaskCount).

peer_assignment_limit(#peer_dispatch{
        concurrency_cap = ConcurrencyCap,
        queue_max_length = QueueMaxLength
    }) ->
    ConcurrencyCap + QueueMaxLength.

peer_queue_capacity(#peer_dispatch{
        fetching_count = FetchingCount,
        assigned_task_count = AssignedTaskCount,
        queue_max_length = QueueMaxLength
    }) ->
    QueuedTaskCount = AssignedTaskCount - FetchingCount,
    max(0, QueueMaxLength - QueuedTaskCount).

%% @doc Rank a peer. Runnable peers precede blocked peers, then lower load and
%% higher capacity win.
priority(Peer, Runnable, Dispatches) ->
    AvailabilityRank = case Runnable of
        true -> 0;
        false -> 1
    end,
    {AvailabilityRank, load(Peer, Dispatches),
        -concurrency_cap(Peer, Dispatches)}.

%% @doc Select the source offered by the least-loaded eligible peer.
best_source(_StoreID, [], _Dispatches) ->
    none;
best_source(StoreID, Sources, Dispatches) ->
    Queue = lists:foldl(
        fun(#task_source{ peer = Peer } = Source, Acc) ->
            Priority = #source_priority{
                peer_load = load(Peer, Dispatches),
                store_load = store_load(Peer, StoreID, Dispatches),
                peer = Peer
            },
            gb_sets:add_element({Priority, Source}, Acc)
        end,
        gb_sets:new(),
        Sources),
    {_Priority, Source} = gb_sets:smallest(Queue),
    {ok, Source}.

peer_dispatch(Peer, #dispatch{ peers = Peers }) ->
    maps:get(Peer, Peers, #peer_dispatch{}).

put_peer_dispatch(Peer, PeerDispatch,
        #dispatch{ peers = Peers } = Dispatches) ->
    Dispatches#dispatch{ peers = maps:put(Peer, PeerDispatch, Peers) }.

store_load(StoreID, #peer_dispatch{ stores = Stores }) ->
    maps:get(StoreID, Stores, #store_load{}).

%% @doc Add one completed fetch attempt to the scheduler's peer observations.
record_result(Peer, DeliveredBytes, FetchTiming, State) ->
    #state{ observations = Observations } = State,
    Observation = maps:get(Peer, Observations, #observation{}),
    Observation2 = Observation#observation{
        total_bytes = Observation#observation.total_bytes + DeliveredBytes,
        fetch_timing = merge_fetch_timing(
            Observation#observation.fetch_timing, FetchTiming)
    },
    State#state{
        observations = maps:put(Peer, Observation2, Observations)
    }.

merge_fetch_timing(#fetch_timing{} = A, #fetch_timing{} = B) ->
    #fetch_timing{
        productive_ms = A#fetch_timing.productive_ms
            + B#fetch_timing.productive_ms,
        reject_ms = A#fetch_timing.reject_ms + B#fetch_timing.reject_ms,
        timeout_ms = A#fetch_timing.timeout_ms + B#fetch_timing.timeout_ms,
        client_error_ms = A#fetch_timing.client_error_ms
            + B#fetch_timing.client_error_ms
    }.

reset_fetch_timings(Observations) ->
    maps:map(
        fun(_Peer, Observation) ->
            Observation#observation{ fetch_timing = #fetch_timing{} }
        end,
        Observations).

%%%===================================================================
%%% The tick: measure, evolve, remember.
%%%===================================================================

%% @doc One control tick over the active-peer set: sample realized goodput,
%% evolve each cap, and size its peer queue.
%% InflightCounts distinguishes an idle peer from one that delivered nothing
%% while requests were running.
%% DrivenPeers contains peers whose runnable queue remained behind a full cap
%% while shared scheduler gates were open.
tick(Peers, InflightCounts, DrivenPeers, NowMs, State) ->
    #state{
        delivery = Delivery,
        aggregate_delivery = AggregateDelivery,
        cap_memory = Memory,
        observations = Observations
    } = State,
    Perfs = maps:from_list([
        {Peer, (maps:get(Peer, Observations, #observation{}))#observation.total_bytes}
        || Peer <- Peers
    ]),
    FetchTimings = maps:from_list([
        {Peer, (maps:get(Peer, Observations, #observation{}))#observation.fetch_timing}
        || Peer <- Peers
    ]),
    Delivery2 = update_delivery(Peers, Perfs, Delivery, InflightCounts, NowMs),
    IsDriven = fun(Peer) ->
        maps:is_key(Peer, DrivenPeers)
    end,
    AggregateDelivery2 = update_aggregate_delivery(
        Peers, Perfs, InflightCounts, NowMs, AggregateDelivery),
    Goodput = bound_aggregate_goodput(
        delivery_goodput(Delivery2), aggregate_goodput(AggregateDelivery2)),
    {Caps2, QueueMaxLengths2, Memory2} = recompute(Peers, Goodput, FetchTimings,
        IsDriven, Memory),
    State2 = State#state{
        cap_memory = Memory2,
        caps = Caps2,
        queue_max_lengths = QueueMaxLengths2,
        delivery = Delivery2, aggregate_delivery = AggregateDelivery2,
        observations = reset_fetch_timings(Observations) },
    publish(State2),
    State2.

%%%===================================================================
%%% Publication.
%%%===================================================================

publish(#state{ caps = Caps } = State) ->
    stash_cap_memory(State),
    TotalCap = lists:sum(maps:values(Caps)),
    arweave_metrics:gauge_set(sync_total_concurrency_cap, TotalCap),
    Labels = [arweave_util:format_peer(Peer) || Peer <- maps:keys(Caps)],
    prune_stale_peer_metrics(Labels),
    ok.

prune_stale_peer_metrics(CurrentLabels) ->
    lists:foreach(
        fun(Name) -> prune_stale_labels(Name, CurrentLabels) end,
        [sync_peer_concurrency_cap,
            sync_peer_goodput_bytes_per_second,
            sync_peer_queue_max_length,
            sync_peer_failure_pressure,
            sync_peer_driven]).

prune_stale_labels(Name, CurrentLabels) ->
    ExistingLabels = [Value
        || {[{_LabelName, Value}], _MetricValue} <-
            arweave_metrics:gauge_values(Name)],
    lists:foreach(
        fun(Label) -> arweave_metrics:gauge_remove(Name, [Label]) end,
        ExistingLabels -- CurrentLabels).

%%%===================================================================
%%% Goodput measurement.
%%%===================================================================

update_delivery(Peers, Perfs, Delivery, InflightCounts, Now) ->
    maps:from_list([
        {Peer, delivery_entry(Peer, Perfs, Delivery,
            maps:get(Peer, InflightCounts, 0), Now)}
        || Peer <- Peers
    ]).

delivery_entry(Peer, Perfs, Delivery, Inflight, Now) ->
    TotalBytes = peer_total_bytes(Peer, Perfs),
    case maps:get(Peer, Delivery, undefined) of
        {PrevBytes, PrevTime, PrevRate}
                when Now > PrevTime andalso TotalBytes >= PrevBytes ->
            case TotalBytes == PrevBytes andalso Inflight == 0 of
                true ->
                    %% Unfed tick — nothing delivered AND nothing in flight: a
                    %% MISSING sample, not a zero one. Folding zeros for unfed
                    %% ticks reflects our own dispatch neglect back as "low
                    %% capacity": the estimate decays, the queue limit
                    %% contracts, and the scheduler assigns even less work.
                    %% Advance the snapshot and keep the estimate.
                    {TotalBytes, Now, PrevRate};
                false ->
                    %% Driven-but-zero still samples: with work in flight, no
                    %% delivery is real signal (the latency blind spot).
                    ElapsedMs = Now - PrevTime,
                    Sample = (TotalBytes - PrevBytes) / ElapsedMs,
                    Rate = case PrevRate of
                        undefined -> Sample;
                        _ when Sample < PrevRate ->
                            arweave_util:ema(
                                PrevRate, Sample, ?GOODPUT_DECREASE_ALPHA);
                        _ ->
                            arweave_util:ema(PrevRate, Sample, ?GOODPUT_ALPHA)
                    end,
                    {TotalBytes, Now,
                        snap_to_noise_floor(Rate, ElapsedMs)}
            end;
        _ ->
            %% First sighting, or the counter went backwards (peer record reset): reseed
            %% without crediting historical bytes as recent delivery.
            {TotalBytes, Now, undefined}
    end.

%% @doc Goodput below one chunk per observation interval is beneath the
%% measurement's resolution — snap it to 0.0 ("unmeasured") so the peer
%% returns to the exploration cap. The EWMA otherwise decays asymptotically
%% and a starved peer's near-zero estimate would retain a near-minimum cap
%% indefinitely. The threshold is the instrument's own quantum, not a tuned
%% constant.
snap_to_noise_floor(Rate, ElapsedMs) ->
    case Rate < ?DATA_CHUNK_SIZE / ElapsedMs of
        true -> 0.0;
        false -> Rate
    end.

peer_total_bytes(Peer, Perfs) ->
    float(maps:get(Peer, Perfs, 0)).

goodput_rate(undefined) -> 0.0;
goodput_rate(Rate) -> Rate.

%% @doc The per-peer goodput map (bytes/ms actually delivered) derived from
%% the delivery-fold state; entries without a measured rate map to 0.0.
delivery_goodput(Delivery) ->
    maps:map(fun(_P, {_B, _T, R}) -> goodput_rate(R) end, Delivery).

update_aggregate_delivery(Peers, Perfs, InflightCounts, Now,
        AggregateDelivery) ->
    ActivePeers = lists:sort(Peers),
    TotalBytes = lists:sum([maps:get(Peer, Perfs, 0) || Peer <- Peers]),
    TotalInflight = lists:sum([
        maps:get(Peer, InflightCounts, 0) || Peer <- Peers
    ]),
    Previous = case AggregateDelivery of
        {ActivePeers, Entry} -> Entry;
        _ -> undefined
    end,
    Entry2 = aggregate_delivery_entry(
        TotalBytes, TotalInflight, Now, Previous),
    {ActivePeers, Entry2}.

aggregate_delivery_entry(TotalBytes, TotalInflight, Now,
        {PrevBytes, PrevTime, PrevRate})
        when Now > PrevTime andalso TotalBytes >= PrevBytes ->
    case TotalBytes == PrevBytes andalso TotalInflight == 0 of
        true ->
            {TotalBytes, Now, PrevRate};
        false ->
            ElapsedMs = Now - PrevTime,
            Sample = (TotalBytes - PrevBytes) / ElapsedMs,
            Rate = case PrevRate of
                undefined -> Sample;
                _ -> arweave_util:ema(
                    PrevRate, Sample, ?AGGREGATE_GOODPUT_ALPHA)
            end,
            {TotalBytes, Now, snap_to_noise_floor(Rate, ElapsedMs)}
    end;
aggregate_delivery_entry(TotalBytes, _TotalInflight, Now, _Previous) ->
    {float(TotalBytes), Now, undefined}.

aggregate_goodput({[_SinglePeer], _Entry}) ->
    undefined;
aggregate_goodput({_ActivePeers, {_Bytes, _Time, Rate}}) ->
    Rate;
aggregate_goodput(_AggregateDelivery) ->
    undefined.

bound_aggregate_goodput(Goodput, undefined) ->
    Goodput;
bound_aggregate_goodput(Goodput, AggregateRate) ->
    TotalRate = lists:sum(maps:values(Goodput)),
    case TotalRate > AggregateRate andalso TotalRate > 0.0 of
        true -> maps:map(
            fun(_Peer, Rate) -> Rate * AggregateRate / TotalRate end,
            Goodput);
        false -> Goodput
    end.

%%%===================================================================
%%% Derived cap.
%%%===================================================================

%% @doc Evolve active fetch caps from worker outcomes and driven demand, and
%% derive peer queue limits from measured delivery.
recompute(Peers, Goodput, FetchTimings, IsDriven, State) ->
    lists:foldl(
        fun(Peer, {CapsAcc, QueueMaxLengthsAcc, StateAcc}) ->
            Control = maps:get(Peer, StateAcc, #cap_control{}),
            PrevCap = Control#cap_control.cap,
            Rate = maps:get(Peer, Goodput, 0.0),
            FetchTiming = maps:get(Peer, FetchTimings, #fetch_timing{}),
            {Pressure, FailureMs} = failure_pressure(FetchTiming),
            ProductiveMs = FetchTiming#fetch_timing.productive_ms,
            QueueMaxLength = queue_max_length(Rate),
            Driven = IsDriven(Peer),
            Control2 = evolve_cap_control(
                Rate, ProductiveMs, Pressure, Driven, Control),
            Cap = Control2#cap_control.cap,
            log_cap_decision(Peer, PrevCap, Cap, Rate,
                QueueMaxLength, Pressure, ProductiveMs, FailureMs,
                Driven, FetchTiming),
            {maps:put(Peer, Cap, CapsAcc),
                maps:put(Peer, QueueMaxLength, QueueMaxLengthsAcc),
                maps:put(Peer, Control2, StateAcc)}
        end,
        {#{}, #{}, State},
        Peers).

%% @doc Record every input and limit used for one peer's scheduler decision.
log_cap_decision(Peer, PrevCap, Cap, Rate, QueueMaxLength, Pressure,
        ProductiveMs, FailureMs, Driven, FetchTiming) ->
    Label = arweave_util:format_peer(Peer),
    arweave_metrics:gauge_set(sync_peer_concurrency_cap, [Label], Cap),
    arweave_metrics:gauge_set(sync_peer_goodput_bytes_per_second, [Label],
        Rate * 1000),
    arweave_metrics:gauge_set(
        sync_peer_queue_max_length, [Label], QueueMaxLength),
    arweave_metrics:gauge_set(sync_peer_failure_pressure, [Label], Pressure),
    arweave_metrics:gauge_set(sync_peer_driven, [Label], boolean_value(Driven)),
    ?LOG_DEBUG([{event, sync_peer_cap_decision},
        {peer, Label},
        {previous_cap, PrevCap},
        {cap, Cap},
        {goodput_mib_per_second, Rate * 1000 / ?MiB},
        {queue_max_length, QueueMaxLength},
        {failure_pressure, Pressure},
        {productive_worker_ms, ProductiveMs},
        {failure_worker_ms, FailureMs},
        {reject_worker_ms, FetchTiming#fetch_timing.reject_ms},
        {timeout_worker_ms, FetchTiming#fetch_timing.timeout_ms},
        {client_error_worker_ms, FetchTiming#fetch_timing.client_error_ms},
        {driven, Driven}]).

failure_pressure(FetchTiming) ->
    #fetch_timing{
        productive_ms = ProductiveMs,
        reject_ms = RejectMs,
        timeout_ms = TimeoutMs,
        client_error_ms = ClientErrorMs
    } = FetchTiming,
    FailureMs = RejectMs + TimeoutMs + ClientErrorMs,
    TotalMs = ProductiveMs + FailureMs,
    Pressure = case TotalMs of
        0 -> 0.0;
        _ -> FailureMs / TotalMs
    end,
    {Pressure, FailureMs}.

%% @doc Apply immediate failure backpressure, then advance a goodput probe only
%% while the peer is productive and has runnable work behind its active cap.
evolve_cap_control(Rate, ProductiveMs, Pressure, Driven, Control) ->
    Cut = min(?MAX_TICK_CUT, ?FAILURE_CUT_GAIN * Pressure),
    case Pressure > 0.0 of
        true ->
            %% Round the reduction down so a fractional request does not turn
            %% occasional errors into repeated whole-request cuts at small caps.
            Cap = max(?CONCURRENCY_CAP_MIN,
                ceil(Control#cap_control.cap * (1.0 - Cut))),
            Control#cap_control{ cap = Cap, phase = cooldown,
                baseline_rate = undefined, baseline_cap = undefined,
                observations = 0 };
        false when ProductiveMs > 0, Driven ->
            advance_goodput_probe(Rate, Control);
        false ->
            Control
    end.

advance_goodput_probe(Rate,
        #cap_control{ phase = establish } = Control) when Rate > 0.0 ->
    begin_goodput_probe(Rate, Control);
advance_goodput_probe(_Rate,
        #cap_control{ phase = establish } = Control) ->
    Control;
advance_goodput_probe(Rate,
        #cap_control{ phase = probe } = Control) ->
    continue_goodput_probe(Rate, Control);
advance_goodput_probe(Rate,
        #cap_control{ phase = settle, observations = Observations } = Control)
        when Rate > 0.0 ->
    Observations2 = Observations + 1,
    case Observations2 >= ?GOODPUT_SETTLE_OBSERVATIONS of
        true -> begin_goodput_probe(Rate, Control);
        false -> Control#cap_control{ observations = Observations2 }
    end;
advance_goodput_probe(_Rate,
        #cap_control{ phase = settle } = Control) ->
    Control#cap_control{ observations = 0 };
advance_goodput_probe(Rate,
        #cap_control{ phase = cooldown, observations = Observations } = Control)
        when Rate > 0.0 ->
    Observations2 = Observations + 1,
    case Observations2 >= ?GOODPUT_BASELINE_OBSERVATIONS of
        true -> begin_goodput_probe(Rate, Control);
        false -> Control#cap_control{ observations = Observations2 }
    end;
advance_goodput_probe(_Rate,
        #cap_control{ phase = cooldown } = Control) ->
    Control#cap_control{ observations = 0 }.

begin_goodput_probe(Rate, Control) ->
    Control#cap_control{
        cap = Control#cap_control.cap + ?CONCURRENCY_CAP_PROBE_STEP,
        phase = probe,
        baseline_rate = Rate,
        baseline_cap = Control#cap_control.cap,
        observations = 0
    }.

continue_goodput_probe(Rate,
        #cap_control{ baseline_rate = BaselineRate,
            observations = Observations } = Control) ->
    case goodput_improved(Rate, BaselineRate) of
        true -> begin_goodput_probe(Rate, Control);
        false ->
            Observations2 = Observations + 1,
            case Observations2 >= ?GOODPUT_FLAT_OBSERVATIONS of
                true -> back_off_goodput_probe(settle, Control);
                false -> Control#cap_control{
                    cap = Control#cap_control.cap
                        + ?CONCURRENCY_CAP_PROBE_STEP,
                    observations = Observations2
                }
            end
    end.

goodput_improved(Rate, BaselineRate) ->
    Rate > BaselineRate * (1.0 + ?GOODPUT_PROBE_MIN_GAIN).

back_off_goodput_probe(Phase,
        #cap_control{ baseline_cap = BaselineCap } = Control) ->
    Control#cap_control{
        cap = max(?CONCURRENCY_CAP_INITIAL, BaselineCap),
        phase = Phase,
        baseline_rate = undefined,
        baseline_cap = undefined,
        observations = 0
    }.

boolean_value(true) -> 1;
boolean_value(false) -> 0.

%% @doc Allow four seconds of measured delivery to wait behind active fetches.
queue_max_length(Rate) when Rate > 0.0 ->
    max(?CONCURRENCY_CAP_INITIAL, round(
        Rate * ?QUEUE_TARGET_DURATION_MS / ?DATA_CHUNK_SIZE));
queue_max_length(_Rate) ->
    ?CONCURRENCY_CAP_INITIAL.

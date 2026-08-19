%%% @doc The sync pipeline's per-peer control plane: measures each active
%%% peer's realized delivery, accounts the worker time consumed by fetch
%%% outcomes, and sizes its concurrency cap. ar_sync_scheduler reports fetch
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
-module(ar_sync_peer).
-test_category([fast]).

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
-include_lib("arweave/include/ar_sync.hrl").

-ifdef(AR_TEST).
-include_lib("eunit/include/eunit.hrl").
-export([test_dispatch/1, test_dispatch/2]).
-endif.

%% The minimum keeps one exploration fetch flowing (a cut can never
%% silence a peer entirely).
-define(CONCURRENCY_CAP_MIN, 1).

%% The exploration seed: what an unmeasured peer may run. It is modest enough
%% to be polite to an unknown peer and large enough to collect an initial
%% delivery sample.
-define(CONCURRENCY_CAP_INITIAL, 8).

%% Keep four seconds of measured delivery queued behind active fetches.
-define(QUEUE_TARGET_DURATION_MS, 4000).

%% Reuse the initial exploration window as each additive increase. This
%% advances cautiously by the same request batch considered safe for an
%% unknown peer.
-define(CONCURRENCY_CAP_PROBE_STEP, ?CONCURRENCY_CAP_INITIAL).

%% Two observations after a failure-pressure cut establish a fresh baseline
%% before the scheduler retries a probe.
-define(GOODPUT_BASELINE_OBSERVATIONS, 2).

%% Require a half-percent improvement. The goodput EWMA and multi-observation
%% rejection suppress one-tick noise while cumulative probes expose shallow
%% gains from a latency-loaded peer.
-define(GOODPUT_PROBE_MIN_GAIN, 0.005).

%% Requests launched before a cap increase may complete during several later
%% scheduler observations. Allow four observations for that delayed goodput
%% to appear, then discard the probe if goodput still has not improved.
-define(GOODPUT_FLAT_OBSERVATIONS, 4).

%% After a goodput-based backoff, hold the lower cap while requests launched at
%% the old cap drain. With the decreasing-rate EWMA's 0.25 weight, four
%% observations discard 1 - 0.75^4 ~= 68% of the old regime before retrying.
-define(GOODPUT_SETTLE_OBSERVATIONS, 4).

%% A full-failure tick cuts at most in half, so a burst cannot zero the cap
%% in one step and the walk-down to a sustainable depth remains geometric.
-define(MAX_TICK_CUT, 0.5).

%% Scale worker-time failure pressure before applying the cut. A one-third
%% failure share reaches the halving clamp; smaller shares leave room for the
%% next probe to find a stable operating depth.
-define(FAILURE_CUT_GAIN, 1.5).

%% There is deliberately no absolute cap: the download-rate limit and chunk
%% cache remain the global bounds, while peer failures and unproductive probes
%% push an individual cap down.

%% EWMA weight for a peer's realized goodput (bytes/ms it actually
%% delivered), sampled each tick from the delta of this module's cumulative
%% chunk-only delivered-byte observations. 0.5 = ~2-tick memory: responsive when
%% the frontier moves off a peer, not jumpy. Sampling is scoped to the
%% scheduler's active peers (peers with queued or nonterminal tasks), and
%% entries are dropped when a peer leaves that set.
-define(GOODPUT_ALPHA, 0.5).
%% Successful delivery is bursty at the control-tick boundary. Decreases use
%% a four-tick weight so one low phase does not collapse the pipeline; sustained
%% slow delivery still replaces the old estimate promptly.
-define(GOODPUT_DECREASE_ALPHA, 0.25).

%% Smooth the aggregate of multiple active peers across five observations.
%% This absorbs whole-second link-bucket boundaries without weakening each
%% peer's faster scheduler response.
-define(AGGREGATE_GOODPUT_ALPHA, 0.2).

-record(observation, {
    total_bytes = 0,
    fetch_timing = #fetch_timing{}
}).

%% Per-peer active-concurrency probe retained across active-set membership.
-record(cap_control, {
    cap = ?CONCURRENCY_CAP_INITIAL,
    phase = establish,
    baseline_rate = undefined,
    baseline_cap = undefined,
    observations = 0
}).

-record(state, {
    %% Peer => #cap_control{} retained across active-peer membership and
    %% bounded by distinct peers seen.
    cap_memory = #{},
    %% Peer => cap published at the last tick (covers active peers only).
    caps = #{},
    %% Peer => maximum peer-bound tasks waiting behind active fetches.
    queue_max_lengths = #{},
    %% Peer => {PrevTotalBytes, PrevTimeMs, GoodputEWMA | undefined}:
    %% snapshot of cumulative delivered bytes at the last active
    %% tick, used to derive realized goodput. Rebuilt from the active-peer set
    %% each tick, so peers that leave are evicted.
    delivery = #{},
    %% {ActivePeers, DeliveryEntry} for their aggregate delivered bytes. This
    %% bounds inflation when delivery moves between peers and their individual
    %% increase/decrease EWMA weights differ.
    aggregate_delivery = undefined,
    %% Peer => #observation{}: cumulative delivered bytes and fetch timing
    %% accumulated since the previous control tick.
    observations = #{}
}).

-opaque state() :: #state{}.

%% One local store's task load and target share of a peer during a dispatch.
-record(store_load, {
    assigned_task_count = 0,
    target_task_count = 0
}).

-record(peer_dispatch, {
    fetching_count = 0,
    assigned_task_count = 0,
    concurrency_cap = ?CONCURRENCY_CAP_MIN,
    queue_max_length = ?CONCURRENCY_CAP_INITIAL,
    stores = #{}
}).

%% Opaque index of every peer participating in one scheduler dispatch.
-record(dispatch, {
    peers = #{}
}).

-opaque dispatch() :: #dispatch{}.

%% Field order is dispatch precedence because records use Erlang term ordering.
-record(source_priority, {
    peer_load,
    store_load,
    peer
}).

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
            Cap = max(?CONCURRENCY_CAP_MIN,
                round(Control#cap_control.cap * (1.0 - Cut))),
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

%%%===================================================================
%%% Tests.
%%%===================================================================

-ifdef(AR_TEST).

dispatch_tracks_fetch_capacity_test() ->
    StoreID = store,
    %% Two concurrent fetches fill this peer's two-request cap while assignment
    %% load remains the two tasks already admitted to its runnable queue.
    Dispatches0 = test_dispatch(#{peer => 2}, #{peer => 2}),
    ?assertEqual(0.0, load(peer, Dispatches0)),
    Task = #task{ state = queued, store_id = StoreID },
    Dispatches1 = enqueue_tasks(peer, [Task], Dispatches0),
    %% The assignment limit combines two active and two queued tasks.
    ?assertEqual(1 / 4, load(peer, Dispatches1)),
    Dispatches2 = start_task(peer, Task, Dispatches1),
    ?assert(has_capacity(peer, Dispatches1)),
    Dispatches3 = enqueue_tasks(peer, [Task], Dispatches2),
    ?assertEqual(1 / 2, load(peer, Dispatches3)),
    Dispatches4 = start_task(peer, Task, Dispatches3),
    ?assertNot(has_capacity(peer, Dispatches4)).

queued_footprint_tasks_share_peer_queue_with_fetches_test() ->
    Peer = peer,
    StoreID = store,
    Footprint = #footprint{ store_id = StoreID },
    Source = #task_source{ peer = Peer },
    Task = #task{ state = queued, store_id = StoreID,
        footprint = Footprint, sources = [Source] },
    %% The two-task queue limit permits two queued replacements independently of the
    %% two active fetch slots.
    Dispatches0 = test_dispatch(#{Peer => 2}, #{Peer => 2}),
    Dispatches1 = enqueue_tasks(Peer, [Task, Task], Dispatches0),
    ?assertEqual(1 / 2, load(Peer, Dispatches1)),
    %% A new reservation cannot claim more peer queue capacity, but an already-queued
    %% task may consume a fetch slot without increasing assigned work.
    Reservation = #footprint_reservation{ store_id = StoreID },
    ?assertNot(has_capacity(Reservation, Source, Dispatches1)),
    ?assert(has_capacity(Peer, Dispatches1)),
    Dispatches2 = start_task(Peer, Task, Dispatches1),
    ?assertEqual(1 / 2, load(Peer, Dispatches2)),
    ?assert(has_capacity(Task, Source, Dispatches2)),
    Dispatches3 = start_task(Peer, Task, Dispatches2),
    ?assertNot(has_capacity(Peer, Dispatches3)).

start_dispatch_restores_queued_assignment_limit_test() ->
    Peer = peer,
    StoreID = store,
    Task = #task{ state = queued, peer = Peer, store_id = StoreID,
        footprint = #footprint{ store_id = StoreID } },
    %% One persistent child occupies half of the two-task queue limit.
    State = #state{
        caps = #{Peer => 2},
        queue_max_lengths = #{Peer => 2}
    },
    Dispatches = start_dispatch(#{}, [Task], State),
    ?assertEqual(1 / 4, load(Peer, Dispatches)),
    ?assertEqual(1, store_capacity(Peer, StoreID, Dispatches)).

queue_max_length_preserves_bounded_store_exploration_test() ->
    Peer = peer,
    Source = #task_source{ peer = Peer },
    %% The one-task queue limit permits one speculative store, not a second.
    Dispatches0 = test_dispatch(#{Peer => 1}, #{Peer => 1}),
    ?assert(queue_has_capacity(store_a, peer_dispatch(Peer, Dispatches0))),
    Task = #task{ store_id = store_a,
        footprint = #footprint{ store_id = store_a },
        sources = [Source] },
    Dispatches = enqueue_tasks(Peer, [Task], Dispatches0),
    ?assertNot(queue_has_capacity(
        store_a, peer_dispatch(Peer, Dispatches))),
    ?assertNot(queue_has_capacity(
        store_b, peer_dispatch(Peer, Dispatches))).

dispatch_splits_peer_capacity_across_stores_test() ->
    %% Four active and four queued tasks split into four assigned tasks per
    %% store.
    Dispatches0 = set_store_task_targets(peer, [store_a, store_b],
        test_dispatch(#{peer => 4}, #{peer => 4})),
    Task = #task{ state = queued, store_id = store_a },
    DispatchesA = enqueue_tasks(peer, [Task], Dispatches0),
    Dispatches1 = start_task(peer, Task, DispatchesA),
    ?assertEqual(1 / 4, store_load(peer, store_a, Dispatches1)),
    ?assertEqual(0.0, store_load(peer, store_b, Dispatches1)),
    %% One fetching task plus three queued tasks fills store A's share.
    Footprint = #footprint{ store_id = store_a },
    FootprintTask = #task{ state = queued, store_id = store_a,
        footprint = Footprint },
    Dispatches2 = enqueue_tasks(
        peer, [FootprintTask, FootprintTask, FootprintTask], Dispatches1),
    ?assertEqual(0,
        store_capacity(peer, store_a, Dispatches2)).

assigned_stores_remain_in_target_split_test() ->
    Peer = peer,
    StoreA = store_a,
    StoreB = store_b,
    %% Four active and four queued tasks fill the peer's combined assignment
    %% limit while store A is the only ready store.
    Dispatches0 = set_store_task_targets(Peer, [StoreA],
        test_dispatch(#{Peer => 4}, #{Peer => 4})),
    Task = #task{ state = queued, store_id = StoreA },
    DispatchesA = enqueue_tasks(Peer,
        lists:duplicate(4, Task), Dispatches0),
    Dispatches1 = lists:foldl(
        fun(_, Acc) -> start_task(Peer, Task, Acc) end,
        DispatchesA,
        lists:seq(1, 4)),
    FootprintTask = #task{ state = queued, store_id = StoreA,
        footprint = #footprint{ store_id = StoreA } },
    Dispatches1A = enqueue_tasks(Peer,
        [FootprintTask, FootprintTask, FootprintTask, FootprintTask],
        Dispatches1),
    Dispatches2 = set_store_task_targets(
        #{Peer => [StoreB]}, Dispatches1A),
    ?assertEqual(0, store_capacity(Peer, StoreA, Dispatches2)),
    ?assertEqual(0, store_capacity(Peer, StoreB, Dispatches2)).

source_capacity_filter_precedes_load_test() ->
    StoreID = store,
    BlockedPeer = blocked_peer,
    ReadyPeer = ready_peer,
    Sources = [
        #task_source{ peer = BlockedPeer },
        #task_source{ peer = ReadyPeer }
    ],
    %% The first peer has the lower tie-break value but its only slot is full.
    Dispatches0 = test_dispatch(
        #{BlockedPeer => 1, ReadyPeer => 2},
        #{BlockedPeer => 1, ReadyPeer => 2}),
    BlockedTask = #task{ state = queued, store_id = StoreID },
    DispatchesA = enqueue_tasks(BlockedPeer, [BlockedTask], Dispatches0),
    Dispatches = start_task(BlockedPeer, BlockedTask, DispatchesA),
    ReadySources = lists:filter(
        fun(Source) ->
            has_capacity(Source, Dispatches)
        end,
        Sources),
    [#task_source{ peer = ReadyPeer }] = ReadySources,
    ?assertMatch({ok, #task_source{ peer = ReadyPeer }},
        best_source(StoreID, ReadySources, Dispatches)).

record_result_accumulates_observations_test() ->
    Peer = {1, 2, 3, 4, 5},
    %% Two outcomes cover every timing category and only one delivers a chunk.
    FirstTiming = #fetch_timing{ productive_ms = 100, reject_ms = 20 },
    SecondTiming = #fetch_timing{ productive_ms = 50, timeout_ms = 30 },
    State1 = record_result(Peer, ?DATA_CHUNK_SIZE, FirstTiming, new()),
    State2 = record_result(Peer, 0, SecondTiming, State1),
    Observation = maps:get(Peer, State2#state.observations),
    ?assertEqual(?DATA_CHUNK_SIZE, Observation#observation.total_bytes),
    ?assertEqual(#fetch_timing{
            productive_ms = 150,
            reject_ms = 20,
            timeout_ms = 30
        },
        Observation#observation.fetch_timing),
    State3 = tick([Peer], #{Peer => 1}, #{Peer => true}, 1000, State2),
    Observation2 = maps:get(Peer, State3#state.observations),
    ?assertEqual(?DATA_CHUNK_SIZE, Observation2#observation.total_bytes),
    ?assertEqual(#fetch_timing{}, Observation2#observation.fetch_timing).

%% Delivery sampling: Δ cumulative bytes / Δ wall-clock, EWMA'd; first sighting is
%% undefined (rates 0.0) so a peer is trialed before being judged. Inactive peers
%% retain their last sample without accumulating idle measurements.
update_delivery_test() ->
    Fast = {1, 1, 1, 1, 1}, Slow = {2, 2, 2, 2, 2},
    %% Rates well above the one-chunk-per-tick noise floor so the snap does
    %% not engage: deliveries are in whole chunks.
    CS = float(?DATA_CHUNK_SIZE),
    Driven = #{ Fast => 1, Slow => 1 },
    D0 = update_delivery([Fast, Slow],
        #{ Fast => 0, Slow => 0 }, #{}, Driven, 0),
    ?assertMatch({_, 0, undefined}, maps:get(Fast, D0)),
    %% Over 1000 ms Fast delivered 3 chunks, Slow 1 -> realized 3 vs 1 chunk/ms... in bytes/ms.
    D1 = update_delivery([Fast, Slow],
        #{ Fast => 3 * ?DATA_CHUNK_SIZE, Slow => ?DATA_CHUNK_SIZE },
        D0, Driven, 1000),
    ?assertEqual({3 * CS, 1000, 3 * CS / 1000}, maps:get(Fast, D1)),
    ?assertEqual({CS, 1000, CS / 1000}, maps:get(Slow, D1)),
    %% DRIVEN and idle (work in flight, no new bytes): real signal, the EWMA
    %% decays toward 0 using the decrease weight.
    D2 = update_delivery([Fast], #{ Fast => 3 * ?DATA_CHUNK_SIZE },
        D1, Driven, 2000),
    DecreasedRate = arweave_util:ema(3 * CS / 1000, 0.0, ?GOODPUT_DECREASE_ALPHA),
    ?assertEqual(DecreasedRate, element(3, maps:get(Fast, D2))),
    %% Slow left the active set, so its sampling state is removed.
    ?assertNot(maps:is_key(Slow, D2)),
    %% UNFED and idle (nothing in flight, no new bytes): a missing sample,
    %% not a zero one - the snapshot advances, the estimate is retained.
    D3 = update_delivery([Fast], #{ Fast => 3 * ?DATA_CHUNK_SIZE },
        D2, #{}, 3000),
    ?assertEqual({3 * CS, 3000, DecreasedRate}, maps:get(Fast, D3)),
    %% Driven-idle ticks below the noise floor snap to 0.0 ("unmeasured"), so
    %% the peer returns to its exploration cap; the EWMA alone would decay
    %% asymptotically and never reach 0.0.
    D6 = lists:foldl(
        fun(N, Acc) ->
            update_delivery([Fast], #{ Fast => 3 * ?DATA_CHUNK_SIZE },
                Acc, Driven, 3000 + N * 1000)
        end, D3, lists:seq(1, 12)),
    ?assertMatch({_, _, +0.0}, maps:get(Fast, D6)),
    ?assertEqual(0.0, goodput_rate(undefined)),
    ?assertEqual(1.5, goodput_rate(1.5)).

aggregate_goodput_bounds_peer_sum_test() ->
    PeerA = peer_a,
    PeerB = peer_b,
    %% Individual asymmetric estimates sum to five while the aggregate path
    %% measured four. Scaling by four-fifths preserves peer proportions.
    Goodput = bound_aggregate_goodput(#{PeerA => 3.0, PeerB => 2.0}, 4.0),
    ?assertEqual(2.4, maps:get(PeerA, Goodput)),
    ?assertEqual(1.6, maps:get(PeerB, Goodput)),
    ?assertEqual(4.0, lists:sum(maps:values(Goodput))),
    %% A single peer keeps its burst-tolerant per-peer estimate.
    ?assertEqual(undefined,
        aggregate_goodput({[PeerA], {0.0, 0, 1.0}})).

aggregate_delivery_smooths_bucket_boundary_test() ->
    CS = float(?DATA_CHUNK_SIZE),
    Entry0 = aggregate_delivery_entry(0, 1, 0, undefined),
    %% Ten chunks in one second seed a ten-chunk/s aggregate estimate.
    Entry1 = aggregate_delivery_entry(10 * ?DATA_CHUNK_SIZE, 1, 1000, Entry0),
    ?assertEqual(10 * CS / 1000, element(3, Entry1)),
    %% A driven empty second retains four-fifths under the five-sample EWMA.
    Entry2 = aggregate_delivery_entry(
        10 * ?DATA_CHUNK_SIZE, 1, 2000, Entry1),
    ?assertEqual(8 * CS / 1000, element(3, Entry2)).

%% A productive driven peer probes upward, accepts a material goodput gain,
%% and backs off when a larger cap leaves goodput flat.
goodput_probe_test() ->
    %% A 0.51% gain clears the 0.5% confirmation floor; a 0.1% gain does not.
    ?assert(goodput_improved(100.51, 100.0)),
    ?assertNot(goodput_improved(100.1, 100.0)),
    Control0 = #cap_control{ cap = 100 },
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
    ?assertEqual(Control6,
        evolve_cap_control(105.0, 0, 0.0, true, Control6)),
    ?assertEqual(Control6,
        evolve_cap_control(105.0, 1000, 0.0, false, Control6)),
    %% Ten percent worker-time pressure scaled by 1.5 cuts 15% of the
    %% 105-request cap, rounding to 89.
    PressureCut = evolve_cap_control(110.0, 1000, 0.1, true,
        #cap_control{ cap = 105 }),
    ?assertEqual(89, PressureCut#cap_control.cap),
    %% One hundred percent failure pressure reaches the 50% cut clamp.
    FullCut = evolve_cap_control(0.0, 0, 1.0, true, Control0),
    ?assertEqual(50, FullCut#cap_control.cap),
    MinimumCut = evolve_cap_control(0.0, 0, 1.0, true,
        #cap_control{ cap = ?CONCURRENCY_CAP_MIN }),
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
        100.0, 18000, FastRejectPressure, true, Control0),
    ?assertEqual(98, FastRejectControl#cap_control.cap),
    ?assertEqual(cooldown, FastRejectControl#cap_control.phase).

%% A rejected probe holds the lower cap while old higher-cap completions and
%% their decreasing-rate EWMA contribution settle before the next baseline.
goodput_backoff_settles_before_retry_test() ->
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
    SettlingControl = lists:foldl(fun(_, Control) ->
        evolve_cap_control(90.0, 1000, 0.0, true, Control)
    end, Control4, lists:seq(1, 3)),
    ?assertEqual(105, SettlingControl#cap_control.cap),
    ControlAfterSettle = evolve_cap_control(
        90.0, 1000, 0.0, true, SettlingControl),
    ?assertEqual(113, ControlAfterSettle#cap_control.cap),
    ?assertEqual(105, ControlAfterSettle#cap_control.baseline_cap),
    ?assertEqual(90.0, ControlAfterSettle#cap_control.baseline_rate),
    ?assertEqual(probe, ControlAfterSettle#cap_control.phase).

queue_max_length_tracks_measured_delivery_test() ->
    HundredTaskRate = 100 * ?DATA_CHUNK_SIZE / ?QUEUE_TARGET_DURATION_MS,
    EightyTaskRate = 80 * ?DATA_CHUNK_SIZE / ?QUEUE_TARGET_DURATION_MS,
    ?assertEqual(100, queue_max_length(HundredTaskRate)),
    ?assertEqual(80, queue_max_length(EightyTaskRate)),
    ?assertEqual(?CONCURRENCY_CAP_INITIAL, queue_max_length(0.0)).

%% Caps and queue limits cover exactly the active peers, while cap memory is
%% retained when a peer leaves.
recompute_test() ->
    A = {1, 1, 1, 1, 1}, B = {2, 2, 2, 2, 2},
    No = fun(_) -> false end,
    Yes = fun(_) -> true end,
    %% About 100 MiB/s in bytes/ms derives a 1600-task queue limit:
    %% 400 chunks/s * 4 seconds.
    Rate = 104857.6,
    MeasuredQueueMaxLength = round(
        Rate * ?QUEUE_TARGET_DURATION_MS / ?DATA_CHUNK_SIZE),
    G = #{ A => Rate, B => Rate },
    InitialState = #{
        A => #cap_control{ cap = 100 },
        B => #cap_control{ cap = 100 }
    },
    Productive = #fetch_timing{ productive_ms = 1000 },
    Timings0 = #{ A => Productive, B => Productive },
    {Caps0, QueueMaxLengths0, S0} = recompute(
        [A, B], G, Timings0, Yes, InitialState),
    ?assertEqual(108, maps:get(A, Caps0)),
    ?assertEqual(108, maps:get(B, Caps0)),
    ?assertEqual(MeasuredQueueMaxLength, maps:get(A, QueueMaxLengths0)),
    ?assertEqual(MeasuredQueueMaxLength, maps:get(B, QueueMaxLengths0)),
    %% Forty percent failed worker time reaches the halving clamp, reducing B's
    %% 108-request probe to 54 while A continues its eight-request probe.
    Failed = #fetch_timing{ productive_ms = 600, timeout_ms = 400 },
    {Caps1, _QueueMaxLengths1, S1} = recompute([A, B], G,
        #{ A => Productive, B => Failed }, Yes, S0),
    ?assertEqual(116, maps:get(A, Caps1)),
    ?assertEqual(54, maps:get(B, Caps1)),
    ?assertEqual(54, (maps:get(B, S1))#cap_control.cap),
    %% B leaves the active-peer set: its caps entry disappears, but its
    %% last cap remains available if it returns.
    {Caps2, QueueMaxLengths2, S2} = recompute(
        [A], G, #{ A => Productive }, Yes, S1),
    ?assertEqual(false, maps:is_key(B, Caps2)),
    ?assertEqual(false, maps:is_key(B, QueueMaxLengths2)),
    ?assertEqual(54, (maps:get(B, S2))#cap_control.cap),
    {Caps3, _QueueMaxLengths3, _S3} = recompute(
        [A, B], G, Timings0, Yes, S2),
    ?assertEqual(54, maps:get(B, Caps3)),
    %% Lower delivery contracts queued work without changing an undriven cap.
    LowRateQueueMaxLength =
        round(5242.88 * ?QUEUE_TARGET_DURATION_MS / ?DATA_CHUNK_SIZE),
    {Caps4, QueueMaxLengths4, _} = recompute([A], #{ A => 5242.88 },
        #{ A => Productive }, No, #{ A => #cap_control{ cap = 200 } }),
    ?assertEqual(200, maps:get(A, Caps4)),
    ?assertEqual(LowRateQueueMaxLength, maps:get(A, QueueMaxLengths4)),
    %% Unmeasured peers receive an eight-task queue bootstrap.
    {Caps5, QueueMaxLengths5, _} = recompute([A], #{}, #{}, No, #{}),
    ?assertEqual(?CONCURRENCY_CAP_INITIAL, maps:get(A, Caps5)),
    ?assertEqual(?CONCURRENCY_CAP_INITIAL,
        maps:get(A, QueueMaxLengths5)),
    ok.

%% Worker-time pressure is isolated by peer and clean productive queued work
%% resumes growth immediately after a failed interval.
worker_time_pressure_isolated_test() ->
    Healthy = {1, 1, 1, 1, 1},
    TimedOut = {2, 2, 2, 2, 2},
    Yes = fun(_) -> true end,
    Rate = 104857.6,
    Goodput = #{ Healthy => Rate, TimedOut => Rate },
    State0 = #{
        Healthy => #cap_control{ cap = 100 },
        TimedOut => #cap_control{ cap = 100 }
    },
    Timings = #{
        Healthy => #fetch_timing{ productive_ms = 1000 },
        TimedOut => #fetch_timing{ timeout_ms = 1000 }
    },
    {Caps1, _QueueMaxLengths1, State1} = recompute(
        [Healthy, TimedOut], Goodput,
        Timings, Yes, State0),
    ?assertEqual(108, maps:get(Healthy, Caps1)),
    ?assertEqual(50, maps:get(TimedOut, Caps1)),
    Recovery = #{ TimedOut => #fetch_timing{ productive_ms = 1000 } },
    {Caps2, _QueueMaxLengths2, _State2} = recompute([TimedOut], Goodput,
        Recovery, Yes, State1),
    ?assertEqual(50, maps:get(TimedOut, Caps2)).

%% Flat driven goodput tests one eight-request window, then holds the lower cap
%% long enough to establish a baseline uncontaminated by old completions.
flat_driven_goodput_bounds_concurrency_test() ->
    Peer = {5, 5, 5, 5, 5},
    TickMs = 1000,
    RTTMs = 250,
    %% The initial observation starts at sixteen. Three more observations reach
    %% forty; the fourth flat comparison restores eight. Four settling
    %% observations retain eight before the ninth observation probes again.
    Caps = delivery_caps(Peer,
        [400, 400, 400, 400, 400, 400, 400, 400, 400],
        RTTMs, TickMs),
    ?assertEqual([16, 24, 32, 40, 8, 8, 8, 8, 16], Caps).

delivery_caps(Peer, ChunksPerTick, RTTMs, TickMs) ->
    State0 = tick([Peer], #{}, #{}, 0, new()),
    {_State, _Now, Caps} = lists:foldl(
        fun(Chunks, {State, Now, Acc}) ->
            Now2 = Now + TickMs,
            FetchTiming = #fetch_timing{
                productive_ms = Chunks * RTTMs
            },
            State1 = record_result(Peer,
                Chunks * ?DATA_CHUNK_SIZE, FetchTiming, State),
            State2 = tick([Peer], #{}, #{ Peer => true }, Now2, State1),
            Cap = maps:get(Peer, State2#state.caps),
            {State2, Now2, [Cap | Acc]}
        end, {State0, 0, []}, ChunksPerTick),
    lists:reverse(Caps).

-endif.

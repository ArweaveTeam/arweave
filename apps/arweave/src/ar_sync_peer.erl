%%% @doc The sync pipeline's per-peer control plane: measures each active
%%% peer's realized delivery, accounts the worker time consumed by fetch
%%% outcomes, and sizes its concurrency cap. ar_sync_scheduler reports fetch
%%% results and supplies the active, inflight, and demand views on each control
%%% tick. Throughout this module "cap" means how many fetch workers a peer may
%%% run at once.
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
%%% Each tick the cap is the minimum of a measured-delivery ceiling and a
%%% bounded growth/cut step:
%%%
%%% - pipeline_ceiling: at most four seconds of the peer's measured delivery.
%%%   This permits the deep request pipelines needed by high-latency storage
%%%   paths while bounding work assigned from stale goodput after a peer slows
%%%   down.
%%% - control_step: a productive peer retains the share of its cap not consumed
%%%   by failures, then probes upward while work has queued or in-flight demand.
%%%   The cut is proportional to the share of worker time consumed by 429s,
%%%   timeouts, and client errors.
%%%   Weighting by occupied worker time prevents a fast rejection from counting
%%%   like a long successful request while still reacting strongly to slow
%%%   failures that monopolize concurrency.
%%% - ?CONCURRENCY_CAP_INITIAL seed: unmeasured peers start with a modest
%%%   exploration pipeline. A collapsed peer may be cut to the one-worker
%%%   minimum and re-grows once it delivers again.
%%%
%%% State memory is retained for peers that leave the active-peer set (the map is
%%% bounded by distinct peers seen, a few hundred), so a peer that flaps out
%%% and back does not have to rediscover its budget through another storm.
%%%
%%% ↔ Prior art. Rate-based pipeline sizing follows the same principle as
%%% BitTorrent request pipelines. The worker-time cut is the one extension
%%% BitTorrent does not need (its peers queue excess
%%% rather than rejecting or shedding it; Arweave peers 429, and our
%%% own HTTP layer sheds as client_error under connection overload).
%%% An earlier true-BDP attempt (cap = gain x rate x MEASURED latency,
%%% 2026-07-09) could not probe: by Little's law rate x latency hands
%%% back the current cap, so it parked wherever it started. The fixed maximum
%%% delivery horizon provides headroom for continued measurement.
-module(ar_sync_peer).
-test_category([fast]).

-export([create_ets/0, reset_rows/0, new/0, record_result/4, tick/5,
        start_dispatch/2, record_task/3,
        set_store_task_targets/2, set_store_task_targets/3, concurrency_cap/2,
        load/2, store_load/3, store_capacity/4, has_capacity/2,
        priority/3,
        best_source/3,
        published_concurrency_cap_total/0]).
-export_type([state/0, dispatch/0]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_sync.hrl").

-ifdef(AR_TEST).
-include_lib("eunit/include/eunit.hrl").
-export([test_dispatch/1]).
-endif.

%% The minimum keeps one exploration fetch flowing (a cut can never
%% silence a peer entirely).
-define(CONCURRENCY_CAP_MIN, 1).

%% The exploration seed: what an unmeasured peer may run. It is modest enough
%% to be polite to an unknown peer and large enough to collect an initial
%% delivery sample; productive demand then grows toward its measured ceiling.
-define(CONCURRENCY_CAP_INITIAL, 8).

%% Keep at most four seconds of the peer's measured delivery in flight.
%% The target is derived from the July 17 live run: roughly 455 chunks/s at
%% 2.4 seconds mean response time required about 1,100 concurrent requests.
%% Four seconds leaves enough exploration headroom when response time grows
%% with request depth.
%% Regressions: peer_latency_increase_recovers_throughput_test_ and
%% high_latency_single_peer_saturation_test_.
-define(MAX_PIPELINE_MS, 4000).

%% Grow productive demand by one quarter per tick from the cap retained after
%% failure pressure. The measured pipeline ceiling remains the level bound.
-define(PRODUCTIVE_GROWTH_FACTOR, 1.25).

%% Productive demand probes at least eight requests beyond the retained cap so
%% small caps continue exploring when one-quarter growth would be smaller.
-define(CONCURRENCY_CAP_GROWTH_STEP, 8).

%% A full-failure tick cuts at most in half, so a burst cannot zero the cap
%% in one step and the walk-down to a sustainable depth remains geometric.
-define(MAX_TICK_CUT, 0.5).

%% Scale worker-time failure pressure before applying the cut. A one-third
%% failure share reaches the halving clamp; smaller shares leave room for the
%% concurrent growth step to find a stable operating depth.
-define(FAILURE_CUT_GAIN, 1.5).

%% There is deliberately no absolute cap: every bound is derived from
%% measured delivery, measured worker-time pressure,
%% or explicit config (the download-rate limit and the chunk
%% cache).
%% Regression: cap_step_test, recompute_test,
%% worker_time_pressure_isolated_test, and fast_peer_turns_slow_test (this
%% module); the outcome-level failure, recovery, link-saturation, and
%% high-latency scenarios in ar_sync_sim_tests.

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

-record(observation, {
    total_bytes = 0,
    fetch_timing = #fetch_timing{}
}).

-record(state, {
    %% Peer => previous cap, retained across active-peer membership and bounded
    %% by distinct peers seen.
    cap_memory = #{},
    %% Peer => cap published at the last tick (covers active peers only).
    caps = #{},
    %% Peer => {PrevTotalBytes, PrevTimeMs, GoodputEWMA | undefined}:
    %% snapshot of cumulative delivered bytes at the last active
    %% tick, used to derive realized goodput. Rebuilt from the active-peer set
    %% each tick, so peers that leave are evicted.
    delivery = #{},
    %% Peer => #observation{}: cumulative delivered bytes and fetch timing
    %% accumulated since the previous control tick.
    observations = #{}
}).

-opaque state() :: #state{}.

%% One local store's task load and target share of a peer during a dispatch.
-record(store_load, {
    active_task_count = 0,
    non_footprint_task_count = 0,
    target_task_count = 0
}).

-record(peer_dispatch, {
    inflight_count = 0,
    non_footprint_task_count = 0,
    concurrency_cap = ?CONCURRENCY_CAP_MIN,
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
    catch ets:delete(?MODULE, concurrency_cap_total),
    catch ets:delete(?MODULE, cap_memory),
    ok.

%% @doc Fresh peer state with no peers tracked.
new() ->
    #state{}.

%% @doc Snapshot the active peer caps for one scheduler dispatch.
start_dispatch(Tasks, #state{ caps = ConcurrencyCaps }) ->
    record_tasks(Tasks, new_dispatch(ConcurrencyCaps)).

record_tasks(Tasks, Dispatches) ->
    maps:fold(
        fun(_TaskRef, #task{ state = TaskState, peer = Peer } = Task, Acc)
                when TaskState =:= fetching; TaskState =:= writing ->
                record_task(Peer, Task, Acc);
            (_TaskRef, _Task, Acc) ->
                Acc
        end,
        Dispatches,
        Tasks).

new_dispatch(ConcurrencyCaps) ->
    Peers = maps:map(
        fun(_Peer, ConcurrencyCap) ->
            #peer_dispatch{ concurrency_cap =
                max(?CONCURRENCY_CAP_MIN, ConcurrencyCap) }
        end,
        ConcurrencyCaps),
    #dispatch{ peers = Peers }.

-ifdef(AR_TEST).
%% @doc Build a dispatch with explicit caps for focused scheduler tests.
test_dispatch(ConcurrencyCaps) ->
    new_dispatch(ConcurrencyCaps).
-endif.

%% @doc Count one existing or newly selected task. Fetching tasks consume both
%% network concurrency and the peer's share of one store; writing tasks retain
%% only the store share. Footprint tasks are counted through their footprint
%% state rather than duplicated here.
record_task(Peer, #task{ state = fetching } = Task, Dispatches) ->
    PeerDispatch = peer_dispatch(Peer, Dispatches),
    PeerDispatch2 = do_record_task(Task, PeerDispatch#peer_dispatch{
        inflight_count = PeerDispatch#peer_dispatch.inflight_count + 1
    }),
    put_peer_dispatch(Peer, PeerDispatch2, Dispatches);
record_task(Peer, #task{ state = writing } = Task, Dispatches) ->
    PeerDispatch = peer_dispatch(Peer, Dispatches),
    put_peer_dispatch(Peer,
        do_record_task(Task, PeerDispatch), Dispatches).

do_record_task(Task, #peer_dispatch{ stores = Stores } = PeerDispatch) ->
    #task{ store_id = StoreID, footprint = Footprint } = Task,
    NonFootprintIncrement = case Footprint of
        none -> 1;
        #footprint{} -> 0
    end,
    StoreLoad = maps:get(StoreID, Stores, #store_load{}),
    StoreLoad2 = StoreLoad#store_load{
        active_task_count = StoreLoad#store_load.active_task_count + 1,
        non_footprint_task_count =
            StoreLoad#store_load.non_footprint_task_count
                + NonFootprintIncrement
    },
    PeerDispatch#peer_dispatch{
        non_footprint_task_count =
            PeerDispatch#peer_dispatch.non_footprint_task_count
                + NonFootprintIncrement,
        stores = maps:put(StoreID, StoreLoad2, Stores)
    }.

%% @doc Set local destination-store task targets for every peer with work.
set_store_task_targets(StoresByPeer, Dispatches) ->
    maps:fold(
        fun(Peer, StoreIDs, Acc) ->
            set_store_task_targets(Peer, StoreIDs, Acc)
        end,
        Dispatches,
        StoresByPeer).

%% @doc Split one peer's concurrency cap across the supplied stores.
set_store_task_targets(Peer, StoreIDs, Dispatches) ->
    PeerDispatch = peer_dispatch(Peer, Dispatches),
    PeerDispatch2 = do_set_store_task_targets(StoreIDs, PeerDispatch),
    put_peer_dispatch(Peer, PeerDispatch2, Dispatches).

do_set_store_task_targets([], PeerDispatch) ->
    clear_store_task_targets(PeerDispatch);
do_set_store_task_targets(StoreIDs, PeerDispatch) ->
    StoreCount = length(StoreIDs),
    Target = max(1,
        (peer_concurrency_cap(PeerDispatch) + StoreCount - 1) div StoreCount),
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

%% @doc Return the fraction of this peer's concurrency cap in use.
load(Peer, Dispatches) ->
    peer_load(peer_dispatch(Peer, Dispatches)).

peer_load(#peer_dispatch{ inflight_count = InflightCount,
        concurrency_cap = ConcurrencyCap }) ->
    case ConcurrencyCap of
        Cap when Cap > 0 -> InflightCount / Cap;
        _ -> 1.0
    end.

%% @doc Return the fraction of this peer's store task target occupied by
%% fetching or writing tasks.
store_load(Peer, StoreID, Dispatches) ->
    StoreLoad = store_load(StoreID, peer_dispatch(Peer, Dispatches)),
    case StoreLoad#store_load.target_task_count of
        Target when Target > 0 ->
            StoreLoad#store_load.active_task_count / Target;
        _ ->
            1.0
    end.

%% @doc Number of additional footprint tasks allowed by both the peer-wide cap
%% and this store's task target. Counts include queued footprint tasks.
store_capacity(Peer, StoreID, FootprintDispatch, Dispatches) ->
    {StoreFootprintTasks, PeerFootprintTasks} =
        ar_sync_footprint:active_task_counts(Peer, StoreID, FootprintDispatch),
    PeerDispatch = peer_dispatch(Peer, Dispatches),
    PeerCap = PeerDispatch#peer_dispatch.concurrency_cap,
    StoreLoad = store_load(StoreID, PeerDispatch),
    StoreTaskCount = StoreFootprintTasks
        + StoreLoad#store_load.non_footprint_task_count,
    PeerTaskCount = PeerFootprintTasks
        + PeerDispatch#peer_dispatch.non_footprint_task_count,
    PairTarget = case StoreLoad#store_load.target_task_count of
        0 -> PeerCap;
        Target -> Target
    end,
    PairCapacity = max(0, PairTarget - StoreTaskCount),
    PeerCapacity = max(0, PeerCap - PeerTaskCount),
    min(PairCapacity, PeerCapacity).

%% @doc Return whether a peer or source may start another fetch.
has_capacity(#task_source{ peer = Peer }, Dispatches) ->
    has_capacity(Peer, Dispatches);
has_capacity(Peer, Dispatches) ->
    has_capacity(peer_dispatch(Peer, Dispatches)).

has_capacity(#peer_dispatch{ inflight_count = InflightCount,
        concurrency_cap = ConcurrencyCap }) ->
    InflightCount < ConcurrencyCap.

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

%% @doc Add one completed fetch attempt to the peer controller's observations.
record_result(Peer, DeliveredBytes, FetchTiming, State) ->
    #state{ observations = Observations } = State,
    Observation = maps:get(Peer, Observations, #observation{}),
    Observation2 = Observation#observation{
        total_bytes = Observation#observation.total_bytes + DeliveredBytes,
        fetch_timing = merge_fetch_timing(
            Observation#observation.fetch_timing, FetchTiming)
    },
    State#state{ observations = maps:put(Peer, Observation2, Observations) }.

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

%% @doc One control tick over the active-peer set: sample realized goodput and
%% recompute each cap from delivery, demand, and worker-time outcomes.
%% InflightCounts distinguishes an idle peer from one that delivered nothing
%% while requests were running.
%% DemandPeers is the scheduler's interval-wide demand view after its global
%% rate gate.
tick(Peers, InflightCounts, DemandPeers, NowMs, State) ->
    #state{
        delivery = Delivery,
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
    HasDemand = fun(Peer) ->
        maps:is_key(Peer, DemandPeers)
    end,
    Goodput = delivery_goodput(Delivery2),
    {Caps2, Memory2} = recompute(Peers, Goodput, FetchTimings,
        HasDemand, Memory),
    State2 = State#state{ cap_memory = Memory2, caps = Caps2,
        delivery = Delivery2,
        observations = reset_fetch_timings(Observations) },
    publish(State2),
    State2.

%%%===================================================================
%%% Publication.
%%%===================================================================

publish(#state{ caps = Caps } = State) ->
    stash_cap_memory(State),
    TotalCap = lists:sum(maps:values(Caps)),
    catch ets:insert(?MODULE, {concurrency_cap_total, TotalCap}),
    arweave_metrics:gauge_set(sync_total_concurrency_cap, TotalCap),
    Labels = [arweave_util:format_peer(Peer) || Peer <- maps:keys(Caps)],
    prune_stale_peer_metrics(Labels),
    ok.

prune_stale_peer_metrics(CurrentLabels) ->
    lists:foreach(
        fun(Name) -> prune_stale_labels(Name, CurrentLabels) end,
        [sync_peer_concurrency_cap,
            sync_peer_goodput_bytes_per_second,
            sync_peer_pipeline_ceiling,
            sync_peer_control_ceiling,
            sync_peer_failure_pressure]).

prune_stale_labels(Name, CurrentLabels) ->
    ExistingLabels = [Value
        || {[{_LabelName, Value}], _MetricValue} <-
            arweave_metrics:gauge_values(Name)],
    lists:foreach(
        fun(Label) -> arweave_metrics:gauge_remove(Name, [Label]) end,
        ExistingLabels -- CurrentLabels).

%% @doc Sum of the latest active-peer concurrency caps.
published_concurrency_cap_total() ->
    try ets:lookup(?MODULE, concurrency_cap_total) of
        [{concurrency_cap_total, Value}] -> Value;
        _ -> 0
    catch _:_ -> 0 end.

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
                    %% capacity": the estimate decays, the pipeline ceiling
                    %% clamps the cap, and the scheduler assigns even less work.
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

%%%===================================================================
%%% Derived cap.
%%%===================================================================

%% @doc Derive each active peer's cap from measured delivery, worker-time
%% outcomes, and demand. The pipeline ceiling bounds the result; cap_step/4
%% controls how quickly the previous cap approaches it or retreats from
%% failure pressure.
recompute(Peers, Goodput, FetchTimings, HasDemand, State) ->
    lists:foldl(
        fun(Peer, {CapsAcc, StateAcc}) ->
            PrevCap = maps:get(Peer, StateAcc, ?CONCURRENCY_CAP_INITIAL),
            Rate = maps:get(Peer, Goodput, 0.0),
            FetchTiming = maps:get(Peer, FetchTimings, #fetch_timing{}),
            {Pressure, FailureMs} = failure_pressure(FetchTiming),
            ProductiveMs = FetchTiming#fetch_timing.productive_ms,
            PipelineCeiling = pipeline_ceiling(Rate),
            ControlCeiling = cap_step(
                PrevCap, ProductiveMs, Pressure, HasDemand(Peer)),
            Cap = max(?CONCURRENCY_CAP_MIN,
                min(PipelineCeiling, ControlCeiling)),
            log_cap_decision(Peer, PrevCap, Cap,
                limiting_bound(PipelineCeiling, ControlCeiling),
                Rate, PipelineCeiling, ControlCeiling, Pressure,
                ProductiveMs, FailureMs, HasDemand(Peer), FetchTiming),
            {maps:put(Peer, Cap, CapsAcc), maps:put(Peer, Cap, StateAcc)}
        end,
        {#{}, State},
        Peers).

%% @doc Identify which independently computed ceiling determines the cap.
limiting_bound(PipelineCeiling, ControlCeiling)
        when ControlCeiling =< PipelineCeiling ->
    control_step;
limiting_bound(_PipelineCeiling, _ControlCeiling) ->
    pipeline.

%% @doc Record every input and bound used for a peer's cap decision. This is
%% emitted once per active peer per control tick for live controller diagnosis.
log_cap_decision(Peer, PrevCap, Cap, LimitingBound, Rate,
        PipelineCeiling, ControlCeiling, Pressure,
        ProductiveMs, FailureMs, HasDemand, FetchTiming) ->
    Label = arweave_util:format_peer(Peer),
    arweave_metrics:gauge_set(sync_peer_concurrency_cap, [Label], Cap),
    arweave_metrics:gauge_set(sync_peer_goodput_bytes_per_second, [Label],
        Rate * 1000),
    arweave_metrics:gauge_set(sync_peer_pipeline_ceiling, [Label],
        PipelineCeiling),
    arweave_metrics:gauge_set(sync_peer_control_ceiling, [Label],
        ControlCeiling),
    arweave_metrics:gauge_set(sync_peer_failure_pressure, [Label], Pressure),
    ?LOG_DEBUG([{event, sync_peer_cap_decision},
        {peer, Label},
        {previous_cap, PrevCap},
        {cap, Cap},
        {limiting_bound, LimitingBound},
        {goodput_mib_per_second, Rate * 1000 / ?MiB},
        {pipeline_ceiling, PipelineCeiling},
        {control_ceiling, ControlCeiling},
        {failure_pressure, Pressure},
        {productive_worker_ms, ProductiveMs},
        {failure_worker_ms, FailureMs},
        {reject_worker_ms, FetchTiming#fetch_timing.reject_ms},
        {timeout_worker_ms, FetchTiming#fetch_timing.timeout_ms},
        {client_error_worker_ms, FetchTiming#fetch_timing.client_error_ms},
        {has_demand, HasDemand}]).

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

%% @doc Move the previous cap toward the measurement-derived ceilings while
%% demand remains productive, and cut it according to failed worker-time share.
%% Productive demand probes upward from the retained cap, so occasional errors
%% do not force an otherwise healthy peer into additive-only growth.
cap_step(PrevCap, ProductiveMs, Pressure, HasDemand) ->
    Cut = min(?MAX_TICK_CUT, ?FAILURE_CUT_GAIN * Pressure),
    RetainedCap = round(PrevCap * (1.0 - Cut)),
    case ProductiveMs > 0 andalso HasDemand of
        true ->
            max(RetainedCap + ?CONCURRENCY_CAP_GROWTH_STEP,
                round(RetainedCap * ?PRODUCTIVE_GROWTH_FACTOR));
        false ->
            max(?CONCURRENCY_CAP_MIN, RetainedCap)
    end.

%% @doc Bound the cap to four seconds of measured delivery, with an
%% exploration floor.
pipeline_ceiling(Rate) when Rate > 0.0 ->
    max(?CONCURRENCY_CAP_INITIAL,
        round(Rate * ?MAX_PIPELINE_MS / ?DATA_CHUNK_SIZE));
pipeline_ceiling(_Rate) ->
    ?CONCURRENCY_CAP_INITIAL.

%%%===================================================================
%%% Tests.
%%%===================================================================

-ifdef(AR_TEST).

dispatch_tracks_fetch_capacity_test() ->
    StoreID = store,
    %% Two concurrent fetches fill this peer's two-request cap.
    Dispatches0 = test_dispatch(#{peer => 2}),
    ?assertEqual(0.0, load(peer, Dispatches0)),
    Task = #task{ state = fetching, store_id = StoreID },
    Dispatches1 = record_task(peer, Task, Dispatches0),
    ?assertEqual(0.5, load(peer, Dispatches1)),
    ?assert(has_capacity(peer, Dispatches1)),
    Dispatches2 = record_task(peer, Task, Dispatches1),
    ?assertEqual(1.0, load(peer, Dispatches2)),
    ?assertNot(has_capacity(peer, Dispatches2)).

dispatch_splits_peer_capacity_across_stores_test() ->
    %% A four-request cap split across two stores gives each a target of two.
    Dispatches0 = set_store_task_targets(peer, [store_a, store_b],
        test_dispatch(#{peer => 4})),
    Task = #task{ state = fetching, store_id = store_a },
    Dispatches1 = record_task(peer, Task, Dispatches0),
    ?assertEqual(0.5, store_load(peer, store_a, Dispatches1)),
    ?assertEqual(0.0, store_load(peer, store_b, Dispatches1)),
    %% One ordinary task plus one reserved task fills store_a's two-task share.
    Reservation = ar_sync_footprint:test_reservation(
        store_a, #footprint{ store_id = store_a }, [], peer, 1, bound),
    FootprintDispatch = ar_sync_footprint:test_dispatch(
        ar_sync_footprint:test_state([Reservation]), 1),
    ?assertEqual(0,
        store_capacity(peer, store_a, FootprintDispatch, Dispatches1)).

source_capacity_filter_precedes_load_test() ->
    StoreID = store,
    BlockedPeer = blocked_peer,
    ReadyPeer = ready_peer,
    Sources = [
        #task_source{ peer = BlockedPeer },
        #task_source{ peer = ReadyPeer }
    ],
    %% The first peer has the lower tie-break value but its only slot is full.
    Dispatches0 = test_dispatch(#{BlockedPeer => 1, ReadyPeer => 2}),
    BlockedTask = #task{ state = fetching, store_id = StoreID },
    Dispatches = record_task(BlockedPeer, BlockedTask, Dispatches0),
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

%% Productive demand probes upward from the cap retained after failure
%% pressure. Demand without useful delivery only applies the cut.
cap_step_test() ->
    ?assertEqual(100, cap_step(100, 0, 0.0, true)),
    ?assertEqual(100, cap_step(100, 1000, 0.0, false)),
    ?assertEqual(125, cap_step(100, 1000, 0.0, true)),
    %% Ten percent failure pressure retains 85 requests, then a one-quarter
    %% probe reaches round(85 * 1.25) = 106.
    ?assertEqual(106, cap_step(100, 1000, 0.1, true)),
    %% One hundred percent failure pressure reaches the 50% cut clamp and has
    %% no productive work with which to justify growth.
    ?assertEqual(50, cap_step(100, 0, 1.0, true)),
    ?assertEqual(?CONCURRENCY_CAP_MIN,
        cap_step(?CONCURRENCY_CAP_MIN, 0, 1.0, true)),
    %% The category split remains diagnostic; pressure uses their total time.
    Timing = #fetch_timing{
        productive_ms = 9000,
        reject_ms = 250,
        timeout_ms = 500,
        client_error_ms = 250
    },
    ?assertEqual({0.1, 1000}, failure_pressure(Timing)),
    %% Nine two-second successes and one 250 ms 429 yield only 1.37%
    %% worker-time pressure. The retained cap is 98 and the one-quarter probe
    %% reaches round(98 * 1.25) = 123.
    FastRejectTiming = #fetch_timing{
        productive_ms = 18000,
        reject_ms = 250
    },
    {FastRejectPressure, 250} = failure_pressure(FastRejectTiming),
    ?assertEqual(123, cap_step(100, 18000, FastRejectPressure, true)).

%% Caps cover exactly the active peers, retain per-peer control memory when a
%% peer leaves, and remain bounded by measured delivery.
recompute_test() ->
    A = {1, 1, 1, 1, 1}, B = {2, 2, 2, 2, 2},
    No = fun(_) -> false end,
    Yes = fun(_) -> true end,
    %% About 100 MiB/s in bytes/ms derives the capped 1600-request,
    %% four-second pipeline ceiling.
    Rate = 104857.6,
    Ceiling = round(Rate * ?MAX_PIPELINE_MS / ?DATA_CHUNK_SIZE),
    G = #{ A => Rate, B => Rate },
    InitialState = #{
        A => 100,
        B => 100
    },
    Productive = #fetch_timing{ productive_ms = 1000 },
    Timings0 = #{ A => Productive, B => Productive },
    {Caps0, S0} = recompute([A, B], G, Timings0, Yes, InitialState),
    ?assertEqual(125, maps:get(A, Caps0)),
    ?assertEqual(125, maps:get(B, Caps0)),
    %% Forty percent failed worker time reaches the halving clamp. The retained
    %% cap is 63 and the one-quarter probe reaches round(63 * 1.25) = 79.
    Failed = #fetch_timing{ productive_ms = 600, timeout_ms = 400 },
    {Caps1, S1} = recompute([A, B], G,
        #{ A => Productive, B => Failed }, Yes, S0),
    ?assertEqual(156, maps:get(A, Caps1)),
    ?assertEqual(79, maps:get(B, Caps1)),
    ?assertEqual(79, maps:get(B, S1)),
    %% B leaves the active-peer set: its caps entry disappears, but its
    %% last cap remains available if it returns.
    {Caps2, S2} = recompute([A], G, #{ A => Productive }, Yes, S1),
    ?assertEqual(false, maps:is_key(B, Caps2)),
    ?assertEqual(79, maps:get(B, S2)),
    {Caps3, _S3} = recompute([A, B], G, Timings0, Yes, S2),
    ?assertEqual(99, maps:get(B, Caps3)),
    %% Pipeline ceiling: a peer whose measured delivery collapses is clamped
    %% to four seconds of its 5242.88 B/ms measured rate.
    LowRateCeiling =
        round(5242.88 * ?MAX_PIPELINE_MS / ?DATA_CHUNK_SIZE),
    {Caps4, _} = recompute([A], #{ A => 5242.88 },
        #{ A => Productive }, No, #{ A => 100 }),
    ?assertEqual(LowRateCeiling, maps:get(A, Caps4)),
    %% Unmeasured peers retain the eight-request exploration seed.
    {Caps5, _} = recompute([A], #{}, #{}, No, #{}),
    ?assertEqual(?CONCURRENCY_CAP_INITIAL, maps:get(A, Caps5)),
    ?assertEqual(1600, Ceiling),
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
        Healthy => 100,
        TimedOut => 100
    },
    Timings = #{
        Healthy => #fetch_timing{ productive_ms = 1000 },
        TimedOut => #fetch_timing{ timeout_ms = 1000 }
    },
    {Caps1, State1} = recompute([Healthy, TimedOut], Goodput,
        Timings, Yes, State0),
    ?assertEqual(125, maps:get(Healthy, Caps1)),
    ?assertEqual(50, maps:get(TimedOut, Caps1)),
    Recovery = #{ TimedOut => #fetch_timing{ productive_ms = 1000 } },
    {Caps2, _State2} = recompute([TimedOut], Goodput,
        Recovery, Yes, State1),
    ?assertEqual(63, maps:get(TimedOut, Caps2)).

%% A formerly fast peer must shed its old concurrency limit on the delivery
%% trace alone; siblings then cannot be trapped behind that stale limit.
fast_peer_turns_slow_test() ->
    Peer = {5, 5, 5, 5, 5},
    TickMs = 1000,
    RTTMs = 250,
    %% Twenty-four productive queued ticks grow the eight-request seed to the
    %% 1600-request pipeline ceiling.
    WarmTicks = 24,
    %% Ten EWMA updates move 400 cps close enough to 10 cps for the
    %% response-time ceiling to reduce the cap to roughly one-eighth of its
    %% fast value.
    Caps = delivery_caps(Peer,
        lists:duplicate(WarmTicks, 400) ++ lists:duplicate(10, 10),
        RTTMs, TickMs),
    FastCap = lists:nth(WarmTicks, Caps),
    SlowCaps = lists:nthtail(WarmTicks, Caps),
    ?assertEqual(1600, FastCap),
    %% The stale goodput estimate may briefly retain the old cap, but the
    %% measured-delivery ceiling must then drain it substantially.
    ?assert(lists:last(SlowCaps) =< FastCap div 8).

%% A serve-rate step may raise the cap only to the configured maximum delivery
%% horizon, and clean growth approaches that ceiling by one quarter per tick.
rate_step_cap_bounded_test() ->
    Peer = {7, 7, 7, 7, 7},
    TickMs = 1000,
    RTTMs = 250,
    %% Twenty ticks approach the old 400-request ceiling; ten ticks at the
    %% higher rate continue toward the new 1600-request ceiling.
    WarmRates = lists:duplicate(20, 100),
    StepRates = lists:duplicate(10, 400),
    Caps = delivery_caps(Peer, WarmRates ++ StepRates, RTTMs, TickMs),
    StepCaps = lists:nthtail(length(WarmRates), Caps),
    NewPipeline = round(400 * ?MAX_PIPELINE_MS / TickMs),
    ?assert(lists:max(StepCaps) =< NewPipeline),
    %% Every clean tick is bounded by one quarter of the previous cap; the cap
    %% still rises after useful capacity increases.
    ?assert(lists:all(fun({A, B}) ->
            B =< max(A + 1, round(A * ?PRODUCTIVE_GROWTH_FACTOR))
        end, lists:zip([hd(StepCaps) | lists:droplast(StepCaps)], StepCaps))),
    ?assert(lists:last(StepCaps) > hd(StepCaps)).

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

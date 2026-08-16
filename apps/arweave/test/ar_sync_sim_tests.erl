%%% @doc Simulation tests for chunk syncing: the real pipeline processes run
%%% against a synthetic world (ar_sync_deps_sim) on ar_timer's simulated clock.
%%% ar_sync_sim_runner owns process and clock mechanics; scenarios declare a
%%% #sim_world{}, advance it, and assert stable outcome and performance
%%% contracts rather than exact schedules or internal scheduler state. These
%%% contracts are intended to replace repeated multi-hour live-node checks when
%%% allocation, backpressure, ratings, limits, or scheduling are changed.
-module(ar_sync_sim_tests).
-test_category([fast]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_sync.hrl").
-include("ar_sync_sim.hrl").

%% The store count just needs to be comfortably plural; sweep cost is per store.

%% Real {A,B,C,D,Port} peers: production paths format them.
-define(PEER_UNLIMITED, {10,0,0,1,1984}).
-define(PEER_LIMITED_1, {10,0,0,11,1984}).
-define(PEER_LIMITED_2, {10,0,0,12,1984}).
-define(PEER_LIMITED_3, {10,0,0,13,1984}).
-define(PEER_LIMITED_4, {10,0,0,14,1984}).
-define(PEER_LIMITED_5, {10,0,0,15,1984}).
-define(PEER_TIMEOUT, {10,0,0,66,1984}).
-define(PEER_SLOW, {10,0,0,2,1984}).
-define(PEER_FLAKY, {10,0,0,3,1984}).

%% Settled deterministic scenarios should use nearly all of their modeled
%% limiting capacity. Five percent allows scheduler and measurement boundaries
%% without accepting independent ten-percent losses across the sync process.
-define(MIN_STEADY_STATE_UTILIZATION, 0.95).

%%====================================================================
%% Test cases
%%====================================================================

%% Contract: a healthy heterogeneous peer population reaches its aggregate
%% serving capacity without allowing the fastest peer to starve slower peers.
%%
%% The configured capacities sum to 455 chunks/s. Over the settled measurement
%% window, stored throughput must reach at least 95% of that capacity, the 250
%% chunks/s peer must stay below 57% of delivered work, and every peer with rate
%% limiting enabled must reach at least 95% of its own capacity. The 5 chunks/s
%% peer covers the extreme slow-peer case without a separate scenario.
steady_state_no_monopoly_test_() ->
    ar_sync_sim_runner:setup_sim(fun test_steady_state_no_monopoly/0, 600).

test_steady_state_no_monopoly() ->
    Peers = #{
        ?PEER_UNLIMITED => #sim_peer{ max_serve_cps = 250 },
        ?PEER_LIMITED_1 => #sim_peer{ max_serve_cps = 60, limited = true },
        ?PEER_LIMITED_2 => #sim_peer{ max_serve_cps = 60, limited = true },
        ?PEER_LIMITED_3 => #sim_peer{ max_serve_cps = 40, limited = true },
        ?PEER_LIMITED_4 => #sim_peer{ max_serve_cps = 40, limited = true },
        ?PEER_LIMITED_5 => #sim_peer{
            max_serve_cps = 5,
            latency_ms = 4 * ?SIM_SUBSTEP_MS
        }
    },
    %% Forty simulated seconds allow peer allocation to settle; ten more
    %% provide about 50 chunks from the slowest 5 chunks/s peer.
    ar_sync_sim_runner:start_sim(#sim_world{ peers = Peers }),
    ar_sync_sim_runner:run_for(40),
    Measurement = ar_sync_sim_runner:run_for(10),
    CpsByPeer = ar_sync_sim_runner:metric(cps_by_peer, Measurement),
    assert_metric_utilization(stored_cps, capacity(Peers), Measurement),
    %% The expected fast-peer share is 250 / 455 = 0.549; 0.57 leaves
    %% measurement margin while rejecting a material shift toward that peer.
    ServedCPS = lists:sum(maps:values(CpsByPeer)),
    assert_less_than(
        maps:get(?PEER_UNLIMITED, CpsByPeer) / ServedCPS,
        0.57,
        #{ peer => ?PEER_UNLIMITED, metric => cps_share }),
    Minimum60CPS = ?MIN_STEADY_STATE_UTILIZATION * 60,
    assert_at_least(
        maps:get(?PEER_LIMITED_1, CpsByPeer),
        Minimum60CPS,
        #{ peer => ?PEER_LIMITED_1, metric => cps }),
    assert_at_least(
        maps:get(?PEER_LIMITED_2, CpsByPeer),
        Minimum60CPS,
        #{ peer => ?PEER_LIMITED_2, metric => cps }),
    Minimum40CPS = ?MIN_STEADY_STATE_UTILIZATION * 40,
    assert_at_least(
        maps:get(?PEER_LIMITED_3, CpsByPeer),
        Minimum40CPS,
        #{ peer => ?PEER_LIMITED_3, metric => cps }),
    assert_at_least(
        maps:get(?PEER_LIMITED_4, CpsByPeer),
        Minimum40CPS,
        #{ peer => ?PEER_LIMITED_4, metric => cps }),
    assert_at_least(
        maps:get(?PEER_LIMITED_5, CpsByPeer),
        ?MIN_STEADY_STATE_UTILIZATION * 5,
        #{ peer => ?PEER_LIMITED_5, metric => cps }),
    ok.

%% Contract: intermittent peer outages do not permanently shift work onto an
%% always-healthy peer. Every recovering peer resumes service and aggregate
%% throughput remains close to the capacity available outside the outage windows.
%%
%% Five 50 chunks/s peers take turns timing out for 6 of each 30 ticks. The
%% expected settled rate is about 490 of the population's 500 chunks/s capacity;
%% both aggregate and per-peer throughput must reach at least 95% of capacity.
timeout_resilience_test_() ->
    ar_sync_sim_runner:setup_sim(fun test_timeout_resilience/0, 600).

test_timeout_resilience() ->
    LimitedPeers = [?PEER_LIMITED_1, ?PEER_LIMITED_2, ?PEER_LIMITED_3,
        ?PEER_LIMITED_4, ?PEER_LIMITED_5],
    Peers0 = #{
        ?PEER_UNLIMITED => #sim_peer{ max_serve_cps = 250 },
        ?PEER_LIMITED_1 => #sim_peer{ max_serve_cps = 50, limited = true },
        ?PEER_LIMITED_2 => #sim_peer{ max_serve_cps = 50, limited = true },
        ?PEER_LIMITED_3 => #sim_peer{ max_serve_cps = 50, limited = true },
        ?PEER_LIMITED_4 => #sim_peer{ max_serve_cps = 50, limited = true },
        ?PEER_LIMITED_5 => #sim_peer{ max_serve_cps = 50, limited = true }
    },
    %% Assign each peer a different timeout cadence.
    Peers = maps:map(
        fun(Name, Peer) ->
            case arweave_util:index_of(Name, LimitedPeers) of
                undefined -> Peer;
                Index -> Peer#sim_peer{ failure_policy = fun(Tick, _RequestSequence) ->
                    case ((Tick div 30) rem 5) =:= Index - 1
                            andalso (Tick rem 30) < 6 of
                        true -> timeout;
                        false -> none
                    end
                end }
            end
        end,
        Peers0),
    ar_sync_sim_runner:start_sim(#sim_world{ peers = Peers }),
    %% Thirty ticks establish the scheduler state before a full 150-tick
    %% five-peer outage rotation supplies the measurement window.
    ar_sync_sim_runner:run_for(30),
    Measurement = ar_sync_sim_runner:run_for(150),
    CpsByPeer = ar_sync_sim_runner:metric(cps_by_peer, Measurement),
    ServedCPS = lists:sum(maps:values(CpsByPeer)),
    TimeoutsByPeer = ar_sync_sim_runner:metric(timed_out_by_peer, Measurement),
    %% One rotation must exercise every limited peer, not merely the fault path
    %% for one member of the population.
    assert_greater_than(
        maps:get(?PEER_LIMITED_1, TimeoutsByPeer, 0),
        0,
        #{ peer => ?PEER_LIMITED_1, metric => timed_out_by_peer }),
    assert_greater_than(
        maps:get(?PEER_LIMITED_2, TimeoutsByPeer, 0),
        0,
        #{ peer => ?PEER_LIMITED_2, metric => timed_out_by_peer }),
    assert_greater_than(
        maps:get(?PEER_LIMITED_3, TimeoutsByPeer, 0),
        0,
        #{ peer => ?PEER_LIMITED_3, metric => timed_out_by_peer }),
    assert_greater_than(
        maps:get(?PEER_LIMITED_4, TimeoutsByPeer, 0),
        0,
        #{ peer => ?PEER_LIMITED_4, metric => timed_out_by_peer }),
    assert_greater_than(
        maps:get(?PEER_LIMITED_5, TimeoutsByPeer, 0),
        0,
        #{ peer => ?PEER_LIMITED_5, metric => timed_out_by_peer }),
    assert_metric_utilization(stored_cps, capacity(Peers), Measurement),
    assert_less_than(
        maps:get(?PEER_UNLIMITED, CpsByPeer) / ServedCPS,
        0.55,
        #{ peer => ?PEER_UNLIMITED, metric => cps_share }),
    %% Each limited peer is unavailable for 6 of the 150 measured seconds, so
    %% its available capacity is 50 * 144 / 150 = 48 chunks/s.
    AvailableLimitedCPS = 50 * (150 - 6) / 150,
    MinimumLimitedCPS =
        ?MIN_STEADY_STATE_UTILIZATION * AvailableLimitedCPS,
    assert_at_least(
        maps:get(?PEER_LIMITED_1, CpsByPeer),
        MinimumLimitedCPS,
        #{ peer => ?PEER_LIMITED_1, metric => cps }),
    assert_at_least(
        maps:get(?PEER_LIMITED_2, CpsByPeer),
        MinimumLimitedCPS,
        #{ peer => ?PEER_LIMITED_2, metric => cps }),
    assert_at_least(
        maps:get(?PEER_LIMITED_3, CpsByPeer),
        MinimumLimitedCPS,
        #{ peer => ?PEER_LIMITED_3, metric => cps }),
    assert_at_least(
        maps:get(?PEER_LIMITED_4, CpsByPeer),
        MinimumLimitedCPS,
        #{ peer => ?PEER_LIMITED_4, metric => cps }),
    assert_at_least(
        maps:get(?PEER_LIMITED_5, CpsByPeer),
        MinimumLimitedCPS,
        #{ peer => ?PEER_LIMITED_5, metric => cps }),
    ok.

%% Contract: after a fast peer becomes rate limited, syncing settles near the
%% temporary useful rate while continuing to probe it. Once the limit is
%% removed, the peer returns to its previous useful throughput.
rate_limit_settles_and_recovers_test_() ->
    ar_sync_sim_runner:setup_sim(
        fun test_rate_limit_settles_and_recovers/0, 400).

test_rate_limit_settles_and_recovers() ->
    HighCps = 400,
    LimitedCps = 80,
    Peer = #sim_peer{ max_serve_cps = HighCps, latency_ms = 250 },
    Peers = #{ ?PEER_LIMITED_1 => Peer },
    World = #sim_world{ peers = Peers },
    ar_sync_sim_runner:start_sim(World),
    %% At 400 chunks/s and 250 ms latency, 100 concurrent requests fill the
    %% path. Forty ticks cover the scheduler's 32-tick evidence horizon.
    ar_sync_sim_runner:run_for(40),
    InitialMeasurement = ar_sync_sim_runner:run_for(10),
    assert_metric_utilization(stored_cps, HighCps, InitialMeasurement),

    LimitedPeer = Peer#sim_peer{
        max_serve_cps = LimitedCps,
        limited = true
    },
    ar_sync_sim_runner:update_world(World#sim_world{
        peers = Peers#{ ?PEER_LIMITED_1 := LimitedPeer }
    }),
    %% Twenty ticks expose the previous 100-request pipeline to the 80 chunks/s
    %% limit and let the peer cap settle across more than half of its 32-tick
    %% evidence horizon before throughput is measured.
    LimitDiscoveryMeasurement = ar_sync_sim_runner:run_for(20),
    assert_greater_than(
        ar_sync_sim_runner:metric(
            {rejected_by_peer, ?PEER_LIMITED_1},
            LimitDiscoveryMeasurement),
        0,
        #{ peer => ?PEER_LIMITED_1, metric => rejected_by_peer }),
    LimitedMeasurement = ar_sync_sim_runner:run_for(20),
    LimitedServed = ar_sync_sim_runner:metric(
        {served_by_peer, ?PEER_LIMITED_1}, LimitedMeasurement),
    LimitedRejected = ar_sync_sim_runner:metric(
        {rejected_by_peer, ?PEER_LIMITED_1}, LimitedMeasurement),
    assert_metric_utilization(stored_cps, LimitedCps, LimitedMeasurement),
    %% Rejections and successes have equal latency in this scenario. Failures
    %% must not consume more request time than useful delivery.
    assert_at_most(LimitedRejected, LimitedServed,
        #{ peer => ?PEER_LIMITED_1, metric => rejected_by_peer }),

    ar_sync_sim_runner:update_world(World),
    %% Twenty clean scheduler ticks should restore the original rate without
    %% immediately discarding the learned limit after one quiet sample.
    ar_sync_sim_runner:run_for(20),
    RecoveryMeasurement = ar_sync_sim_runner:run_for(10),
    assert_metric_utilization(stored_cps, HighCps, RecoveryMeasurement).

%% Contract: a peer that regularly rate limits requests continues serving
%% near its useful capacity. One hundred chunks/s at two-second latency
%% requires about 200 productive requests in flight. Every eighth request
%% receives a fast 250 ms 429: seven successful requests consume 14000 ms of
%% worker time while one rejection consumes 250 ms, keeping recurring rejection
%% pressure below two percent of occupied worker time.
recurring_rate_limit_preserves_throughput_test_() ->
    ar_sync_sim_runner:setup_sim(
        fun test_recurring_rate_limit_preserves_throughput/0, 400).

test_recurring_rate_limit_preserves_throughput() ->
    MaxServeCps = 100,
    RejectEvery = 8,
    Peer = #sim_peer{
        max_serve_cps = MaxServeCps,
        latency_ms = 2000,
        failure_policy = fun(_Tick, RequestSequence) ->
            case RequestSequence rem RejectEvery of
                0 -> {reject, 250};
                _ -> none
            end
        end
    },
    ar_sync_sim_runner:start_sim(#sim_world{
        peers = #{ ?PEER_LIMITED_1 => Peer }
    }),
    %% Forty ticks cover the scheduler's 32-tick evidence horizon.
    ar_sync_sim_runner:run_for(40),
    %% Sixty ticks smooth the recurring rejection cadence and measurement
    %% boundaries against the 100 chunks/s serving rate.
    Measurement = ar_sync_sim_runner:run_for(60),
    assert_greater_than(
        ar_sync_sim_runner:metric(
            {rejected_by_peer, ?PEER_LIMITED_1}, Measurement),
        0,
        #{ peer => ?PEER_LIMITED_1, metric => rejected_by_peer }),
    assert_metric_utilization(stored_cps, MaxServeCps, Measurement).

%% Contract: occasional full-latency client errors do not permanently suppress
%% a healthy peer's useful throughput. Every fortieth request fails after the
%% same four-second latency as a success, matching the latency and 2.5%
%% worker-time failure share observed in the live single-peer run.
recurring_client_errors_preserve_throughput_test_() ->
    ar_sync_sim_runner:setup_sim(
        fun test_recurring_client_errors_preserve_throughput/0, 400).

test_recurring_client_errors_preserve_throughput() ->
    MaxServeCPS = 400,
    ErrorEvery = 40,
    Peer = #sim_peer{
        max_serve_cps = MaxServeCPS,
        latency_ms = 4000,
        failure_policy = fun(_Tick, RequestSequence) ->
            case RequestSequence rem ErrorEvery of
                0 -> client_error;
                _ -> none
            end
        end
    },
    ar_sync_sim_runner:start_sim(#sim_world{
        peers = #{ ?PEER_UNLIMITED => Peer }
    }),
    %% At 400 chunks/s and four-second latency, 1600 productive requests fill
    %% the path. One hundred ticks let the peer discover that depth despite recurring
    %% failures; twenty measured ticks contain about 8000 requests and 200
    %% errors at full rate, enough to smooth their one-in-forty cadence.
    ar_sync_sim_runner:run_for(100),
    Measurement = ar_sync_sim_runner:run_for(20),
    assert_greater_than(
        ar_sync_sim_runner:metric(
            {client_errors_by_peer, ?PEER_UNLIMITED}, Measurement),
        0,
        #{ peer => ?PEER_UNLIMITED, metric => client_errors_by_peer }),
    assert_metric_utilization(stored_cps, MaxServeCPS, Measurement).

%% A single peer serving everything (the sync-from-one-local-peer
%% workflow): with nothing else to compete, the peer must be saturated —
%% its concurrency cap grows while work is queued until it covers the
%% concurrency the round-trip latency requires, and throughput reaches
%% the peer's full rate.
%%
%% Expected over the 10-tick measurement window:
%%   - the peer's rate: 400 cps at 250 ms latency needs 100 concurrent
%%     fetches; the cap covers that within warmup, so the peer serves
%%     its full 400 cps.
single_peer_saturation_test_() ->
    ar_sync_sim_runner:setup_sim(fun test_single_peer_saturation/0, 300).

test_single_peer_saturation() ->
    MaxServeCps = 400,
    Peers = #{ ?PEER_UNLIMITED => #sim_peer{ max_serve_cps = MaxServeCps } },
    %% Retain one end-to-end scenario with the adversarial alternating-chunk
    %% local layout; other scenarios model the contiguous need of a fresh store.
    ar_sync_sim_runner:start_sim(#sim_world{ peers = Peers, local_data_layout = fragmented }),
    %% Forty simulated seconds allow the fragmented workload to reach full rate.
    ar_sync_sim_runner:run_for(40),
    %% Ten seconds contain 4000 chunks at full rate, enough to average the
    %% fragmented store layout without extending the scheduler warmup.
    Measurement = ar_sync_sim_runner:run_for(10),
    assert_metric_utilization(stored_cps, MaxServeCps, Measurement).

%% Contract: a peer near its outbound quota remains usable when it is the only
%% peer holding needed data. Request-layer throttling may delay fetches, but
%% peer selection must not stop all progress.
single_throttled_peer_keeps_progressing_test_() ->
    ar_sync_sim_runner:setup_sim(
        fun test_single_throttled_peer_keeps_progressing/0, 300).

test_single_throttled_peer_keeps_progressing() ->
    MaxServeCps = 100,
    Peers = #{
        ?PEER_UNLIMITED => #sim_peer{
            max_serve_cps = MaxServeCps,
            selection_throttled = true
        }
    },
    ar_sync_sim_runner:start_sim(#sim_world{ peers = Peers }),
    %% Ten seconds cover the fixed sweep-range warm wait; ten more let the
    %% scheduler grow the only peer's request cap before measurement.
    ar_sync_sim_runner:run_for(20),
    Measurement = ar_sync_sim_runner:run_for(20),
    assert_metric_utilization(stored_cps, MaxServeCps, Measurement).

%% Contract: a high-latency peer can use nearly all of its declared capacity.
%% The assignment horizon bounds work waiting for the future, but must not cap
%% requests that are already fetching when a productive peer takes longer than
%% that horizon to respond.
high_latency_single_peer_saturation_test_() ->
    ar_sync_sim_runner:setup_sim(
        fun test_high_latency_single_peer_saturation/0, 400).

test_high_latency_single_peer_saturation() ->
    MaxServeCps = 440,
    %% Fifteen 250ms network substeps model a 3.75-second request path. At
    %% 440 chunks/s the peer needs roughly 1650 concurrent requests to use all
    %% of its declared capacity.
    LatencyMS = 15 * ?SIM_SUBSTEP_MS,
    Peers = #{
        ?PEER_UNLIMITED => #sim_peer{
            max_serve_cps = MaxServeCps,
            latency_ms = LatencyMS
        }
    },
    %% Exercise the production scheduler cadence. A one-second scheduler tick
    %% would fold several driven-zero samples into a 3.75-second request even
    %% though production observes it within one ten-second interval.
    ar_sync_scheduler:override_tick_interval_ms(10_000),
    ar_sync_sim_runner:start_sim(#sim_world{ peers = Peers }),
    %% Five simulated minutes require recovery well before the hours-long
    %% underutilization this scenario protects against while covering several
    %% scheduler evidence horizons.
    ar_sync_sim_runner:run_for(30),
    %% Twenty seconds contain five complete four-second pipeline periods and
    %% 8800 chunks at full capacity. The 5% margin is 440 chunks, so one
    %% full-rate second may cross a measurement boundary without hiding a
    %% sustained capacity loss.
    Measurement = ar_sync_sim_runner:run_for(2),
    assert_metric_utilization(stored_cps, MaxServeCps, Measurement).

%% Contract: a productive peer remains near its serving capacity when request
%% latency rises as the request pipeline deepens. The scheduler must continue
%% exploring instead of treating the current rate-latency product as a ceiling.
load_dependent_latency_preserves_single_peer_throughput_test_() ->
    ar_sync_sim_runner:setup_sim(
        fun test_load_dependent_latency_preserves_single_peer_throughput/0,
        400).

test_load_dependent_latency_preserves_single_peer_throughput() ->
    MaxServeCPS = 440,
    MinimumLatencyMS = 4 * ?SIM_SUBSTEP_MS,
    LoadedLatencyMS = 20 * ?SIM_SUBSTEP_MS,
    %% Three milliseconds of queueing per concurrent request grows a one-second
    %% idle path to a five-second loaded path at about 1350 requests.
    QueueDelayPerRequestMS = 3,
    Latency = fun(NumInflight) ->
        min(LoadedLatencyMS,
            MinimumLatencyMS + NumInflight * QueueDelayPerRequestMS)
    end,
    Peer = #sim_peer{
        max_serve_cps = MaxServeCPS,
        latency_ms = Latency
    },
    %% Use the production scheduler period so this scenario isolates whether
    %% measured goodput can probe beyond its current concurrency-rate fixed
    %% point, rather than testing a simulation-only sampling cadence.
    ar_sync_scheduler:override_tick_interval_ms(10_000),
    ar_sync_sim_runner:start_sim(#sim_world{
        peers = #{?PEER_UNLIMITED => Peer}
    }),
    %% Five simulated minutes cover several scheduler evidence horizons after
    %% the request path crosses into its loaded latency.
    ar_sync_sim_runner:run_for(30),
    %% Forty seconds cover four production control periods, so a one-tick probe
    %% cannot hide a lower sustained operating point.
    Measurement = ar_sync_sim_runner:run_for(4),
    assert_metric_utilization(stored_cps, MaxServeCPS, Measurement).

%% Contract: syncing scales with a peer whose serving capacity increases
%% fourfold, then retains that capacity when response latency increases
%% fourfold. The first transition requires using newly available capacity; the
%% second requires growing concurrency without a serving-rate change.
peer_capacity_and_latency_growth_test_() ->
    ar_sync_sim_runner:setup_sim(
        fun test_peer_capacity_and_latency_growth/0, 400).

test_peer_capacity_and_latency_growth() ->
    InitialServeCPS = 100,
    IncreasedServeCPS = 400,
    InitialLatencyMS = 2 * ?SIM_SUBSTEP_MS,
    IncreasedLatencyMS = 4 * InitialLatencyMS,
    Peer = #sim_peer{
        max_serve_cps = InitialServeCPS,
        latency_ms = InitialLatencyMS
    },
    Peers = #{ ?PEER_UNLIMITED => Peer },
    World = #sim_world{ peers = Peers },
    ar_sync_sim_runner:start_sim(World),
    %% Forty ticks establish the initial 100 chunks/s operating state.
    ar_sync_sim_runner:run_for(40),
    FasterPeer = Peer#sim_peer{ max_serve_cps = IncreasedServeCPS },
    ar_sync_sim_runner:update_world(World#sim_world{ peers = Peers#{
        ?PEER_UNLIMITED := FasterPeer
    } }),
    %% Ten ticks allow the scheduler to use the fourfold capacity increase.
    ar_sync_sim_runner:run_for(10),
    CapacityMeasurement = ar_sync_sim_runner:run_for(20),
    assert_metric_utilization(stored_cps, IncreasedServeCPS, CapacityMeasurement),
    ar_sync_sim_runner:update_world(World#sim_world{ peers = Peers#{
        ?PEER_UNLIMITED := FasterPeer#sim_peer{
            latency_ms = IncreasedLatencyMS
        }
    } }),
    %% At 400 chunks/s and two-second latency, the path needs about 800
    %% concurrent requests. Twenty ticks bound the request-depth recovery.
    ar_sync_sim_runner:run_for(10),
    LatencyMeasurement = ar_sync_sim_runner:run_for(10),
    assert_metric_utilization(stored_cps, IncreasedServeCPS, LatencyMeasurement).

%% Contract: syncing recovers from a client-side HTTP failure episode. Once the
%% per-peer HTTP in-flight request limit can accommodate the peer's
%% bandwidth-delay product, useful throughput recovers.
client_error_recovery_with_http_headroom_test_() ->
    ar_sync_sim_runner:setup_sim(
        fun test_client_error_recovery_with_http_headroom/0, 400).

test_client_error_recovery_with_http_headroom() ->
    MaxServeCps = 400,
    Peer = #sim_peer{ max_serve_cps = MaxServeCps, http_inflight_limit = 0 },
    Peers = #{ ?PEER_UNLIMITED => Peer },
    World = #sim_world{ peers = Peers },
    ar_sync_sim_runner:start_sim(World),
    %% Twelve one-second ticks cover coarse discovery, the ten-second sweep-range
    %% warm wait, and time to issue requests against the zero-connection limit.
    ForcedMeasurement = ar_sync_sim_runner:run_for(12),
    assert_greater_than(
        ar_sync_sim_runner:metric(
            {client_errors_by_peer, ?PEER_UNLIMITED}, ForcedMeasurement),
        0,
        #{ peer => ?PEER_UNLIMITED, metric => client_errors_by_peer }),
    %% At 400 chunks/s and 250 ms latency, about 100 concurrent requests fill
    %% the path. A limit of 110 permits full throughput with modest headroom.
    ar_sync_sim_runner:update_world(World#sim_world{ peers = Peers#{
        ?PEER_UNLIMITED := Peer#sim_peer{ http_inflight_limit = 110 }
    } }),
    %% Twenty ticks are well beyond the short client-error recovery memory.
    ar_sync_sim_runner:run_for(20),
    Measurement = ar_sync_sim_runner:run_for(20),
    Errors = ar_sync_sim_runner:metric(
        {client_errors_by_peer, ?PEER_UNLIMITED}, Measurement),
    Served = ar_sync_sim_runner:metric(
        {served_by_peer, ?PEER_UNLIMITED}, Measurement),
    assert_metric_utilization(stored_cps, MaxServeCps, Measurement),
    %% Client errors and successes have equal latency in this scenario. Failures
    %% must not consume more request time than useful delivery.
    assert_at_most(Errors, Served,
        #{ peer => ?PEER_UNLIMITED, metric => client_errors_by_peer }).

%% A mixed peer population includes fast and high-latency peers, a peer with
%% rate limiting enabled, and a flaky peer. It delivers near aggregate capacity
%% with every class contributing. The high-latency peer needs a deep cap: the
%% pipeline ceiling rises with measured goodput until it covers the peer's
%% bandwidth-delay product. The peer with rate limiting enabled reaches a stable
%% operating point as HTTP 429 responses reduce its cap and successful requests
%% allow it to grow again. The flaky peer's periodic timeout windows must not
%% stop it being allocated work.
%%
%% Expected rates over the 20-tick measurement window:
%%   - aggregate: only the flaky peer misses its rate, so ~520 of the
%%     540 cps capacity (0.96 x).
%%   - slow peer and peer with rate limiting enabled: their full 100 and 60 cps.
%%   - flaky peer: timed out for 3 of every 20 ticks (15%), and each
%%     window's timeout cuts leave ~2 more ticks of concurrency-cap
%%     regrowth below rate (~10%): ~0.75 x 80 cps.
mixed_peer_classes_test_() ->
    ar_sync_sim_runner:setup_sim(fun test_mixed_peer_classes/0, 400).

test_mixed_peer_classes() ->
    SlowCps = 100,
    ThrottledCps = 60,
    FlakyCps = 80,
    Peers = #{
        ?PEER_UNLIMITED => #sim_peer{ max_serve_cps = 300 },
        ?PEER_SLOW => #sim_peer{ max_serve_cps = SlowCps, latency_ms = 1000 },
        ?PEER_LIMITED_1 =>
            #sim_peer{ max_serve_cps = ThrottledCps, limited = true },
        ?PEER_FLAKY => #sim_peer{
            max_serve_cps = FlakyCps,
            latency_ms = 500,
            limited = true,
            failure_policy = fun(Tick, _RequestSequence) ->
                case (Tick rem 20) < 3 of
                    true -> timeout;
                    false -> none
                end
            end
        }
    },
    ar_sync_sim_runner:start_sim(#sim_world{ peers = Peers }),
    %% The flaky peer times out for 3 seconds of every 20.
    %% Forty simulated seconds cover two complete flaky-peer cycles.
    ar_sync_sim_runner:run_for(40),
    %% Twenty measured ticks cover one complete flaky-peer cycle. Starting
    %% after two complete warmup cycles keeps all three outage ticks in-window.
    Measurement = ar_sync_sim_runner:run_for(20),
    CpsByPeer = ar_sync_sim_runner:metric(cps_by_peer, Measurement),
    %% The flaky peer is unavailable for 3 of every 20 seconds, reducing the
    %% population's available capacity from 540 to 528 chunks/s.
    AvailableCPS = capacity(Peers) - FlakyCps * 3 / 20,
    assert_metric_utilization(stored_cps, AvailableCPS, Measurement),
    %% The healthy high-latency peer reaches its full rate.
    assert_at_least(
        maps:get(?PEER_SLOW, CpsByPeer),
        ?MIN_STEADY_STATE_UTILIZATION * SlowCps,
        #{ peer => ?PEER_SLOW, metric => cps }),
    %% Ongoing limit probes consume some requests. Requiring 90% keeps this peer
    %% useful while the aggregate 95% floor above guards total throughput.
    assert_at_least(
        maps:get(?PEER_LIMITED_1, CpsByPeer),
        0.9 * ThrottledCps,
        #{ peer => ?PEER_LIMITED_1, metric => cps }),
    %% ~0.75 x expected; 0.6 leaves recovery-tail margin.
    assert_at_least(
        maps:get(?PEER_FLAKY, CpsByPeer),
        0.6 * FlakyCps,
        #{ peer => ?PEER_FLAKY, metric => cps }).

%% Contract: when a peer's capacity falls sharply, the sync pipeline adapts
%% without leaving sibling peers starved behind work assigned at the old rate.
%% The changed peer remains useful and settles near its new capacity.
fast_peer_turns_slow_test_() ->
    ar_sync_sim_runner:setup_sim(fun test_fast_peer_turns_slow/0, 300).

test_fast_peer_turns_slow() ->
    Peer = #sim_peer{ max_serve_cps = 300 },
    Peers = #{
        ?PEER_UNLIMITED => Peer,
        ?PEER_LIMITED_1 => #sim_peer{ max_serve_cps = 60, limited = true },
        ?PEER_LIMITED_2 => #sim_peer{ max_serve_cps = 60, limited = true },
        ?PEER_LIMITED_3 => #sim_peer{ max_serve_cps = 60, limited = true }
    },
    World = #sim_world{ peers = Peers },
    ar_sync_sim_runner:start_sim(World),
    %% Establish the peer's high-rate state before reducing its serving capacity.
    ar_sync_sim_runner:run_for(32),
    CollapsedCps = 10,
    %% Five hundred milliseconds models a higher-latency slowdown where queued
    %% requests retain connections while completing at the peer's lower rate.
    CollapsedLatencyMS = 500,
    ar_sync_sim_runner:update_world(World#sim_world{ peers = Peers#{
        ?PEER_UNLIMITED := Peer#sim_peer{
            max_serve_cps = CollapsedCps,
            latency_ms = CollapsedLatencyMS
        }
    } }),
    %% Twenty ticks permit stale work to finish and the short EWMAs to settle.
    ar_sync_sim_runner:run_for(20),
    Measurement = ar_sync_sim_runner:run_for(20),
    CpsByPeer = ar_sync_sim_runner:metric(cps_by_peer, Measurement),
    %% Three unchanged 60 chunks/s peers plus the changed 10 chunks/s peer
    %% provide 190 chunks/s of sustainable capacity.
    assert_metric_utilization(stored_cps, 3 * 60 + CollapsedCps, Measurement),
    MinimumUnchangedPeerCPS = ?MIN_STEADY_STATE_UTILIZATION * 60,
    assert_at_least(
        maps:get(?PEER_LIMITED_1, CpsByPeer),
        MinimumUnchangedPeerCPS,
        #{ peer => ?PEER_LIMITED_1, metric => cps }),
    assert_at_least(
        maps:get(?PEER_LIMITED_2, CpsByPeer),
        MinimumUnchangedPeerCPS,
        #{ peer => ?PEER_LIMITED_2, metric => cps }),
    assert_at_least(
        maps:get(?PEER_LIMITED_3, CpsByPeer),
        MinimumUnchangedPeerCPS,
        #{ peer => ?PEER_LIMITED_3, metric => cps }),
    CollapsedPeerCps = maps:get(?PEER_UNLIMITED, CpsByPeer),
    assert_less_than(CollapsedPeerCps, CollapsedCps + 1,
        #{ peer => ?PEER_UNLIMITED, metric => cps }),
    assert_at_least(
        CollapsedPeerCps,
        ?MIN_STEADY_STATE_UTILIZATION * CollapsedCps,
        #{ peer => ?PEER_UNLIMITED, metric => cps }).

%% Contract: a peer unavailable for a prolonged period remains eligible for
%% exploration and returns to full useful service after it recovers. This test
%% observes requests and throughput rather than a particular cap or rating state.
starved_peer_recovers_test_() ->
    ar_sync_sim_runner:setup_sim(fun test_starved_peer_recovers/0, 300).

test_starved_peer_recovers() ->
    RecoveredCps = 80,
    Peers = #{
        ?PEER_UNLIMITED => #sim_peer{ max_serve_cps = 400 },
        ?PEER_TIMEOUT => #sim_peer{
            max_serve_cps = RecoveredCps,
            limited = true,
            %% Tick indices are zero-based, so 0..89 is exactly 90 ticks.
            failure_policy = fun(Tick, _RequestSequence) ->
                case Tick < 90 of
                    true -> timeout;
                    false -> none
                end
            end
        }
    },
    ar_sync_sim_runner:start_sim(#sim_world{ peers = Peers }),
    %% Eighty-nine ticks are intentionally much longer than the scheduler's
    %% normal adaptation windows while ending strictly before tick 90 recovers.
    OutageMeasurement = ar_sync_sim_runner:run_for(60),
    Timeouts = ar_sync_sim_runner:metric(
        {timed_out_by_peer, ?PEER_TIMEOUT}, OutageMeasurement),
    assert_greater_than(Timeouts, 0,
        #{ peer => ?PEER_TIMEOUT, metric => timed_out_by_peer }),
    StarvedCps = ar_sync_sim_runner:metric(
        {cps_by_peer, ?PEER_TIMEOUT}, ar_sync_sim_runner:run_for(29)),
    assert_less_than(StarvedCps, 1,
        #{ peer => ?PEER_TIMEOUT, metric => cps }),
    %% Tick 90 begins recovery. Forty control intervals let the recovered
    %% subsecond path settle; twenty more average across dispatch/refill
    %% boundaries. The 600 production-equivalent seconds remain far below the
    %% multi-hour recovery failure this scenario guards against.
    ar_sync_sim_runner:run_for(41),
    Measurement = ar_sync_sim_runner:run_for(20),
    CpsByPeer = ar_sync_sim_runner:metric(cps_by_peer, Measurement),
    %% The always-healthy 400 chunks/s peer and recovered 80 chunks/s peer
    %% provide 480 chunks/s after the outage.
    assert_metric_utilization(stored_cps, 400 + RecoveredCps, Measurement),
    RecoveredPeerCps = maps:get(?PEER_TIMEOUT, CpsByPeer),
    %% A 90% peer floor distinguishes useful recovery from residual starvation;
    %% the aggregate 95% assertion above catches broader throughput loss.
    assert_at_least(
        RecoveredPeerCps,
        0.9 * RecoveredCps,
        #{ peer => ?PEER_TIMEOUT, metric => cps }).

%% Contract: when writes stall for every store, fetch throughput stops before
%% pending writes grow without bound. Once writes resume, useful peer throughput
%% recovers without a long-lived allocation penalty.
slow_disk_write_recovery_test_() ->
    ar_sync_sim_runner:setup_sim(fun test_slow_disk_write_recovery/0, 600).

test_slow_disk_write_recovery() ->
    Peers = #{
        ?PEER_UNLIMITED => #sim_peer{ max_serve_cps = 250 },
        ?PEER_LIMITED_1 => #sim_peer{ max_serve_cps = 60, limited = true },
        ?PEER_LIMITED_2 => #sim_peer{ max_serve_cps = 60, limited = true },
        ?PEER_LIMITED_3 => #sim_peer{ max_serve_cps = 40, limited = true },
        ?PEER_LIMITED_4 => #sim_peer{ max_serve_cps = 40, limited = true },
        ?PEER_LIMITED_5 => #sim_peer{ max_serve_cps = 20, limited = true }
    },
    World = #sim_world{ peers = Peers },
    ar_sync_sim_runner:start_sim(World),
    %% Forty warmup ticks cover the 32-tick scheduler horizon.
    ar_sync_sim_runner:run_for(40),
    ar_sync_sim_runner:update_world(World#sim_world{
        store_write_cps = 0
    }),
    %% At 470 chunks/s the 7200-chunk cache represents about 15.3 seconds of
    %% delivered work. Thirty-two ticks cover twice that bounded cache-fill horizon
    %% before the following five-tick frozen-state observation.
    ar_sync_sim_runner:run_for(32),
    %% Five additional ticks measure the stalled state. Five percent permits at
    %% most 23.5 chunks/s of stragglers from the 470 chunks/s peer capacity.
    FrozenMeasurement = ar_sync_sim_runner:run_for(5),
    FrozenCps = lists:sum(maps:values(
        ar_sync_sim_runner:metric(cps_by_peer, FrozenMeasurement))),
    assert_at_most(FrozenCps, 0.05 * capacity(Peers)),
    ar_sync_sim_runner:update_world(World),
    %% Five ticks allow the delayed writes to complete and fetching to restart.
    ar_sync_sim_runner:run_for(5),
    %% Twenty ticks provide 400 chunks from the slowest 20 chunks/s peer. Its 5%
    %% margin is 20 chunks, allowing one one-second capacity bucket to cross the
    %% measurement boundary without hiding persistent post-stall starvation.
    Measurement = ar_sync_sim_runner:run_for(20),
    CpsByPeer = ar_sync_sim_runner:metric(cps_by_peer, Measurement),
    assert_metric_utilization(stored_cps, capacity(Peers), Measurement),
    %% A peer queue left starved after recovery would remain near zero. Each
    %% peer must return to the same 95% steady-state utilization contract.
    Minimum60CPS = ?MIN_STEADY_STATE_UTILIZATION * 60,
    assert_at_least(
        maps:get(?PEER_LIMITED_1, CpsByPeer),
        Minimum60CPS,
        #{ peer => ?PEER_LIMITED_1, metric => cps }),
    assert_at_least(
        maps:get(?PEER_LIMITED_2, CpsByPeer),
        Minimum60CPS,
        #{ peer => ?PEER_LIMITED_2, metric => cps }),
    Minimum40CPS = ?MIN_STEADY_STATE_UTILIZATION * 40,
    assert_at_least(
        maps:get(?PEER_LIMITED_3, CpsByPeer),
        Minimum40CPS,
        #{ peer => ?PEER_LIMITED_3, metric => cps }),
    assert_at_least(
        maps:get(?PEER_LIMITED_4, CpsByPeer),
        Minimum40CPS,
        #{ peer => ?PEER_LIMITED_4, metric => cps }),
    assert_at_least(
        maps:get(?PEER_LIMITED_5, CpsByPeer),
        ?MIN_STEADY_STATE_UTILIZATION * 20,
        #{ peer => ?PEER_LIMITED_5, metric => cps }).

%% Contract: one store with stalled writes cannot fill the shared cache or
%% prevent the healthy stores from using the available peer capacity.
single_slow_store_isolation_test_() ->
    ar_sync_sim_runner:setup_sim(fun test_single_slow_store_isolation/0, 600).

test_single_slow_store_isolation() ->
    Peers = #{
        ?PEER_UNLIMITED => #sim_peer{ max_serve_cps = 250 },
        ?PEER_LIMITED_1 => #sim_peer{ max_serve_cps = 60, limited = true },
        ?PEER_LIMITED_2 => #sim_peer{ max_serve_cps = 60, limited = true },
        ?PEER_LIMITED_3 => #sim_peer{ max_serve_cps = 40, limited = true }
    },
    %% A 564 MiB sync budget resolves to a 2000-chunk fetched cache. Without
    %% per-store isolation, the stalled store's roughly one-sixth share of the
    %% 410 chunks/s workload would fill it in about 29 seconds.
    World = #sim_world{
        peers = Peers,
        node_config = #{ [sync, cache_size] => 564 }
    },
    ar_sync_sim_runner:start_sim(World),
    [SlowStore | _] = ar_sync_sim_world:store_ids(),
    ar_sync_sim_runner:update_world(World#sim_world{
        store_write_cps = fun(Store, _Tick) ->
            case Store =:= SlowStore of
                true -> 0;
                false -> infinity
            end
        end
    }),
    %% Forty ticks exceed both the 29-second failure horizon and the
    %% scheduler's 32-tick adaptation period.
    ar_sync_sim_runner:run_for(40),
    ?assertNot(ar_sync_deps:is_chunk_cache_full()),
    %% Ten ticks provide at least 400 chunks at the smallest 40 chunks/s rate.
    Measurement = ar_sync_sim_runner:run_for(10),
    %% The healthy stores must absorb nearly all of the peers' 410 chunks/s
    %% capacity. A 5/6 store split reaches only about 83% and fails this floor.
    assert_metric_utilization(stored_cps, capacity(Peers), Measurement),
    ?assertNot(ar_sync_deps:is_chunk_cache_full()).

%% Contract: stores that write slowly but never stall cannot together hold
%% enough of the shared chunk cache to stop the healthy stores from using the
%% peers' full capacity.
%%
%% Several slow stores are required, not one: the single-stalled-store case is
%% covered separately. Production combines a claim horizon derived from each
%% store's measured write rate with reserve-only dispatch near global pressure.
%% Four accumulating stores exercise the aggregate path where losing both
%% protections lets them fill the shared cache and block healthy work.
slow_stores_do_not_monopolize_cache_test_() ->
    ar_sync_sim_runner:setup_sim(
        fun test_slow_stores_do_not_monopolize_cache/0, 600).

test_slow_stores_do_not_monopolize_cache() ->
    Peers = #{
        ?PEER_UNLIMITED => #sim_peer{ max_serve_cps = 250 },
        ?PEER_LIMITED_1 => #sim_peer{ max_serve_cps = 60, limited = true },
        ?PEER_LIMITED_2 => #sim_peer{ max_serve_cps = 60, limited = true },
        ?PEER_LIMITED_3 => #sim_peer{ max_serve_cps = 40, limited = true }
    },
    %% 564 MiB resolves to a 2000-chunk fetched cache: large enough to carry the
    %% peers' 410 chunks/s when every store is healthy, so a shortfall measures
    %% the slow stores rather than the cache.
    World = #sim_world{
        peers = Peers,
        node_config = #{ [sync, cache_size] => 564 }
    },
    ar_sync_sim_runner:start_sim(World),
    {SlowStores, _Healthy} = lists:split(4, ar_sync_sim_world:store_ids()),
    %% Five chunks/s is slow enough to accumulate but fast enough that no store
    %% ever looks stalled to the cache-full guard.
    ar_sync_sim_runner:update_world(World#sim_world{
        store_write_cps = fun(Store, _Tick) ->
            case lists:member(Store, SlowStores) of
                true -> 5;
                false -> infinity
            end
        end
    }),
    %% Without both store-rate and reserve protections, the 2000-chunk cache can
    %% fill well inside forty ticks. This also clears the scheduler's 32-tick
    %% horizon.
    ar_sync_sim_runner:run_for(40),
    ?assertNot(ar_sync_deps:is_chunk_cache_full()),
    %% Ten seconds contain at least 400 chunks at the smallest 40 chunks/s rate.
    Measurement = ar_sync_sim_runner:run_for(10),
    assert_metric_utilization(stored_cps, capacity(Peers), Measurement),
    ?assertNot(ar_sync_deps:is_chunk_cache_full()).

%% Contract: several slower store ranges cannot monopolize the shared chunk
%% cache while other store paths are temporarily unavailable. When two
%% healthy peer/store paths become available, each must promptly sustain its
%% own 100 chunks/s serving capacity.
shared_slow_device_isolation_test_() ->
    ar_sync_sim_runner:setup_sim(fun test_shared_slow_device_isolation/0, 600).

test_shared_slow_device_isolation() ->
    StoreRanges = ar_sync_sim_world:store_ranges(),
    PeerCPS = 50,
    PeerStoreRanges = lists:zip(
        lists:seq(1, length(StoreRanges)), StoreRanges),
    {SlowPeerStoreRanges, HealthyPeerStoreRanges} =
        lists:split(4, PeerStoreRanges),
    SlowStores = [StoreID || {_N, {StoreID, _Range}} <- SlowPeerStoreRanges],
    Peers = maps:from_list([
        {{10, 2, 0, N, 1984}, #sim_peer{
            max_serve_cps = PeerCPS,
            sync_availability = {stores, [StoreID]}
        }}
        || {N, {StoreID, _Range}} <- PeerStoreRanges
    ]),
    HealthyPeerIDs = [
        {10, 2, 0, N, 1984}
        || {N, _StoreRange} <- HealthyPeerStoreRanges
    ],
    InitiallyUnavailablePeers = maps:map(
        fun(PeerID, Peer) ->
            case lists:member(PeerID, HealthyPeerIDs) of
                true ->
                    %% Zero HTTP slots keeps the path unavailable without
                    %% withholding its metadata from the sweeper.
                    Peer#sim_peer{ http_inflight_limit = 0 };
                false ->
                    Peer
            end
        end,
        Peers),
    %% Four independent peer/store paths first warm at 50 chunks/s, then their
    %% stores stall. The other two peer/store paths begin accepting requests only
    %% after the warm peers have had enough time to fill the shared cache.
    HealthyStores = [StoreID || {_N, {StoreID, _Range}} <- HealthyPeerStoreRanges],
    World = #sim_world{
        peers = InitiallyUnavailablePeers,
        %% A 564 MiB sync budget resolves to a 2000-chunk fetched cache. Four
        %% old one-quarter claim allowances can fill it. The six storage modules
        %% give each low-pressure path a 2000 / 6 = 333-chunk eligibility
        %% threshold.
        node_config = #{ [sync, cache_size] => 564 }
    },
    ar_sync_sim_runner:start_sim(World),
    %% Twenty ticks let each peer demonstrate its full serving capacity before
    %% its store stalls.
    ar_sync_sim_runner:run_for(20),
    StalledWorld = World#sim_world{
        store_write_cps = fun(StoreID, _Tick) ->
            case lists:member(StoreID, SlowStores) of
                true -> 0;
                false -> infinity
            end
        end
    },
    ar_sync_sim_runner:update_world(StalledWorld),
    %% Four peers at 50 chunks/s provide 200 chunks/s, so ten ticks can consume
    %% their combined 2000-chunk old claim allowance.
    ar_sync_sim_runner:run_for(10),
    ar_sync_sim_runner:update_world(StalledWorld#sim_world{ peers = Peers }),
    %% Forty ticks let the newly available paths' caps rise from the exploration
    %% floor before measuring.
    ar_sync_sim_runner:run_for(40),
    %% Ten seconds contain 500 chunks from each healthy 50 chunks/s path.
    Measurement = ar_sync_sim_runner:run_for(10),
    ProgressByStore = ar_sync_sim_runner:metric(
        chunks_stored_by_store, Measurement),
    MeasurementSeconds = ar_sync_sim_runner:metric(
        duration_seconds, Measurement),
    HealthyStoredCPS = lists:sum([
        maps:get(StoreID, ProgressByStore, 0)
        || StoreID <- HealthyStores
    ]) / MeasurementSeconds,
    ExpectedHealthyCPS = length(HealthyStores) * PeerCPS,
    %% The settled healthy paths must use nearly all of their independent
    %% serving capacity.
    assert_at_least(
        HealthyStoredCPS,
        ?MIN_STEADY_STATE_UTILIZATION * ExpectedHealthyCPS,
        #{ metric => stored_cps }),
    %% Each healthy store must contribute materially; one quarter of an even
    %% two-store split is 25 chunks/s and rejects one-store concentration.
    MinimumHealthyStoreCPS =
        0.25 * ExpectedHealthyCPS / length(HealthyStores),
    lists:foreach(
        fun(StoreID) ->
            StoredCPS = maps:get(StoreID, ProgressByStore, 0)
                / MeasurementSeconds,
            assert_at_least(StoredCPS, MinimumHealthyStoreCPS,
                #{ store => StoreID, metric => stored_cps })
        end,
        HealthyStores).

%% Contract: a very low configured download rate is sustained even when all
%% current work completes before enough capacity is available for the next
%% fetch. The result must not depend on a particular wakeup mechanism.
bandwidth_cap_trickle_test_() ->
    ar_sync_sim_runner:setup_sim(fun test_bandwidth_cap_trickle/0, 300).

test_bandwidth_cap_trickle() ->
    TrickleCps = 2,
    Peers = #{ ?PEER_UNLIMITED => #sim_peer{ max_serve_cps = 400 } },
    ar_sync_sim_runner:start_sim(#sim_world{
        peers = Peers,
        node_config = #{ [sync, max_download_rate] => 2 * ?DATA_CHUNK_SIZE }
    }),
    %% Forty ticks cover the scheduler horizon; thirty ticks average a two chunks/s
    %% rate whose individual dispatches are intentionally sparse.
    ar_sync_sim_runner:run_for(40),
    MeasurementTicks = 30,
    Measurement = ar_sync_sim_runner:run_for(MeasurementTicks),
    ServedCPS = lists:sum(maps:values(
        ar_sync_sim_runner:metric(cps_by_peer, Measurement))),
    %% Boundary snapshots can include one additional second of completions.
    MaxObservedCPS = TrickleCps * (MeasurementTicks + 1) / MeasurementTicks,
    assert_at_most(ServedCPS, MaxObservedCPS),
    assert_metric_utilization(stored_cps, TrickleCps, Measurement).

%% Contract: failed fetch attempts do not consume the configured delivered-byte
%% rate when healthy capacity is available. With one peer timing out for half
%% of each ten-tick cycle, aggregate useful throughput must still reach at least
%% 95% of the 100 chunks/s node-wide limit. If failed attempts consumed the
%% limit and requests were split evenly, the healthy peer would deliver 50
%% chunks/s and the flaky peer 25 chunks/s, for only 75 chunks/s total.
bandwidth_cap_with_failures_test_() ->
    ar_sync_sim_runner:setup_sim(fun test_bandwidth_cap_with_failures/0, 300).

test_bandwidth_cap_with_failures() ->
    BudgetCps = 100,
    Peers = #{
        ?PEER_UNLIMITED => #sim_peer{ max_serve_cps = 400 },
        ?PEER_FLAKY => #sim_peer{
            max_serve_cps = 400,
            failure_policy = fun(Tick, _RequestSequence) ->
                case (Tick rem 10) < 5 of
                    true -> timeout;
                    false -> none
                end
            end
        }
    },
    ar_sync_sim_runner:start_sim(#sim_world{
        peers = Peers,
        node_config = #{ [sync, max_download_rate] => 100 * ?DATA_CHUNK_SIZE }
    }),
    %% Forty ticks cover the scheduler horizon and four complete fault cycles.
    WarmupTicks = 40,
    WarmupMeasurement = ar_sync_sim_runner:run_for(WarmupTicks),
    %% Two complete ten-tick fault cycles avoid phase-dependent results.
    MeasurementTicks = 20,
    Measurement = ar_sync_sim_runner:run_for(MeasurementTicks),
    ServedCPS = lists:sum(maps:values(
        ar_sync_sim_runner:metric(cps_by_peer, Measurement))),
    WarmupTimeouts = ar_sync_sim_runner:metric(
        {timed_out_by_peer, ?PEER_FLAKY}, WarmupMeasurement),
    MeasurementTimeouts = ar_sync_sim_runner:metric(
        {timed_out_by_peer, ?PEER_FLAKY}, Measurement),
    Timeouts = WarmupTimeouts + MeasurementTimeouts,
    ScenarioSeconds = (WarmupTicks + MeasurementTicks) *
        (ar_sync_scheduler:tick_interval_ms() div 1000),
    %% The old 100-timeout floor over 180 seconds required at least 0.56
    %% timeouts/s. The 0.5 floor preserves a material active fault load.
    TimeoutRate = Timeouts / ScenarioSeconds,
    assert_at_least(TimeoutRate, 0.5,
        #{ peer => ?PEER_FLAKY, metric => timeout_rate }),
    assert_less_than(ServedCPS, BudgetCps + 1),
    assert_metric_utilization(stored_cps, BudgetCps, Measurement).

%% Contract: a binding node-wide download limit is neither exceeded nor left
%% underutilized by a heterogeneous peer population. No peer monopolizes the
%% limit, and slow, rate-limited, and intermittently failing peers remain active.
mixed_peer_classes_rate_limited_test_() ->
    ar_sync_sim_runner:setup_sim(fun test_mixed_peer_classes_rate_limited/0, 400).

test_mixed_peer_classes_rate_limited() ->
    BudgetCps = 120,
    %% Twenty chunks/s sits below the peer's expected rotating share of the
    %% 120 chunks/s node budget, so the modeled 429 constraint must engage.
    LimitedServeCPS = 20,
    Peers = #{
        ?PEER_UNLIMITED => #sim_peer{ max_serve_cps = 300 },
        ?PEER_SLOW => #sim_peer{ max_serve_cps = 100, latency_ms = 1000 },
        ?PEER_LIMITED_1 => #sim_peer{
            max_serve_cps = LimitedServeCPS, limited = true },
        ?PEER_FLAKY => #sim_peer{
            max_serve_cps = 80,
            latency_ms = 500,
            limited = true,
            failure_policy = fun(Tick, _RequestSequence) ->
                case (Tick rem 20) < 3 of
                    true -> timeout;
                    false -> none
                end
            end
        }
    },
    ar_sync_sim_runner:start_sim(#sim_world{
        peers = Peers,
        node_config = #{ [sync, max_download_rate] => 120 * ?DATA_CHUNK_SIZE }
    }),
    %% Forty ticks cover the scheduler horizon and two flaky-peer cycles.
    WarmupMeasurement = ar_sync_sim_runner:run_for(40),
    Measurement = ar_sync_sim_runner:run_for(20),
    CpsByPeer = ar_sync_sim_runner:metric(cps_by_peer, Measurement),
    ServedCPS = lists:sum(maps:values(CpsByPeer)),
    WarmupTimeouts = ar_sync_sim_runner:metric(
        {timed_out_by_peer, ?PEER_FLAKY}, WarmupMeasurement),
    MeasurementTimeouts = ar_sync_sim_runner:metric(
        {timed_out_by_peer, ?PEER_FLAKY}, Measurement),
    Timeouts = WarmupTimeouts + MeasurementTimeouts,
    WarmupHTTP429s = ar_sync_sim_runner:metric(
        {rejected_by_peer, ?PEER_LIMITED_1}, WarmupMeasurement),
    MeasurementHTTP429s = ar_sync_sim_runner:metric(
        {rejected_by_peer, ?PEER_LIMITED_1}, Measurement),
    HTTP429s = WarmupHTTP429s + MeasurementHTTP429s,
    %% Both modeled constraints must engage during the scenario. HTTP 429
    %% responses may stop once a future scheduler learns the peer's rate limit.
    assert_greater_than(Timeouts, 0,
        #{ peer => ?PEER_FLAKY, metric => timed_out_by_peer }),
    assert_greater_than(HTTP429s, 0,
        #{ peer => ?PEER_LIMITED_1, metric => rejected_by_peer }),
    %% One chunk/s permits endpoint quantization while rejecting a sustained
    %% overshoot of the configured 120 chunks/s limit.
    assert_less_than(ServedCPS, BudgetCps + 1),
    assert_metric_utilization(stored_cps, BudgetCps, Measurement),
    %% No peer may consume more than 85% of the node-wide rate, and no class
    %% may be starved out of the rotation.
    %%
    %% A partial regression is caught by the stored-cps utilization assertion
    %% above: the peers together must still deliver 95% of the budget, and that
    %% aggregate is stable run to run. The split between peers is not, so these
    %% floors sit below the smallest useful rotating share. Deriving them from
    %% a typical share would test scheduling jitter rather than starvation.
    MaxPeerCPS = 0.85 * BudgetCps,
    %% The flaky peer times out three ticks in twenty and its exploration
    %% stream settles near one chunk/s, so its floor must sit below that. A
    %% floor of one lands exactly on the expected value, where a single chunk
    %% of jitter decides the run.
    MinimumFlakyCPS = 0.5,
    %% The other three hold whole-number shares; the lowest observed was the
    %% slow peer at 4.15 chunks/s.
    MinimumPeerCPS = 3,
    UnlimitedCPS = maps:get(?PEER_UNLIMITED, CpsByPeer),
    assert_at_most(UnlimitedCPS, MaxPeerCPS,
        #{ peer => ?PEER_UNLIMITED, metric => cps }),
    SlowCPS = maps:get(?PEER_SLOW, CpsByPeer),
    assert_at_most(SlowCPS, MaxPeerCPS,
        #{ peer => ?PEER_SLOW, metric => cps }),
    LimitedCPS = maps:get(?PEER_LIMITED_1, CpsByPeer),
    assert_at_most(LimitedCPS, MaxPeerCPS,
        #{ peer => ?PEER_LIMITED_1, metric => cps }),
    FlakyCPS = maps:get(?PEER_FLAKY, CpsByPeer),
    assert_at_most(FlakyCPS, MaxPeerCPS,
        #{ peer => ?PEER_FLAKY, metric => cps }),
    assert_at_least(UnlimitedCPS, MinimumPeerCPS,
        #{ peer => ?PEER_UNLIMITED, metric => cps }),
    assert_at_least(SlowCPS, MinimumPeerCPS,
        #{ peer => ?PEER_SLOW, metric => cps }),
    assert_at_least(LimitedCPS, MinimumPeerCPS,
        #{ peer => ?PEER_LIMITED_1, metric => cps }),
    assert_at_least(FlakyCPS, MinimumFlakyCPS,
        #{ peer => ?PEER_FLAKY, metric => cps }).

%% Contract: when one peer serves six independently limited store ranges,
%% requests are distributed across the ranges so their combined capacity is
%% usable. Each range serves 25 chunks/s, giving 150 chunks/s in aggregate;
%% concentrating work on one or two ranges would produce only 25-50 chunks/s.
single_peer_store_spread_test_() ->
    ar_sync_sim_runner:setup_sim(fun test_single_peer_store_spread/0, 400).

test_single_peer_store_spread() ->
    StoreCps = 25,
    %% At 25 chunks/s per store and two seconds of latency, each store needs
    %% about 50 concurrent requests to use its remote read capacity.
    Peers = #{ ?PEER_UNLIMITED => #sim_peer{
        max_serve_cps = 600,
        latency_ms = 2000
    } },
    ar_sync_sim_runner:start_sim(#sim_world{
        peers = Peers,
        remote_store_cps = StoreCps,
        %% Each store can write exactly as fast as its remote path can serve.
        %% This leaves transient writes pending without reducing the expected
        %% aggregate capacity of six stores at 25 chunks/s each.
        store_write_cps = StoreCps
    }),
    %% Forty simulated seconds allow work to spread across all stores.
    ar_sync_sim_runner:run_for(40),
    MeasurementTicks = 20,
    Measurement = ar_sync_sim_runner:run_for(MeasurementTicks),
    %% Equal per-store limits should produce an even split. A 90% floor around
    %% the average allows scheduling variation while directly rejecting skew.
    assert_chunks_spread_across_stores(Measurement),
    CpsByPeer = ar_sync_sim_runner:metric(cps_by_peer, Measurement),
    ServedCPS = lists:sum(maps:values(CpsByPeer)),
    AggregateStoreCps = ?SIM_STORES * StoreCps,
    %% Sampling can include completions at both endpoint seconds, so an N-second
    %% window may observe at most N+1 store-capacity buckets.
    MaxObservedCps = AggregateStoreCps * (MeasurementTicks + 1) / MeasurementTicks,
    assert_at_most(ServedCPS, MaxObservedCps),
    assert_metric_utilization(stored_cps, AggregateStoreCps, Measurement).

%% Contract: one footprint peer serving independently limited store ranges
%% uses every store's capacity. Repeated partial footprint assignments for one
%% store must not monopolize enough entropy slots to starve other stores.
single_footprint_peer_store_spread_test_() ->
    ar_sync_sim_runner:setup_sim(
        fun test_single_footprint_peer_store_spread/0, 500).

test_single_footprint_peer_store_spread() ->
    ar_sync_sim_world:use_mainnet_replica_2_9_sizes(),
    StoreCPS = 25,
    %% Six stores at 25 chunks/s provide 150 chunks/s. The peer's larger
    %% network limit ensures the store paths determine expected throughput.
    AggregateStoreCPS = ?SIM_STORES * StoreCPS,
    Peer = #sim_peer{
        max_serve_cps = 600,
        latency_ms = 2000,
        sync_kinds = [footprint]
    },
    ar_sync_sim_runner:start_sim(#sim_world{
        peers = #{?PEER_UNLIMITED => Peer},
        remote_store_cps = StoreCPS,
        store_write_cps = StoreCPS,
        node_config = #{
            %% Six active mainnet-sized footprints require 1536 MiB. Two GiB
            %% leaves room for one footprint per store plus transitions.
            [packing, entropy, cache_size] => 2048,
            [sync, cache_size] => 32768
        }
    }),
    %% Fifty seconds cover discovery and peer-cap growth. Sixty measurement
    %% seconds contain 1,500 writes per store at the expected serving rate;
    %% the longer window makes the fixed one-second endpoint buckets and
    %% footprint refill transitions less than the five-percent allowance.
    ar_sync_sim_runner:run_for(50),
    Measurement = ar_sync_sim_runner:run_for(60),
    assert_chunks_spread_across_stores(Measurement),
    assert_metric_utilization(stored_cps, AggregateStoreCPS, Measurement).

%% Contract: short, staggered pauses in individual store writes must not reduce
%% a peer's aggregate useful throughput or skew progress toward whichever stores
%% most recently wrote chunks. Every store retains the same average write capacity,
%% so all stores must progress fairly at their combined capacity.
single_footprint_peer_store_bursts_preserve_throughput_test_() ->
    ar_sync_sim_runner:setup_sim(
        fun test_single_footprint_peer_store_bursts_preserve_throughput/0, 500).

test_single_footprint_peer_store_bursts_preserve_throughput() ->
    ar_sync_sim_world:use_mainnet_replica_2_9_sizes(),
    StoreIDs = ar_sync_sim_world:store_ids(),
    StoreCount = length(StoreIDs),
    StoreCPS = 25,
    AggregateStoreCPS = StoreCount * StoreCPS,
    %% Divide the six stores into three phase pairs:
    %%   second 0: stores 0 and 3 write 75 chunks each
    %%   second 1: stores 1 and 4 write 75 chunks each
    %%   second 2: stores 2 and 5 write 75 chunks each
    %% Each store writes 75 chunks once every three seconds, averaging 25
    %% chunks/s, while one pair always writes and preserves the 150 chunks/s
    %% aggregate capacity. The two-second pauses are shorter than the modeled
    %% five-second buffering horizon, so they rotate per-store headroom without
    %% modeling a persistently slow store. Ten-second rate observations span
    %% several cycles and therefore see approximately the 25 chunks/s average.
    BurstPeriod = StoreCount div 2,
    BurstCPS = StoreCPS * BurstPeriod,
    StoreIndexes = maps:from_list(
        lists:zip(StoreIDs, lists:seq(0, StoreCount - 1))),
    StoreWriteCPS = fun(StoreID, Tick) ->
        StorePhase = maps:get(StoreID, StoreIndexes) rem BurstPeriod,
        case Tick rem BurstPeriod =:= StorePhase of
            true -> BurstCPS;
            false -> 0
        end
    end,
    Peer = #sim_peer{
        %% The six remote store paths, not the peer-wide network, set the
        %% expected 150 chunks/s ceiling.
        max_serve_cps = 600,
        latency_ms = 2000,
        sync_kinds = [footprint]
    },
    ar_sync_sim_runner:start_sim(#sim_world{
        peers = #{?PEER_UNLIMITED => Peer},
        %% Fifty remote chunks/s exceeds each store's 25 chunks/s sustainable
        %% write rate, ensuring writes accumulate during pauses and the rotating
        %% local headroom—not remote availability—is the bottleneck under test.
        remote_store_cps = 2 * StoreCPS,
        %% The phase function returns either 75 chunks/s for the active second
        %% or zero for the two paused seconds, averaging 25 chunks/s per store.
        store_write_cps = StoreWriteCPS,
        node_config = #{
            %% Six active mainnet footprints require 1536 MiB. Two GiB leaves
            %% room for one footprint per store plus transitions.
            [packing, entropy, cache_size] => 2048,
            [sync, cache_size] => 32768
        }
    }),
    %% Sixty one-second ticks cover metadata discovery and capacity growth. The
    %% 180-second measurement contains sixty complete three-second burst cycles
    %% and makes endpoint backlog less than five percent of total capacity.
    ar_sync_sim_runner:run_for(60),
    Measurement = ar_sync_sim_runner:run_for(180),
    assert_chunks_spread_across_stores(Measurement),
    assert_metric_utilization(stored_cps, AggregateStoreCPS, Measurement).

%% Contract: a productive high-latency peer must grow beyond its initial
%% request pipeline and sustain its serving capacity across all local stores.
high_latency_peer_preserves_store_throughput_test_() ->
    ar_sync_sim_runner:setup_sim(
        fun test_high_latency_peer_preserves_store_throughput/0, 500).

test_high_latency_peer_preserves_store_throughput() ->
    ar_sync_sim_world:use_mainnet_replica_2_9_sizes(),
    %% Use the production ten-second control interval. The peer takes five
    %% seconds to answer a request, so each delivery sample spans two complete
    %% response cycles. The default one-second simulation interval would record
    %% several samples with requests still in flight and no completions, followed
    %% by a burst of completions. That phase-dependent pattern would test the
    %% shortened simulation cadence rather than sustained high-latency throughput.
    ar_sync_scheduler:override_tick_interval_ms(10_000),
    PeerCPS = 150,
    Peer = #sim_peer{
        %% Five seconds require a 750-request pipeline to sustain the peer's
        %% 150 chunks/s serving capacity across all six stores.
        max_serve_cps = PeerCPS,
        latency_ms = 5000,
        %% A 250 ms detailed-metadata response makes discovery faster than the
        %% 150 chunks/s fetch path, keeping request latency as the bottleneck.
        chunk_interval_latency_ms = ?SIM_SUBSTEP_MS,
        sync_kinds = [footprint]
    },
    ar_sync_sim_runner:start_sim(#sim_world{
        peers = #{?PEER_UNLIMITED => Peer},
        node_config = #{
            %% Six active footprints require 1,536 MiB of entropy. A 2 GiB
            %% cache permits all stores to participate without entropy churn.
            [packing, entropy, cache_size] => 2048,
            %% 564 MiB resolves to a 2,000-chunk fetched cache, comfortably
            %% above the peer's required 750-request pipeline.
            [sync, cache_size] => 564
        }
    }),
    %% One hundred sixty scheduler ticks cover discovery and enough
    %% one-quarter growth steps to exceed the 750-request pipeline required for
    %% 150 chunks/s at five-second latency. The 120 measured ticks span 1,200
    %% seconds and contain 30,000 writes per store at the expected rate.
    ar_sync_sim_runner:run_for(160),
    Measurement = ar_sync_sim_runner:run_for(120),
    assert_chunks_spread_across_stores(Measurement),
    assert_metric_utilization(stored_cps, PeerCPS, Measurement).

%% Contract: raising response latency from 0.5 to 5 seconds must not make the six
%% 25 chunks/s destination stores alternate between waiting for a response batch
%% and writing one. Fetches must stay in flight while completed chunks are
%% written so every store remains supplied. The peer can serve 300 chunks/s
%% against the stores' combined 150 chunks/s write rate, and each remote source
%% store can serve 35 chunks/s against its destination's 25 chunks/s write rate.
%% Local writes are therefore the intended bottleneck. The average must remain
%% near that bottleneck, and no scheduler interval may become broadly idle.
delayed_fetches_preserve_store_write_throughput_test_() ->
    ar_sync_sim_runner:setup_sim(
        fun test_delayed_fetches_preserve_store_write_throughput/0, 500).

test_delayed_fetches_preserve_store_write_throughput() ->
    ar_sync_sim_world:use_mainnet_replica_2_9_sizes(),
    %% Match the production scheduler cadence so five-second responses finish
    %% in large waves inside each observation interval.
    ar_sync_scheduler:override_tick_interval_ms(10_000),
    StoreCPS = 25,
    AggregateStoreCPS = ?SIM_STORES * StoreCPS,
    InitialLatencyMS = 500,
    DelayedLatencyMS = 5000,
    Peer = #sim_peer{
        %% Six stores write 150 chunks/s; 300 chunks/s leaves a twofold remote
        %% margin while the initial half-second latency establishes full rate.
        max_serve_cps = 2 * AggregateStoreCPS,
        latency_ms = InitialLatencyMS,
        %% One 250 ms metadata response exposes a complete 1024-chunk footprint,
        %% comfortably faster than the peer's 300 chunks/s serving path. This
        %% keeps the test focused on chunk-fetch latency. The default 8 GB
        %% advertised prefix contains over 30,000 chunks per store, more than
        %% the 27,500 each can write during this 1100-second scenario.
        chunk_interval_latency_ms = ?SIM_SUBSTEP_MS,
        sync_kinds = [footprint]
    },
    Peers = #{?PEER_UNLIMITED => Peer},
    World = #sim_world{
        peers = Peers,
        %% Serving at 7/5 of the destination's write rate gives the remote source
        %% store 40% headroom, so destination-store writes remain the bottleneck.
        remote_store_cps = 7 * StoreCPS div 5,
        store_write_cps = StoreCPS,
        node_config = #{
            %% Twelve footprint slots let each of the six stores overlap one
            %% footprint transition, keeping entropy outside this store-write test.
            [packing, entropy, cache_size] => 3072,
            [sync, cache_size] => 564
        }
    },
    ar_sync_sim_runner:start_sim(World),
    %% Sixty scheduler ticks establish full peer and store throughput.
    ar_sync_sim_runner:run_for(60),
    ar_sync_sim_runner:update_world(World#sim_world{ peers = Peers#{
        ?PEER_UNLIMITED := Peer#sim_peer{ latency_ms = DelayedLatencyMS }
    } }),
    %% Twenty ticks cover the latency transition and store-write-rate resampling.
    ar_sync_sim_runner:run_for(20),
    %% Thirty one-tick samples require at least 95% average utilization. A 70%
    %% per-sample floor permits one response-wave boundary in a ten-second
    %% observation while rejecting alternating idle and active intervals.
    Measurements = [ar_sync_sim_runner:run_for(1)
        || _ <- lists:seq(1, 30)],
    StoredCPSByTick = [ar_sync_sim_runner:metric(stored_cps, Measurement)
        || Measurement <- Measurements],
    AverageStoredCPS = lists:sum(StoredCPSByTick) / length(StoredCPSByTick),
    assert_at_least(AverageStoredCPS,
        ?MIN_STEADY_STATE_UTILIZATION * AggregateStoreCPS),
    assert_at_least(lists:min(StoredCPSByTick),
        0.7 * AggregateStoreCPS).

%% Contract: delayed fetch response waves must keep each destination store's
%% completed-chunk backlog bounded. A transient excess must not become
%% self-reinforcing and cause a persistent throughput collapse.
%%
%% Test mechanism: the simulated completion rate represents the entire local
%% path after a fetch, not raw disk bandwidth. Each store normally completes 25
%% chunks/s. If its backlog exceeds ten seconds of that capacity, the simulator
%% cuts completions to one-fifth of normal. This artificial penalty turns an
%% excessive backlog into an observable throughput failure; it does not assert
%% that a production disk slows at that threshold.
delayed_fetch_waves_do_not_amplify_store_backlog_test_() ->
    ar_sync_sim_runner:setup_sim(
        fun test_delayed_fetch_waves_do_not_amplify_store_backlog/0, 500).

test_delayed_fetch_waves_do_not_amplify_store_backlog() ->
    ar_sync_sim_world:use_mainnet_replica_2_9_sizes(),
    ar_sync_scheduler:override_tick_interval_ms(10_000),
    StoreCPS = 25,
    AggregateStoreCPS = ?SIM_STORES * StoreCPS,
    WriteAllowanceSeconds = 5,
    %% The normal five-second cached-write allowance is 5 * 25 = 125 chunks.
    %% A 250-chunk threshold tolerates one additional allowance-sized response
    %% wave before activating the synthetic pressure penalty described above.
    BacklogPressureThreshold = 2 * WriteAllowanceSeconds * StoreCPS,
    PressuredCompletionCPS = StoreCPS div 5,
    StoreCompletionCPS = fun(StoreID, _Tick) ->
        case ar_sync_sim_world:chunk_cache_size(StoreID)
                > BacklogPressureThreshold of
            true -> PressuredCompletionCPS;
            false -> StoreCPS
        end
    end,
    InitialLatencyMS = 500,
    DelayedLatencyMS = 5000,
    Peer = #sim_peer{
        %% The six local paths normally complete 150 chunks/s; a twofold peer
        %% margin keeps local completion capacity as the intended bottleneck.
        max_serve_cps = 2 * AggregateStoreCPS,
        latency_ms = InitialLatencyMS,
        %% A complete footprint arrives from metadata every 250 ms, keeping
        %% discovery outside this fetched-backlog pressure scenario.
        chunk_interval_latency_ms = ?SIM_SUBSTEP_MS,
        sync_kinds = [footprint]
    },
    Peers = #{?PEER_UNLIMITED => Peer},
    World = #sim_world{
        peers = Peers,
        %% Each remote source serves 7/5 of the normal local rate, giving it 40%
        %% headroom so remote storage cannot activate the synthetic penalty.
        remote_store_cps = 7 * StoreCPS div 5,
        store_write_cps = StoreCompletionCPS,
        node_config = #{
            %% Twelve footprint slots let every store overlap one transition;
            %% this scenario isolates fetched-backlog pressure from entropy.
            [packing, entropy, cache_size] => 3072,
            [sync, cache_size] => 564
        }
    },
    ar_sync_sim_runner:start_sim(World),
    %% Sixty scheduler ticks establish full throughput before latency creates
    %% larger completion waves. Twenty ticks cover the transition and sixty
    %% measured ticks expose any recurring backlog-driven collapse.
    ar_sync_sim_runner:run_for(60),
    ar_sync_sim_runner:update_world(World#sim_world{ peers = Peers#{
        ?PEER_UNLIMITED := Peer#sim_peer{ latency_ms = DelayedLatencyMS }
    } }),
    ar_sync_sim_runner:run_for(20),
    Measurement = ar_sync_sim_runner:run_for(60),
    assert_chunks_spread_across_stores(Measurement),
    assert_metric_utilization(stored_cps, AggregateStoreCPS, Measurement).

%% Contract: a temporarily low observation of completed local writes must not
%% become a permanent throughput ceiling. Once later observations demonstrate
%% more capacity, every store and the aggregate must recover to that rate.
%%
%% Test mechanism: the simulator deliberately permits 60 completions every four
%% seconds at first, then 100 every four seconds. That changes the observed
%% average from 15 to 25 chunks/s and verifies recovery from the earlier low
%% observation. It represents temporary contention anywhere in the end-to-end
%% local completion path, not a claim that disk speed normally changes abruptly.
low_local_completion_observation_does_not_limit_recovery_test_() ->
    ar_sync_sim_runner:setup_sim(
        fun test_low_local_completion_observation_does_not_limit_recovery/0,
        500).

test_low_local_completion_observation_does_not_limit_recovery() ->
    StoreCPS = 25,
    AggregateStoreCPS = ?SIM_STORES * StoreCPS,
    BatchPeriod = 4,
    LowObservationBatchSize = 60,
    RecoveredBatchSize = StoreCPS * BatchPeriod,
    RecoveryTick = 40,
    StoreCompletionCPS = fun(_StoreID, Tick) ->
        case Tick rem BatchPeriod of
            0 when Tick < RecoveryTick -> LowObservationBatchSize;
            0 -> RecoveredBatchSize;
            _ -> 0
        end
    end,
    Peer = #sim_peer{
        %% Four seconds of response latency requires a deep, continuously fed
        %% request pipeline to use the six stores' 150 chunks/s capacity.
        max_serve_cps = 600,
        latency_ms = 4000
    },
    ar_sync_sim_runner:start_sim(#sim_world{
        peers = #{?PEER_UNLIMITED => Peer},
        %% Serving at 7/5 of the recovered local rate gives each remote source
        %% 40% headroom, so recovered local completions set expected throughput.
        remote_store_cps = 7 * StoreCPS div 5,
        store_write_cps = StoreCompletionCPS
    }),
    %% Ten four-second batches establish the initial 60 / 4 = 15 chunks/s
    %% completion observation before the simulated local path recovers.
    ar_sync_sim_runner:run_for(RecoveryTick),
    %% Sixty seconds cover fifteen full-capacity batches and scheduler ramp-up.
    ar_sync_sim_runner:run_for(60),
    %% Forty seconds average ten complete batches from every store.
    Measurement = ar_sync_sim_runner:run_for(40),
    assert_chunks_spread_across_stores(Measurement),
    DurationSeconds = ar_sync_sim_runner:metric(duration_seconds, Measurement),
    %% Permit one completion to cross the measurement boundary while retaining
    %% the 95% aggregate-throughput floor for every other chunk.
    MinimumStoredChunks = ?MIN_STEADY_STATE_UTILIZATION
        * AggregateStoreCPS * DurationSeconds - 1,
    assert_at_least(
        ar_sync_sim_runner:metric(chunks_stored_total, Measurement),
        MinimumStoredChunks,
        #{ metric => chunks_stored_total }).

%% An invisible ceiling: a shared downlink smaller than the peers'
%% aggregate capacity. Nothing in the pipeline is told the link's size —
%% requests continue to succeed but queue behind the shared capacity. Throughput
%% must reach the link's capacity without accumulating unbounded outstanding work.
%%
%% Expected over the 30-tick measurement window (peers could serve 800
%% cps, the link carries 100):
%%   - total served: the link rate, pinned exactly.
%%   - the split is emergent and can be extreme — a peer holding more
%%     inflight wins more of each second's slots, and delivery feeds its
%%     cap (e.g. two-thirds of the link to one peer while the trailing
%%     peers trickle at a few cps). Every peer must continue making progress.
%%   - outstanding HTTP requests remain within seven seconds of link capacity.
link_saturation_test_() ->
    ar_sync_sim_runner:setup_sim(fun test_link_saturation/0, 400).

test_link_saturation() ->
    LinkCps = 100,
    Peers = #{
        ?PEER_UNLIMITED => #sim_peer{ max_serve_cps = 200 },
        ?PEER_LIMITED_1 => #sim_peer{ max_serve_cps = 200 },
        ?PEER_LIMITED_2 => #sim_peer{ max_serve_cps = 200 },
        ?PEER_LIMITED_3 => #sim_peer{ max_serve_cps = 200 }
    },
    ar_sync_sim_runner:start_sim(#sim_world{
        peers = Peers,
        link_capacity_cps = LinkCps
    }),
    %% Sixty ticks let queued work behind the invisible link ceiling settle;
    %% forty ticks leaves enough carry-over to exceed the ceiling transiently.
    ar_sync_sim_runner:run_for(60),
    %% Thirty ticks smooth the residual one-second link-queue quantization.
    MeasurementTicks = 30,
    Measurement = ar_sync_sim_runner:run_for(MeasurementTicks),
    CpsByPeer = ar_sync_sim_runner:metric(cps_by_peer, Measurement),
    ServedCPS = lists:sum(maps:values(CpsByPeer)),
    %% Sampling can include completions at both endpoint seconds, so an N-second
    %% window may observe at most N+1 link-capacity buckets.
    MaxObservedCps = LinkCps * (MeasurementTicks + 1) / MeasurementTicks,
    assert_at_most(ServedCPS, MaxObservedCps),
    assert_metric_utilization(stored_cps, LinkCps, Measurement),
    %% No eviction: trailing peers keep a noise-floor trickle (a few
    %% cps; the 1 cps floor sits under it).
    MinimumPeerCPS = 1,
    assert_at_least(
        maps:get(?PEER_UNLIMITED, CpsByPeer),
        MinimumPeerCPS,
        #{ peer => ?PEER_UNLIMITED, metric => cps }),
    assert_at_least(
        maps:get(?PEER_LIMITED_1, CpsByPeer),
        MinimumPeerCPS,
        #{ peer => ?PEER_LIMITED_1, metric => cps }),
    assert_at_least(
        maps:get(?PEER_LIMITED_2, CpsByPeer),
        MinimumPeerCPS,
        #{ peer => ?PEER_LIMITED_2, metric => cps }),
    assert_at_least(
        maps:get(?PEER_LIMITED_3, CpsByPeer),
        MinimumPeerCPS,
        #{ peer => ?PEER_LIMITED_3, metric => cps }),
    HTTPInflightByPeer = ar_sync_sim_runner:metric(
        final_http_inflight_by_peer, Measurement),
    TotalHTTPInflight = lists:sum(maps:values(HTTPInflightByPeer)),
    %% The five-second measured horizon may take one 25% exploration step;
    %% seven whole seconds also cover integer and endpoint quantization.
    MaxOutstandingSeconds = 7,
    MaxHTTPInflight = MaxOutstandingSeconds * LinkCps,
    assert_at_most(TotalHTTPInflight, MaxHTTPInflight,
        #{ metric => total_http_inflight }).

%% Contract: a footprint source can fill its network path without regenerating
%% entropy for each small request batch. Two peers + six stores and eight
%% entropy slots let twelve peer/store fronts expose more footprints
%% than fit in cache.
%% Keeping each footprint together amortizes generation; repeatedly rotating
%% across the slots cannot sustain the peers' 50 cps total.
footprint_entropy_reuse_preserves_throughput_test_() ->
    ar_sync_sim_runner:setup_sim(
        fun test_footprint_entropy_reuse_preserves_throughput/0, 500).

test_footprint_entropy_reuse_preserves_throughput() ->
    PeerCPS = 25,
    %% Two peers retain twelve peer/store fronts, four more than the eight-slot
    %% entropy cache can hold, while reducing simulated task volume.
    PeerCount = 2,
    TotalCPS = PeerCount * PeerCPS,
    Peers = maps:from_list([
        {{10, 0, 1, PeerID, 1984},
            #sim_peer{ max_serve_cps = PeerCPS, sync_kinds = [footprint] }}
        || PeerID <- lists:seq(1, PeerCount)
    ]),
    ar_sync_sim_world:use_mainnet_replica_2_9_sizes(),
    World = #sim_world{
        peers = Peers,
        %% A 250-millisecond miss cost populates the initial eight-entry working
        %% set without making startup the contract under test.
        entropy_generation_ms = 250,
        %% Eight 256 MiB footprint working sets are fewer than the twelve
        %% possible peer/store fronts. A 3 GiB sync budget holds about 11,000 chunks,
        %% enough for the eight working sets and transition work without making
        %% task admission the throughput limit.
        node_config = #{
            [sync, cache_size] => 3072,
            [packing, entropy, cache_size] => 2048
        }
    },
    ar_sync_sim_runner:start_sim(World),
    %% Twenty seconds cover metadata warming, initial entropy generation, and
    %% peer-cap growth. During measurement, a one-second miss cost makes
    %% repeated working-set churn expensive relative to entropy reuse.
    ar_sync_sim_runner:run_for(20),
    ar_sync_sim_runner:update_world(World#sim_world{ entropy_generation_ms = 1000 }),
    %% Ten seconds contain 250 chunks from each peer, enough to distinguish
    %% sustained capacity from regeneration-bound churn.
    Measurement = ar_sync_sim_runner:run_for(10),
    assert_metric_utilization(stored_cps, TotalCPS, Measurement).

%% Contract: stable footprint peers continue to saturate their aggregate
%% serving capacity when many peer/store fronts compete for the entropy cache.
%% Completing one request must not rotate an unfinished footprint out merely
%% because another peer also has queued work.
footprint_contention_preserves_throughput_test_() ->
    ar_sync_sim_runner:setup_sim(
        fun test_footprint_contention_preserves_throughput/0, 900).

test_footprint_contention_preserves_throughput() ->
    PeerCPS = 60,
    %% Six equal peers provide 360 chunks/s while exposing 36 peer/store fronts
    %% across six stores.
    PeerCount = 6,
    TotalCPS = PeerCount * PeerCPS,
    Peers = maps:from_list([
        {{10, 0, 2, PeerID, 1984},
            #sim_peer{
                max_serve_cps = PeerCPS,
                chunk_interval_latency_ms = ?SIM_SUBSTEP_MS,
                sync_kinds = [footprint]
            }}
        || PeerID <- lists:seq(1, PeerCount)
    ]),
    ar_sync_sim_world:use_mainnet_replica_2_9_sizes(),
    ar_sync_sim_runner:start_sim(#sim_world{
        peers = Peers,
        %% A 500-millisecond miss cost is expensive when assignments churn, while
        %% a stable assignment amortizes one generation over 1024 chunks.
        entropy_generation_ms = 500,
        node_config = #{
            [sync, cache_size] => 32768,
            %% Each peer/store receives ten concurrent requests. At 250 ms
            %% latency, every 60 chunks/s peer needs two active footprints, so
            %% twelve slots carry steady work. Sixteen slots leave four for
            %% transitions while 36 fronts still compete for them.
            [packing, entropy, cache_size] => 4096
        }
    }),
    %% Fifty seconds cover discovery, peer-cap growth, and the footprint
    %% transitions that give every peer a binding under entropy contention.
    %% The final thirty seconds contain 1800 chunks from each peer at the settled
    %% aggregate serving rate and average across footprint and dispatch boundary
    %% phases.
    ar_sync_sim_runner:run_for(50),
    Measurement = ar_sync_sim_runner:run_for(30),
    assert_metric_utilization(stored_cps, TotalCPS, Measurement).

%% Contract: a footprint peer that slows after earning several entropy slots
%% does not prevent healthy footprint peers from sustaining their combined rate.
slow_footprint_peer_preserves_healthy_capacity_test_() ->
    ar_sync_sim_runner:setup_sim(
        fun test_slow_footprint_peer_preserves_healthy_capacity/0, 500).

test_slow_footprint_peer_preserves_healthy_capacity() ->
    ar_sync_sim_world:use_mainnet_replica_2_9_sizes(),
    SlowPeer = {10, 6, 6, 1, 1984},
    HealthyPeerIDs = [{10, 6, 6, N, 1984} || N <- lists:seq(2, 6)],
    InitialSlowPeerCPS = 300,
    HealthyPeerCPS = 60,
    InitialSlowPeer = #sim_peer{
        max_serve_cps = InitialSlowPeerCPS,
        %% Metadata stays outside the entropy-slot recovery contract.
        chunk_interval_latency_ms = ?SIM_SUBSTEP_MS,
        sync_kinds = [footprint]
    },
    HealthyPeers = maps:from_list([
        {Peer, #sim_peer{
            max_serve_cps = HealthyPeerCPS,
            chunk_interval_latency_ms = ?SIM_SUBSTEP_MS,
            sync_kinds = [footprint],
            %% Initially these peers advertise their data but cannot accept a
            %% request, separating scheduler recovery from peer discovery.
            http_inflight_limit = 0
        }}
        || Peer <- HealthyPeerIDs
    ]),
    InitiallyUnavailablePeers = maps:put(
        SlowPeer, InitialSlowPeer, HealthyPeers),
    Peers = maps:map(
        fun(_Peer, Peer) -> Peer#sim_peer{ http_inflight_limit = infinity } end,
        InitiallyUnavailablePeers),
    World = #sim_world{
        peers = InitiallyUnavailablePeers,
        node_config = #{
            %% A 32 GiB task cache keeps admission out of this entropy-slot
            %% scenario. Sixty-four 256 MiB entropy entries reproduce the live
            %% node's footprint capacity.
            [sync, cache_size] => 32768,
            [packing, entropy, cache_size] => 16384
        }
    },
    ar_sync_sim_runner:start_sim(World),
    %% Fifty seconds warm metadata and let the 300 chunks/s peer occupy the
    %% footprint slots before the healthy peers begin accepting requests.
    ar_sync_sim_runner:run_for(50),
    ar_sync_sim_runner:update_world(World#sim_world{
        peers = Peers#{
            SlowPeer => InitialSlowPeer#sim_peer{
                max_serve_cps = 1,
                latency_ms = 10_000,
                limited = true
            }
        }
    }),
    %% Two 32-tick scheduler evidence horizons let healthy sources rotate the
    %% larger 200-chunk store horizons, then settle before the steady measurement.
    ar_sync_sim_runner:run_for(64),
    Measurement = ar_sync_sim_runner:run_for(20),
    CpsByPeer = ar_sync_sim_runner:metric(cps_by_peer, Measurement),
    HealthyCPS = lists:sum([
        maps:get(Peer, CpsByPeer, 0) || Peer <- HealthyPeerIDs
    ]),
    %% Five healthy peers at 60 chunks/s provide 300 chunks/s.
    assert_at_least(HealthyCPS,
        ?MIN_STEADY_STATE_UTILIZATION
            * length(HealthyPeerIDs) * HealthyPeerCPS).

%% Contract: one whole footprint cannot consume a store's claim headroom and
%% prevent another peer from receiving work for that store.
%%
%% A 564 MiB total sync budget leaves a 2000-chunk cache after the interval-cache
%% allowance. One quarter is 500 chunks, which rounds to one complete 1024-chunk
%% footprint. The scheduler must let completions restore capacity and keep both
%% peers serving instead of leaving the second peer without work.
footprint_claim_headroom_preserves_peer_breadth_test_() ->
    ar_sync_sim_runner:setup_sim(
        fun test_footprint_claim_headroom_preserves_peer_breadth/0, 500).

test_footprint_claim_headroom_preserves_peer_breadth() ->
    TargetStore = first_store(),
    PeerCPS = 15,
    Peer = #sim_peer{
        max_serve_cps = PeerCPS,
        sync_kinds = [footprint],
        sync_availability = {stores, [TargetStore]}
    },
    Peers = #{
        ?PEER_UNLIMITED => Peer,
        ?PEER_LIMITED_1 => Peer
    },
    ar_sync_sim_world:use_mainnet_replica_2_9_sizes(),
    ar_sync_sim_runner:start_sim(#sim_world{
        peers = Peers,
        node_config = #{
            [sync, cache_size] => 564,
            [packing, entropy, cache_size] => 2048
        }
    }),
    %% Twenty-five ticks warm discovery and peer control. Twenty measurement
    %% ticks contain 300 chunks at each peer's 15 chunks/s serving capacity.
    ar_sync_sim_runner:run_for(25),
    Measurement = ar_sync_sim_runner:run_for(20),
    MinimumPeerCPS = ?MIN_STEADY_STATE_UTILIZATION * PeerCPS,
    assert_at_least(
        ar_sync_sim_runner:metric(
            {cps_by_peer, ?PEER_UNLIMITED}, Measurement),
        MinimumPeerCPS,
        #{ peer => ?PEER_UNLIMITED, metric => cps }),
    assert_at_least(
        ar_sync_sim_runner:metric(
            {cps_by_peer, ?PEER_LIMITED_1}, Measurement),
        MinimumPeerCPS,
        #{ peer => ?PEER_LIMITED_1, metric => cps }),
    assert_metric_utilization(stored_cps, 2 * PeerCPS, Measurement),
    ProgressByStore = ar_sync_sim_runner:metric(
        chunks_stored_by_store, Measurement),
    TotalStored = ar_sync_sim_runner:metric(
        chunks_stored_total, Measurement),
    ?assertEqual(TotalStored, maps:get(TargetStore, ProgressByStore, 0)),
    ?assertEqual([], [
        StoreID
        || {StoreID, Stored} <- maps:to_list(ProgressByStore),
            StoreID =/= TargetStore,
            Stored > 0
    ]).

%% Contract: a byte peer can serve one chunk in a footprint and a footprint
%% peer can serve the remaining chunks it advertises from the same footprint.
byte_and_footprint_sources_share_footprint_test_() ->
    ar_sync_sim_runner:setup_sim(
        fun test_byte_and_footprint_sources_share_footprint/0, 500).

test_byte_and_footprint_sources_share_footprint() ->
    ar_sync_sim_world:use_mainnet_replica_2_9_sizes(),
    {StoreStart, _StoreEnd} = first_store_range(),
    SingleChunkPeer = {10, 6, 2, 1, 1984},
    FootprintPeer = {10, 6, 2, 2, 1984},
    %% The ninth byte query step is beyond the initial eight-step byte queue.
    %% Byte-mode discovery also queries the footprint containing that frontier,
    %% keeping cold coarse discovery outside this source-sharing contract.
    Step = ar_sync_cursor:query_range_step_size(),
    FirstChunkIndex = 9 * Step div ?DATA_CHUNK_SIZE + 1,
    %% The byte peer advertises the first chunk; the footprint peer advertises
    %% that chunk and the following nine chunks in the same footprint.
    AdvertisedChunkCount = 10,
    AdvertisedChunkIntervals = footprint_chunk_intervals(
        StoreStart, FirstChunkIndex, AdvertisedChunkCount),
    [FirstChunkInterval | _] = AdvertisedChunkIntervals,
    InitialPeers = #{
        SingleChunkPeer => #sim_peer{
            max_serve_cps = 100,
            sync_kinds = [byte],
            sync_availability = {intervals, [FirstChunkInterval]}
        },
        FootprintPeer => #sim_peer{
            max_serve_cps = 100,
            sync_kinds = [footprint],
            sync_availability = {intervals, AdvertisedChunkIntervals},
            footprint_coverage = exact
        }
    },
    InitialWorld = #sim_world{
        peers = InitialPeers,
        node_config = #{
            [packing, entropy, cache_size] => 2048
        }
    },
    ar_sync_sim_runner:start_sim(InitialWorld),
    %% Thirty ticks cover metadata discovery plus the sparse byte chunk and the
    %% footprint source's nine remaining chunks.
    InitialMeasurement = ar_sync_sim_runner:run_for(30),
    assert_at_least(ar_sync_sim_runner:metric(
        {served_by_peer, SingleChunkPeer}, InitialMeasurement), 1),
    assert_at_least(ar_sync_sim_runner:metric(
        {served_by_peer, FootprintPeer}, InitialMeasurement), 9),
    ?assertEqual(AdvertisedChunkCount,
        ar_sync_sim_runner:metric(chunks_stored_total, InitialMeasurement)).

%% Contract: after a footprint reservation binds, its unmaterialized intervals
%% must not consume ordinary task-admission headroom. Only the finite batch of
%% concrete tasks awaiting dispatch uses that headroom; active fetches are
%% bounded separately. Non-overlapping work discovered later for the same store
%% must therefore be admitted while the footprint's slow requests remain active.
unpromoted_footprint_work_does_not_block_store_test_() ->
    ar_sync_sim_runner:setup_sim(
        fun test_unpromoted_footprint_work_does_not_block_store/0, 500).

test_unpromoted_footprint_work_does_not_block_store() ->
    ar_sync_sim_world:use_mainnet_replica_2_9_sizes(),
    TargetStore = first_store(),
    StoreRange = first_store_range(),
    {StoreStart, _StoreEnd} = StoreRange,
    SlowFootprintPeer = {10, 6, 5, 1, 1984},
    HealthyBytePeer = {10, 6, 5, 2, 1984},
    HealthyPeerCPS = 100,
    %% Full footprint coverage expands this one advertised chunk into all 1024
    %% chunks in the footprint before the ten-second source update.
    FirstChunkInterval = single_chunk_interval(StoreStart, 1),
    SlowPeer = #sim_peer{
        max_serve_cps = HealthyPeerCPS,
        %% Thirty-nine seconds keeps the initial child fetches active during
        %% healthy-peer discovery while remaining below the 40-second timeout.
        latency_ms = 39_000,
        sync_kinds = [footprint],
        sync_availability = {intervals, [FirstChunkInterval]},
        footprint_coverage = full
    },
    %% Sixteen one-GiB sweep steps provide 16,384 healthy chunks, enough for more
    %% than the complete 40-second warm-up and measurement at 100 chunks/s.
    %% Four empty byte sweep steps let the slow footprint bind before the
    %% healthy range is discovered. The healthy range then has enough work to
    %% sustain both the warmup and measurement.
    HealthyIntervals = [sweep_chunk_interval(StoreRange, 4, 16)],
    HealthyPeer = #sim_peer{
        max_serve_cps = HealthyPeerCPS,
        sync_kinds = [byte],
        sync_availability = {intervals, HealthyIntervals}
    },
    InitialWorld = #sim_world{
        peers = #{
            SlowFootprintPeer => SlowPeer,
            HealthyBytePeer => HealthyPeer
        },
        store_write_cps = HealthyPeerCPS,
        %% A 564 MiB node cache gives each of six stores a 500-chunk share,
        %% rounded up to one complete 1024-chunk footprint reservation.
        node_config = #{
            [sync, cache_size] => 564,
            [packing, entropy, cache_size] => 2048
        }
    },
    ar_sync_sim_runner:start_sim(InitialWorld),
    %% The slow requests remain active for almost all forty ticks. Correct
    %% footprint accounting lets healthy metadata, sweeping, and cap growth
    %% proceed concurrently; retained unpromoted claims delay them until the
    %% measurement and fail the steady-state floor.
    ar_sync_sim_runner:run_for(40),
    Measurement = ar_sync_sim_runner:run_for(20),
    assert_metric_utilization(
        {cps_by_peer, HealthyBytePeer}, HealthyPeerCPS, Measurement),
    DurationSeconds = ar_sync_sim_runner:metric(
        duration_seconds, Measurement),
    ExpectedStoredChunks = HealthyPeerCPS * DurationSeconds,
    assert_metric_utilization(
        {chunks_stored_by_store, TargetStore},
        ExpectedStoredChunks,
        Measurement).

%% Contract: a peer advertising a small group of chunks in each of many
%% footprints can sustain its serving rate. When the entropy cache has room,
%% those footprints must progress concurrently rather than one at a time.
sparse_footprint_sources_preserve_throughput_test_() ->
    ar_sync_sim_runner:setup_sim(
        fun test_sparse_footprint_sources_preserve_throughput/0, 700).

test_sparse_footprint_sources_preserve_throughput() ->
    ar_sync_sim_world:use_mainnet_replica_2_9_sizes(),
    SparsePeer = {10, 6, 3, 1, 1984},
    PeerCPS = 120,
    ChunksPerFootprint = 32,
    %% Each footprint supplies 32 chunks. With two-second responses, one active
    %% footprint per store can deliver at most 6 * 32 / 2 = 96 chunks/s, below
    %% the peer's 120 chunks/s capacity. The 256 footprints per store provide
    %% 49,152 chunks, almost six times the 8,400 needed by this scenario and
    %% leave useful work beyond cold-start readahead.
    FootprintCountPerStore = 256,
    SparseIntervals = lists:append([
        footprint_chunk_intervals(
            StoreStart, FirstChunkIndex, ChunksPerFootprint)
        || {_StoreID, {StoreStart, _StoreEnd}}
                <- ar_sync_sim_world:store_ranges(),
            FirstChunkIndex <- lists:seq(1, FootprintCountPerStore)
    ]),
    Peers = #{
        SparsePeer => #sim_peer{
            max_serve_cps = PeerCPS,
            latency_ms = 2000,
            %% One 250 ms metadata response exposes 32 chunks, a 128 chunks/s
            %% metadata path above the peer's 120 chunks/s serving rate.
            %% Metadata-scarcity behavior is covered by separate scenarios.
            chunk_interval_latency_ms = ?SIM_SUBSTEP_MS,
            sync_kinds = [footprint],
            sync_availability = {intervals, SparseIntervals},
            footprint_coverage = exact
        }
    },
    ar_sync_sim_runner:start_sim(#sim_world{
        peers = Peers,
        node_config = #{
            [sync, cache_size] => 32768,
            %% Sixty-four mainnet-sized entropy entries can carry the thirty
            %% 250 ms requests needed for 120 chunks/s without constraining it.
            [packing, entropy, cache_size] => 16384
        }
    }),
    %% Fifty seconds cover detailed metadata and peer-cap growth. Twenty
    %% measurement seconds contain 2400 chunks at the peer's serving rate.
    ar_sync_sim_runner:run_for(50),
    Measurement = ar_sync_sim_runner:run_for(20),
    assert_metric_utilization(stored_cps, PeerCPS, Measurement).

%% Contract: a detailed footprint-availability request that remains pending does
%% not stop a store from discovering and syncing later work from a healthy peer.
pending_chunk_intervals_do_not_stall_store_test_() ->
    ar_sync_sim_runner:setup_sim(
        fun test_pending_chunk_intervals_do_not_stall_store/0, 300).

test_pending_chunk_intervals_do_not_stall_store() ->
    ar_sync_sim_world:use_mainnet_replica_2_9_sizes(),
    TargetStore = first_store(),
    StoreRange = first_store_range(),
    {StoreStart, _StoreEnd} = StoreRange,
    StalePeer = {10, 6, 1, 1, 1984},
    HealthyPeer = {10, 6, 1, 2, 1984},
    HealthyPeerCPS = 25,
    StaleInterval = single_chunk_interval(StoreStart, 1),
    %% Four sweep steps place healthy work beyond the unavailable frontier.
    %% The rolling window warms those later steps while the first request waits.
    %% One 1 GiB sweep interval contains 4096 chunks, or more than 160 seconds
    %% at 25 chunks/s, so the healthy source cannot run dry here.
    HealthyIntervals = [sweep_chunk_interval(StoreRange, 4, 1)],
    Peers = #{
        StalePeer => #sim_peer{
            max_serve_cps = 100,
            sync_kinds = [footprint],
            sync_availability = {intervals, [StaleInterval]},
            footprint_coverage = exact,
            %% The scenario lasts sixty seconds. Twice that duration keeps the
            %% first frontier request pending throughout it.
            chunk_interval_latency_ms = 120_000
        },
        HealthyPeer => #sim_peer{
            max_serve_cps = HealthyPeerCPS,
            sync_kinds = [byte],
            sync_availability = {intervals, HealthyIntervals}
        }
    },
    ar_sync_sim_runner:start_sim(#sim_world{
        peers = Peers
    }),
    %% Forty ticks cover detailed metadata, traversal to the healthy range,
    %% and peer-cap growth. Twenty ticks then average 500 chunks at
    %% the healthy peer's 25 chunks/s modeled serving capacity.
    ar_sync_sim_runner:run_for(40),
    Measurement = ar_sync_sim_runner:run_for(20),
    assert_metric_utilization(
        {cps_by_peer, HealthyPeer}, HealthyPeerCPS, Measurement),
    DurationSeconds = ar_sync_sim_runner:metric(
        duration_seconds, Measurement),
    ExpectedStoredChunks = HealthyPeerCPS * DurationSeconds,
    assert_metric_utilization(
        {chunks_stored_by_store, TargetStore},
        ExpectedStoredChunks,
        Measurement).

%% Contract: repeated empty footprints inside coarse peer advertisements must
%% not make detailed metadata traversal the throughput bottleneck. Useful
%% footprints must be discovered often enough to keep aggregate writes at the
%% peer's serving rate throughout the measurement window without starving a
%% store.
repeated_empty_footprint_gaps_preserve_throughput_test_() ->
    ar_sync_sim_runner:setup_sim(
        fun test_repeated_empty_footprint_gaps_preserve_throughput/0, 400).

test_repeated_empty_footprint_gaps_preserve_throughput() ->
    Peer = {10, 6, 6, 1, 1984},
    PeerCPS = 2,
    %% Two 250 ms simulation substeps model a responsive metadata endpoint
    %% while leaving every request asynchronous.
    MetadataLatencyMS = 2 * ?SIM_SUBSTEP_MS,
    FootprintSize = ar_replica_2_9:get_footprint_size(),
    NetworkBucketSize = ar_sync_buckets:get_network_footprint_bucket_size(),
    0 = NetworkBucketSize rem FootprintSize,
    %% Four chunks per footprint and thirty-six footprint slots per coarse
    %% bucket yield nine footprints per bucket. Advertising only the last
    %% footprint makes the preceding eight detailed responses empty.
    FootprintsPerBucket = NetworkBucketSize div FootprintSize,
    EmptyFootprintsPerGap = FootprintsPerBucket - 1,
    FootprintPeriod = EmptyFootprintsPerGap + 1,
    %% Twelve useful footprints per store provide 12 * 6 * 4 = 288 chunks,
    %% more than the 100 seconds * 2 chunks/s = 200 this scenario can use.
    UsefulFootprintsPerStore = 12,
    UsefulFootprintIndexes = [
        N * FootprintPeriod
        || N <- lists:seq(1, UsefulFootprintsPerStore)
    ],
    AvailableIntervals = [
        Interval
        || {_StoreID, {StoreStart, _StoreEnd}}
                <- ar_sync_sim_world:store_ranges(),
            FootprintIndex <- UsefulFootprintIndexes,
            Interval <- footprint_chunk_intervals(
                StoreStart, FootprintIndex, FootprintSize)
    ],
    Peers = #{Peer => #sim_peer{
        max_serve_cps = PeerCPS,
        sync_kinds = [footprint],
        sync_availability = {intervals, AvailableIntervals},
        footprint_coverage = exact,
        chunk_interval_latency_ms = MetadataLatencyMS
    }},
    ar_sync_sim_runner:start_sim(#sim_world{ peers = Peers }),
    %% Six stores expose 6 * 4 / (9 * 0.5) = 5.33 chunks/s, enough for the
    %% two chunks/s peer. Forty seconds reach several useful waves and ramp
    %% the scheduler before throughput is measured.
    ar_sync_sim_runner:run_for(40),
    %% Sixty seconds require 120 chunks, more than four six-store waves (96),
    %% so this measurement requires repeated discovery throughout the window.
    Measurement = ar_sync_sim_runner:run_for(60),
    assert_metric_utilization(stored_cps, PeerCPS, Measurement),
    %% Store ranges enter global coarse buckets at different phases. This
    %% throughput contract requires progress from every store, not equal shares
    %% during the finite measurement window.
    assert_chunks_spread_across_stores(Measurement, 0).

%% Contract: cached footprint availability from a healthy peer remains usable
%% while detailed availability requests to other peers are still pending. A
%% slow metadata peer must not hold every store behind an all-peers barrier.
ready_footprint_peer_progresses_while_metadata_pending_test_() ->
    ar_sync_sim_runner:setup_sim(
        fun test_ready_footprint_peer_progresses_while_metadata_pending/0, 400).

test_ready_footprint_peer_progresses_while_metadata_pending() ->
    HealthyPeer = {10, 7, 0, 1, 1984},
    SlowPeers = [{10, 7, 0, ID, 1984} || ID <- lists:seq(2, 5)],
    HealthyPeerCPS = 100,
    HealthyPeerSpec = #sim_peer{ max_serve_cps = HealthyPeerCPS,
        sync_kinds = [footprint] },
    %% The scenario lasts fifty seconds. A 120-second response keeps each slow
    %% peer's detailed availability request pending throughout it.
    SlowPeerSpec = HealthyPeerSpec#sim_peer{
        chunk_interval_latency_ms = 120_000
    },
    Peers = maps:from_list(
        [{HealthyPeer, HealthyPeerSpec}
            | [{SlowPeer, SlowPeerSpec} || SlowPeer <- SlowPeers]]),
    ar_sync_sim_world:use_mainnet_replica_2_9_sizes(),
    ar_sync_sim_runner:start_sim(#sim_world{
        peers = Peers,
        node_config = #{[packing, entropy, cache_size] => 2048}
    }),
    %% Thirty seconds warm the healthy peer's metadata and concurrency cap.
    ar_sync_sim_runner:run_for(30),
    %% Twenty seconds contain 2000 chunks at the healthy peer's 100 chunks/s
    %% serving capacity while every slow peer remains metadata-pending.
    Measurement = ar_sync_sim_runner:run_for(20),
    assert_metric_utilization(
        {cps_by_peer, HealthyPeer}, HealthyPeerCPS, Measurement).

%% Contract: byte-serving peers remain usable while footprint work occupies
%% every entropy slot. A full footprint lane must not prevent independently
%% fetchable byte work from sustaining its available serving rate.
dual_advertisement_uses_byte_capacity_test_() ->
    ar_sync_sim_runner:setup_sim(
        fun test_dual_advertisement_uses_byte_capacity/0, 300).

test_dual_advertisement_uses_byte_capacity() ->
    FootprintPeer = {10, 6, 1, 1, 1984},
    BytePeer = {10, 6, 1, 2, 1984},
    FootprintPeerCPS = 5,
    BytePeerCPS = 25,
    TargetStore = first_store(),
    %% Production's 1 GB byte step is large enough that its 1.5x task budget
    %% contains both that byte range and one 1024-chunk footprint. Preserve the
    %% same relationship while limiting advertised work to one store.
    QueryStep = 2 * ?MAINNET_REPLICA_2_9_ENTROPY_SIZE * ?SUB_CHUNK_COUNT,
    Peers = #{
        FootprintPeer => #sim_peer{
            %% Five chunks/s keeps the sole footprint slot occupied
            %% but far below the independent byte peer's useful capacity.
            max_serve_cps = FootprintPeerCPS,
            sync_kinds = [footprint],
            sync_availability = {stores, [TargetStore]}
        },
        BytePeer => #sim_peer{
            max_serve_cps = BytePeerCPS,
            sync_kinds = [byte],
            sync_availability = {stores, [TargetStore]}
        }
    },
    ar_sync_sim_world:use_mainnet_replica_2_9_sizes(),
    ar_sync_cursor:override_query_range_step_size(QueryStep),
    ar_sync_sim_runner:start_sim(#sim_world{
        peers = Peers,
        %% A mainnet footprint contains 1024 chunks and occupies 256 MiB of
        %% entropy cache, so this permits exactly one active footprint. A
        %% 564 MiB total sync budget resolves to a 2000-chunk cache, enough for
        %% the active footprint and independent byte work without a large queue.
        node_config = #{
            [sync, cache_size] => 564,
            [packing, entropy, cache_size] => 256
        }
    }),
    %% Forty seconds cover metadata warming and scheduler adaptation while the
    %% five chunks/s source keeps its first 1024-chunk footprint active.
    ar_sync_sim_runner:run_for(40),
    %% Twenty seconds contains 100 expected footprint completions, enough for
    %% the 95% floor to distinguish persistent source starvation from a task at
    %% either measurement boundary.
    Measurement = ar_sync_sim_runner:run_for(20),
    %% Requiring 95% of both peers' independent capacity rejects either source
    %% class monopolizing the overlapping work.
    assert_metric_utilization(
        {cps_by_peer, FootprintPeer}, FootprintPeerCPS, Measurement),
    assert_metric_utilization(
        {cps_by_peer, BytePeer}, BytePeerCPS, Measurement).

%% Contract: the node discovers and syncs footprint-formatted data at the
%% serving peer's full capacity while every store progresses, even when many
%% known peers advertise no sync data. Those peers must not reduce the chunk
%% interval job capacity available to the serving peer.
%%
%% A live node syncing from one peer also knew about roughly 200 non-serving
%% gossip peers. The job limit was divided across every known peer, reducing
%% the serving peer to one concurrent job. A footprint job previously made up
%% to 128 sequential metadata requests, so stores obtained chunk intervals one
%% at a time while their sweepers waited for metadata. Throughput fell from 43
%% MiB/s to 9.7 MiB/s as the known-peer set grew. Job capacity must instead be
%% divided across peers that advertise sync data. This scenario requires both
%% sustained aggregate throughput and concurrent progress across all stores.
footprint_gossip_share_collapse_test_() ->
    ar_sync_sim_runner:setup_sim(fun test_footprint_gossip_share_collapse/0, 500).

test_footprint_gossip_share_collapse() ->
    Peers = maps:merge(
        #{?PEER_UNLIMITED => #sim_peer{
            max_serve_cps = 100,
            sync_kinds = [footprint]
        }},
        %% These peers are known through gossip but advertise no sync data.
        non_serving_peers(200)),
    ar_sync_sim_world:use_mainnet_replica_2_9_sizes(),
    ar_sync_sim_runner:start_sim(#sim_world{
        peers = Peers,
        %% Six stores need six 256 MiB footprint working sets (1536 MiB).
        %% 4096 MiB holds sixteen, leaving ten spare so fine-metadata arrival
        %% order cannot turn this discovery contract into entropy contention.
        node_config = #{ [packing, entropy, cache_size] => 4096 }
    }),
    %% Twenty ticks cover discovery and metadata warming despite the 200 known
    %% non-serving peers; twenty more contain 2000 chunks at serving capacity.
    ar_sync_sim_runner:run_for(20),
    Measurement = ar_sync_sim_runner:run_for(20),
    %% Only the serving peer contributes capacity; the 200 gossip-only peers
    %% must not reduce its 100 chunks/s path.
    assert_metric_utilization(stored_cps, 100, Measurement),
    assert_chunks_spread_across_stores(Measurement, 0.25),
    ok.

%% Contract: one peer can supply chunk interval metadata to all stores
%% concurrently when each metadata request takes one simulated second.
%%
%% Discovery previously allowed only one chunk interval job per peer. A single
%% data-holding peer serving six stores therefore queried the stores one at a
%% time, leaving one job inflight against a backlog that reached 1024 jobs.
%% Job selection must distribute available metadata capacity across
%% stores rather than leaving it occupied by one store's backlog. Every store
%% must complete chunks during the measurement window, regardless of the exact
%% job limit.
metadata_scarce_store_spread_test_() ->
    ar_sync_sim_runner:setup_sim(fun test_metadata_scarce_store_spread/0, 400).

test_metadata_scarce_store_spread() ->
    ChunksPerRange = 64,
    MetadataLatencySeconds = 1,
    ar_sync_cursor:override_query_range_step_size(
        ChunksPerRange * ?DATA_CHUNK_SIZE),
    %% Sixty-four metadata ranges per store keep metadata scarce while leaving
    %% useful work beyond the twenty-second warm-up.
    PeerIntervals = [
        sweep_chunk_interval(StoreRange, 0, 64)
        || {_StoreID, StoreRange} <- ar_sync_sim_world:store_ranges()
    ],
    Peers = #{ ?PEER_UNLIMITED => #sim_peer{
        max_serve_cps = 400,
        %% Six stores need six concurrent metadata requests. Requests beyond
        %% one per store model endpoint overload and cannot complete during
        %% this forty-second scenario.
        chunk_interval_latency_ms = fun(NumInflight) ->
            case NumInflight =< ?SIM_STORES of
                true -> MetadataLatencySeconds * 1000;
                false -> 60_000
            end
        end,
        sync_availability = {intervals, PeerIntervals}
    } },
    ar_sync_sim_runner:start_sim(#sim_world{
        peers = Peers
    }),
    %% Twenty seconds warm detailed metadata and distribute requests across
    %% every store before measurement.
    ar_sync_sim_runner:run_for(20),
    Measurement = ar_sync_sim_runner:run_for(20),
    %% Metadata capacity must be spread across every store without duplicate
    %% requests overloading the peer's metadata endpoint.
    MetadataCPS = ?SIM_STORES * ChunksPerRange / MetadataLatencySeconds,
    %% The peer can serve 400 chunks/s while metadata exposes 384 chunks/s.
    %% Ninety percent permits scheduler boundaries but rejects a starved lane.
    assert_at_least(ar_sync_sim_runner:metric(stored_cps, Measurement),
        0.90 * MetadataCPS, #{ metric => stored_cps }),
    assert_chunks_spread_across_stores(Measurement, 0),
    ok.

%% Contract: when many peers advertise the same sparse chunks, old metadata
%% requests do not displace metadata needed at each store's current frontier.
%% Aggregate progress must continue across all stores.
overlapping_peer_metadata_preserves_store_throughput_test_() ->
    ar_sync_sim_runner:setup_sim(
        fun test_overlapping_peer_metadata_preserves_store_throughput/0, 500).

test_overlapping_peer_metadata_preserves_store_throughput() ->
    PeerCount = 24,
    PeerCPS = 100,
    ChunksPerRange = 64,
    %% Eight successive frontiers can submit 24 peers x 6 stores x 8 starts =
    %% 1152 metadata requests. The 128 ranges keep useful data available for
    %% the complete 38-second scenario after that backlog develops.
    RangeCount = 128,
    SharedIntervals = [
        {StoreStart + RangeIndex * ?QUERY_RANGE_STEP_SIZE
                + ChunksPerRange * ?DATA_CHUNK_SIZE,
            StoreStart + RangeIndex * ?QUERY_RANGE_STEP_SIZE}
        || {_StoreID, {StoreStart, _StoreEnd}} <- ar_sync_sim_world:store_ranges(),
            RangeIndex <- lists:seq(0, RangeCount - 1)
    ],
    Peers = maps:from_list([
        {{10, 5, 0, ID, 1984}, #sim_peer{
            max_serve_cps = PeerCPS,
            sync_availability = {intervals, SharedIntervals}
        }}
        || ID <- lists:seq(1, PeerCount)
    ]),
    ar_sync_sim_runner:start_sim(#sim_world{
        peers = Peers
    }),
    %% Eight seconds let every sweep queue fill before measuring serialized
    %% per-peer/store metadata progress.
    ar_sync_sim_runner:run_for(8),
    Measurement = ar_sync_sim_runner:run_for(30),
    %% Each store must expose at least one 64-chunk range every two seconds.
    %% The 90% floor permits three seconds of aggregate measurement-boundary
    %% loss from that conservative 192 chunks/s path.
    ExpectedCPS = ?SIM_STORES * ChunksPerRange / 2,
    assert_at_least(ar_sync_sim_runner:metric(stored_cps, Measurement),
        0.90 * ExpectedCPS, #{ metric => stored_cps }),
    %% Half of each store's conservative 32 chunks/s path permits metadata
    %% scheduling variation while rejecting progress concentrated on only a
    %% subset of stores. Other stores' surplus work must not raise this floor.
    MinimumStoreCPS = 0.5 * ChunksPerRange / 2,
    MeasurementSeconds = ar_sync_sim_runner:metric(
        duration_seconds, Measurement),
    ChunksStoredByStore = ar_sync_sim_runner:metric(
        chunks_stored_by_store, Measurement),
    ?assertEqual(?SIM_STORES, map_size(ChunksStoredByStore)),
    maps:foreach(
        fun(StoreID, ChunksStored) ->
            assert_at_least(ChunksStored / MeasurementSeconds, MinimumStoreCPS,
                #{ store => StoreID, metric => stored_cps,
                    chunks_stored_by_store => ChunksStoredByStore })
        end,
        ChunksStoredByStore).

%% Contract: discovery can sustain sync when each store's needed data is held
%% by a different peer. This prevents metadata work for one peer or store from
%% delaying all other usable peer-store paths.
disjoint_peer_holdings_test_() ->
    ar_sync_sim_runner:setup_sim(fun test_disjoint_peer_holdings/0, 500).

test_disjoint_peer_holdings() ->
    StoreRanges = ar_sync_sim_world:store_ranges(),
    PeerCps = 100,
    Peers = maps:from_list([
        {{10, 1, 0, N, 1984}, #sim_peer{
            max_serve_cps = PeerCps,
            %% At one second, each 100 chunks/s peer needs about 100 concurrent
            %% requests to use its declared capacity.
            latency_ms = 4 * ?SIM_SUBSTEP_MS,
            sync_availability = {stores, [StoreID]}
        }}
        || {N, {StoreID, _Range}}
            <- lists:zip(lists:seq(1, length(StoreRanges)), StoreRanges)
    ]),
    ar_sync_sim_runner:start_sim(#sim_world{
        peers = Peers
    }),
    %% Fifty ticks cover the 32-tick control horizon after initially cold
    %% metadata is discovered for all six peer-store paths.
    ar_sync_sim_runner:run_for(50),
    %% Twenty seconds contain 2000 chunks from each 100 chunks/s store path.
    Measurement = ar_sync_sim_runner:run_for(20),
    %% Six independent 100 chunks/s peers provide 600 chunks/s. The common
    %% five-percent settled margin applies to this composed path.
    assert_metric_utilization(stored_cps, capacity(Peers), Measurement),
    %% Half of an equal store share permits allocation variation while rejecting
    %% a run where only a subset of stores progresses.
    assert_chunks_spread_across_stores(Measurement, 0.5).

%% Contract: a peer that cannot complete work near each store's sweep frontier
%% does not hide healthy peers serving later ranges. The stalled peer advertises
%% one frontier chunk in every store; one 100 chunks/s peer advertises 1024
%% ranges after the next empty range. The healthy capacity must remain usable,
%% and every store must progress.
stalled_frontier_peer_does_not_hide_later_capacity_test_() ->
    ar_sync_sim_runner:setup_sim(
        fun test_stalled_frontier_peer_does_not_hide_later_capacity/0, 500).

test_stalled_frontier_peer_does_not_hide_later_capacity() ->
    StoreRanges = ar_sync_sim_world:store_ranges(),
    %% One unavailable chunk per store gives the stalled peer six outstanding
    %% requests, fitting its initial eight-request exploration cap. The empty
    %% remainder of the first two ranges requires the cursor to keep advancing.
    %% The following 1024 ranges remain active during measurement.
    FrontierRangeCount = 2,
    HealthyRangeCount = 1024,
    StalledIntervals = [
        {min(RangeEnd, RangeStart + ?DATA_CHUNK_SIZE), RangeStart}
        || {_StoreID, {RangeStart, RangeEnd}} <- StoreRanges
    ],
    HealthyIntervals = [
        sweep_chunk_interval(StoreRange, FrontierRangeCount, HealthyRangeCount)
        || {_StoreID, StoreRange} <- StoreRanges
    ],
    StalledPeer = {10, 4, 0, 1, 1984},
    HealthyPeer = {10, 4, 0, 2, 1984},
    %% One 100 chunks/s source still creates substantial later-range work while
    %% keeping this cursor-advancement fixture smaller than capacity tests.
    HealthyPeerCPS = 100,
    Peers = maps:put(
        StalledPeer,
        #sim_peer{
            %% Zero serving capacity keeps its initial claims outstanding for
            %% the scenario, modeling a peer whose requests stop progressing.
            max_serve_cps = 0,
            sync_availability = {intervals, StalledIntervals}
        },
        #{
            HealthyPeer => #sim_peer{
                max_serve_cps = HealthyPeerCPS,
                sync_availability = {intervals, HealthyIntervals}
            }
        }),
    ar_sync_sim_runner:start_sim(#sim_world{
        peers = Peers
    }),
    %% Fifty seconds cover the scheduler horizon after discovery reaches the
    %% healthy ranges; twenty seconds smooth the one-second serving limits.
    ar_sync_sim_runner:run_for(50),
    Measurement = ar_sync_sim_runner:run_for(20),
    assert_metric_utilization(stored_cps, HealthyPeerCPS, Measurement),
    %% The stalled peer cannot complete a write, so any cumulative store
    %% progress proves that store reached the healthy peer's later ranges.
    #sim_snapshot{ chunks_stored_by_store = ChunksStoredByStore } =
        ar_sync_sim_world:snapshot(),
    ?assertEqual(?SIM_STORES, map_size(ChunksStoredByStore)),
    maps:foreach(
        fun(StoreID, ChunksStored) ->
            assert_greater_than(ChunksStored, 0,
                #{ store => StoreID, metric => chunks_stored })
        end,
        ChunksStoredByStore).

%% @doc Return the first configured simulation store ID.
first_store() ->
    [StoreID | _] = ar_sync_sim_world:store_ids(),
    StoreID.

%% @doc Return the byte range of the first configured simulation store.
first_store_range() ->
    [{_StoreID, StoreRange} | _] = ar_sync_sim_world:store_ranges(),
    StoreRange.

%% @doc Return the byte interval for a one-based chunk index relative to the
%% store start. An index of 1 identifies the store's first chunk.
single_chunk_interval(StoreStart, ChunkIndex) ->
    ChunkEnd = ar_block:get_chunk_padded_offset(
        StoreStart + ChunkIndex * ?DATA_CHUNK_SIZE),
    {ChunkEnd, ChunkEnd - ?DATA_CHUNK_SIZE}.

%% @doc Return `ChunkCount' byte intervals from the footprint containing the
%% one-based `FirstChunkIndex' relative to the store start.
footprint_chunk_intervals(StoreStart, FirstChunkIndex, ChunkCount) ->
    {FirstChunkEnd, _FirstChunkStart} = single_chunk_interval(
        StoreStart, FirstChunkIndex),
    FirstFootprintOffset = ar_footprint_record:get_offset(FirstChunkEnd),
    FootprintOffsets = lists:seq(
        FirstFootprintOffset,
        FirstFootprintOffset + ChunkCount - 1),
    lists:map(
        fun(FootprintOffset) ->
            ChunkEnd = ar_footprint_record:
                get_padded_offset_from_footprint_offset(FootprintOffset),
            {ChunkEnd, ChunkEnd - ?DATA_CHUNK_SIZE}
        end,
        FootprintOffsets).

%% @doc Return one byte interval covering consecutive sweep steps within a
%% store range.
sweep_chunk_interval({StoreStart, StoreEnd}, StartStep, StepCount) ->
    Step = ar_sync_cursor:query_range_step_size(),
    Start = StoreStart + StartStep * Step,
    End = min(StoreEnd, Start + StepCount * Step),
    {End, Start}.

%% @doc Assert that every configured store completes at least 90% of an equal
%% share. The ten-percent margin permits store-level scheduling variation while
%% rejecting material skew.
assert_chunks_spread_across_stores(Measurement) ->
    assert_chunks_spread_across_stores(Measurement, 0.9).

%% @doc Assert that every configured store completes an explicit minimum
%% fraction of an equal share of the chunks stored during the measurement.
assert_chunks_spread_across_stores(Measurement, MinimumFairShareFraction) ->
    ChunksStoredByStore = ar_sync_sim_runner:metric(
        chunks_stored_by_store, Measurement),
    TotalChunksStored = ar_sync_sim_runner:metric(chunks_stored_total, Measurement),
    assert_greater_than(TotalChunksStored, 0,
        #{ metric => chunks_stored_total }),
    ?assertEqual(?SIM_STORES, map_size(ChunksStoredByStore)),
    %% A zero fraction is the progress-only contract: every store must still
    %% complete at least one chunk.
    MinimumChunksPerStore = max(1,
        MinimumFairShareFraction * TotalChunksStored / ?SIM_STORES),
    maps:foreach(
        fun(StoreID, ChunksStored) ->
            assert_at_least(ChunksStored, MinimumChunksPerStore,
                #{ store => StoreID, metric => chunks_stored,
                    chunks_stored_by_store => ChunksStoredByStore })
        end,
        ChunksStoredByStore).

%% @doc Assert that a measured rate uses nearly all modeled sustainable capacity.
assert_metric_utilization(Metric, ExpectedValue, Measurement) ->
    ActualValue = ar_sync_sim_runner:metric(Metric, Measurement),
    MinimumValue = ?MIN_STEADY_STATE_UTILIZATION * ExpectedValue,
    assert_at_least(ActualValue, MinimumValue, #{ metric => Metric }).

assert_less_than(Actual, Expected) ->
    assert_less_than(Actual, Expected, #{}).

assert_less_than(Actual, Expected, Context) ->
    ?assert(Actual < Expected,
        maps:merge(Context, #{ actual => Actual, expected => Expected })).

assert_at_most(Actual, Expected) ->
    assert_at_most(Actual, Expected, #{}).

assert_at_most(Actual, Expected, Context) ->
    ?assert(Actual =< Expected,
        maps:merge(Context, #{ actual => Actual, expected => Expected })).

assert_greater_than(Actual, Expected, Context) ->
    ?assert(Actual > Expected,
        maps:merge(Context, #{ actual => Actual, expected => Expected })).

assert_at_least(Actual, Expected) ->
    assert_at_least(Actual, Expected, #{}).

assert_at_least(Actual, Expected, Context) ->
    ?assert(Actual >= Expected,
        maps:merge(Context, #{ actual => Actual, expected => Expected })).

%% The scenario's aggregate serving capacity, chunks per second.
capacity(Peers) ->
    lists:sum([B || #sim_peer{ max_serve_cps = B } <- maps:values(Peers)]).

%% @doc Build online gossip peers that advertise neither byte nor footprint
%% data. Discovery sees them as known peers but cannot select them for syncing.
non_serving_peers(Count) ->
    maps:from_list([
        {{10, 99, N div 250, N rem 250, 1984},
            #sim_peer{ sync_kinds = [] }}
        || N <- lists:seq(1, Count)
    ]).

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

%% Scenario: A healthy heterogeneous peer population shares useful work without
%% allowing its fastest member to monopolize sync.
%%
%% Context:
%% - One fast peer serves alongside five slower peers, including a very slow
%%   peer and peers that exercise rate-limit handling.
%%
%% Timeline:
%% - Let allocation settle, then measure all peers under unchanged conditions.
%%
%% Contract:
%% - C1: Aggregate writes approach the population's combined serving capacity.
%% - C2: The fast peer does not starve any slower peer.
%%
%% Verification:
%% - V1: Stored throughput reaches 95% of the configured aggregate capacity.
%% - V2: The fast peer stays below 60% of served work and every slower peer
%%   reaches at least 90% of its own capacity.
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
    %% Forty simulated seconds cover the scheduler evidence horizon with one
    %% grow/probe cycle of headroom; twenty more provide about 100 chunks from
    %% the slowest peer and reduce endpoint skew in the fast peer's share.
    ar_sync_sim_runner:start_sim(#sim_world{ peers = Peers }),
    ar_sync_sim_runner:run_for(40),
    Measurement = ar_sync_sim_runner:run_for(20),
    CpsByPeer = ar_sync_sim_runner:metric(cps_by_peer, Measurement),
    assert_metric_utilization(stored_cps, capacity(Peers), Measurement),
    %% The expected fast-peer share is 250 / 455 = 0.549. A 60% ceiling leaves
    %% five percentage points for allocation variation while still rejecting a
    %% material shift of slower peers' work onto the fast peer.
    ServedCPS = lists:sum(maps:values(CpsByPeer)),
    assert_less_than(
        maps:get(?PEER_UNLIMITED, CpsByPeer) / ServedCPS,
        0.60,
        #{ peer => ?PEER_UNLIMITED, metric => cps_share }),
    Minimum60CPS = 0.9 * 60,
    assert_rate_at_least(
        cps_by_peer,
        ?PEER_LIMITED_1,
        Minimum60CPS,
        Measurement),
    assert_rate_at_least(
        cps_by_peer,
        ?PEER_LIMITED_2,
        Minimum60CPS,
        Measurement),
    Minimum40CPS = 0.9 * 40,
    assert_rate_at_least(
        cps_by_peer,
        ?PEER_LIMITED_3,
        Minimum40CPS,
        Measurement),
    assert_rate_at_least(
        cps_by_peer,
        ?PEER_LIMITED_4,
        Minimum40CPS,
        Measurement),
    assert_rate_at_least(
        cps_by_peer,
        ?PEER_LIMITED_5,
        0.9 * 5,
        Measurement),
    ok.

%% Scenario: Peers that take turns timing out recover their share of work while
%% an always-healthy peer continues serving.
%%
%% Context:
%% - One healthy peer serves alongside five equal, slower peers.
%% - Each slower peer has its own recurring timeout phase.
%%
%% Timeline:
%% - Warm the scheduler, then measure one complete rotation of all five outage
%%   phases.
%%
%% Contract:
%% - C1: Every configured outage occurs and every affected peer later resumes.
%% - C2: Aggregate writes track the capacity available outside the outages.
%% - C3: The healthy peer does not permanently absorb the recovering peers' work.
%%
%% Verification:
%% - V1: Every slower peer records at least one timeout during measurement.
%% - V2: Every slower peer reaches 95% of its outage-adjusted capacity.
%% - V3: Stored throughput reaches 95% of outage-adjusted aggregate capacity.
%% - V4: The healthy peer remains below 55% of served work.
timeout_resilience_test_() ->
    ar_sync_sim_runner:setup_sim(fun test_timeout_resilience/0, 600).

test_timeout_resilience() ->
    HealthyCPS = 25,
    LimitedCPS = 5,
    PhaseTicks = 10,
    OutageTicks = 2,
    LimitedPeers = [?PEER_LIMITED_1, ?PEER_LIMITED_2, ?PEER_LIMITED_3,
        ?PEER_LIMITED_4, ?PEER_LIMITED_5],
    Peers0 = #{
        ?PEER_UNLIMITED => #sim_peer{ max_serve_cps = HealthyCPS },
        ?PEER_LIMITED_1 => #sim_peer{
            max_serve_cps = LimitedCPS, limited = true },
        ?PEER_LIMITED_2 => #sim_peer{
            max_serve_cps = LimitedCPS, limited = true },
        ?PEER_LIMITED_3 => #sim_peer{
            max_serve_cps = LimitedCPS, limited = true },
        ?PEER_LIMITED_4 => #sim_peer{
            max_serve_cps = LimitedCPS, limited = true },
        ?PEER_LIMITED_5 => #sim_peer{
            max_serve_cps = LimitedCPS, limited = true }
    },
    %% Assign each peer a different timeout cadence.
    Peers = maps:map(
        fun(Name, Peer) ->
            case arweave_util:index_of(Name, LimitedPeers) of
                undefined -> Peer;
                Index -> Peer#sim_peer{ failure_policy = fun(Tick, _RequestSequence) ->
                    case ((Tick div PhaseTicks) rem 5) =:= Index - 1
                            andalso (Tick rem PhaseTicks) < OutageTicks of
                        true -> timeout;
                        false -> none
                    end
                end }
            end
        end,
        Peers0),
    ar_sync_sim_runner:start_sim(#sim_world{ peers = Peers }),
    %% Thirty ticks establish the scheduler state before a full 50-tick
    %% five-peer outage rotation supplies the measurement window.
    ar_sync_sim_runner:run_for(30),
    RotationTicks = length(LimitedPeers) * PhaseTicks,
    Measurement = ar_sync_sim_runner:run_for(RotationTicks),
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
    %% The healthy peer is always available. Each limited peer serves 144 of
    %% the 50 measured seconds, so 25 + 5 * 5 * 48 / 50s = 49 chunks/s.
    ExpectedAvailableCPS = HealthyCPS
        + length(LimitedPeers) * LimitedCPS
            * (RotationTicks - OutageTicks) / RotationTicks,
    assert_at_least(ar_sync_sim_runner:metric(stored_cps, Measurement),
        ?MIN_STEADY_STATE_UTILIZATION * ExpectedAvailableCPS,
        #{ metric => stored_cps }),
    assert_less_than(
        maps:get(?PEER_UNLIMITED, CpsByPeer) / ServedCPS,
        0.55,
        #{ peer => ?PEER_UNLIMITED, metric => cps_share }),
    %% Each limited peer is unavailable for 2 of the 50 measured seconds, so
    %% its available capacity is 5 * 48 / 50 = 4.8 chunks/s.
    AvailableLimitedCPS = LimitedCPS
        * (RotationTicks - OutageTicks) / RotationTicks,
    MinimumLimitedCPS =
        ?MIN_STEADY_STATE_UTILIZATION * AvailableLimitedCPS,
    assert_rate_at_least(
        cps_by_peer,
        ?PEER_LIMITED_1,
        MinimumLimitedCPS,
        Measurement),
    assert_rate_at_least(
        cps_by_peer,
        ?PEER_LIMITED_2,
        MinimumLimitedCPS,
        Measurement),
    assert_rate_at_least(
        cps_by_peer,
        ?PEER_LIMITED_3,
        MinimumLimitedCPS,
        Measurement),
    assert_rate_at_least(
        cps_by_peer,
        ?PEER_LIMITED_4,
        MinimumLimitedCPS,
        Measurement),
    assert_rate_at_least(
        cps_by_peer,
        ?PEER_LIMITED_5,
        MinimumLimitedCPS,
        Measurement),
    ok.

%% Scenario: A fast peer is temporarily rate limited and then restored.
%%
%% Context:
%% - A single low-latency peer initially has a high serving capacity.
%%
%% Timeline:
%% - Measure the initial rate, lower the peer's capacity while enabling 429
%%   responses, then remove the limit and measure recovery.
%%
%% Contract:
%% - C1: While rate limited, the peer continues serving near its reduced
%%   capacity.
%% - C2: Rejected requests do not consume more request time than successful
%%   fetches.
%% - C3: Removing the limit restores the peer's original serving capacity.
%%
%% Verification:
%% - V1: The limited phase records rejected requests.
%% - V2: Limited throughput reaches 95% of reduced capacity.
%% - V3: Rejections do not exceed successful responses during the settled
%%   limited window.
%% - V4: Initial and recovered throughput each reach 95% of high capacity.
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
    %% At 400 chunks/s and 250 ms latency, 100 concurrent requests are needed
    %% to reach serving capacity. Thirty-two ticks cover the scheduler's
    %% evidence horizon.
    ar_sync_sim_runner:run_for(32),
    InitialMeasurement = ar_sync_sim_runner:run_for(10),
    assert_metric_utilization(stored_cps, HighCps, InitialMeasurement),

    LimitedPeer = Peer#sim_peer{
        max_serve_cps = LimitedCps,
        limited = true
    },
    ar_sync_sim_runner:update_world(World#sim_world{
        peers = Peers#{ ?PEER_LIMITED_1 := LimitedPeer }
    }),
    %% Twenty ticks expose requests left in flight from the higher rate and let
    %% the peer cap settle across more than half its 32-tick evidence horizon.
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

%% Scenario: A slow peer continues useful service while regularly returning
%% fast rate-limit responses.
%%
%% Context:
%% - Successful requests to the only peer take two seconds, while every eighth
%%   request is rejected after a short delay.
%%
%% Timeline:
%% - Run at the production scheduler cadence through repeated rejection cycles,
%%   then measure settled throughput.
%%
%% Contract:
%% - C1: Recurring rate-limit responses do not suppress useful throughput.
%%
%% Verification:
%% - V1: At least one request is rejected during measurement.
%% - V2: Stored throughput reaches 95% of the peer's serving capacity.
recurring_rate_limit_preserves_throughput_test_() ->
    ar_sync_sim_runner:setup_sim(
        fun test_recurring_rate_limit_preserves_throughput/0, 400).

test_recurring_rate_limit_preserves_throughput() ->
    MaxServeCps = 2,
    RejectEvery = 8,
    %% The simulator normally shortens the scheduler interval to one second.
    %% That would observe 250 ms rejections before any two-second successes and
    %% create artificial all-failure samples, so retain the production interval.
    ar_sync_scheduler:override_tick_interval_ms(10_000),
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
    %% The four productive requests plus recurring fast rejections fit within
    %% the initial eight-request cap. Twenty production observations cover
    %% discovery and repeated rejection recovery; twenty more contain about
    %% 400 successful responses at full rate.
    ar_sync_sim_runner:run_for(20),
    Measurement = ar_sync_sim_runner:run_for(20),
    assert_greater_than(
        ar_sync_sim_runner:metric(
            {rejected_by_peer, ?PEER_LIMITED_1}, Measurement),
        0,
        #{ peer => ?PEER_LIMITED_1, metric => rejected_by_peer }),
    assert_metric_utilization(stored_cps, MaxServeCps, Measurement).

%% Scenario: A healthy peer retains useful throughput despite occasional
%% full-latency client errors.
%%
%% Context:
%% - Requests to the only peer take four seconds, and every fortieth request
%%   fails after occupying the same time as a success.
%%
%% Timeline:
%% - Run at the production scheduler cadence through repeated error cycles,
%%   then measure settled throughput.
%%
%% Contract:
%% - C1: Occasional full-latency client errors do not materially amplify their
%%   unavoidable throughput loss.
%%
%% Verification:
%% - V1: At least one client error occurs during measurement.
%% - V2: Stored throughput reaches 95% of the achievable 39/40 useful rate.
recurring_client_errors_preserve_throughput_test_() ->
    ar_sync_sim_runner:setup_sim(
        fun test_recurring_client_errors_preserve_throughput/0, 400).

test_recurring_client_errors_preserve_throughput() ->
    MaxServeCPS = 2,
    ErrorEvery = 40,
    %% Match production so one scheduler observation includes both successes
    %% and errors under the four-second request latency.
    ar_sync_scheduler:override_tick_interval_ms(10_000),
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
    %% Two chunks/s with four-second request latency needs eight productive
    %% requests, exactly the initial cap. Twenty production observations cover
    %% discovery, repeated error recovery, and restoring the required in-flight
    %% concurrency. Twenty measured observations contain about 400 requests and
    %% ten one-in-forty errors at full rate.
    ar_sync_sim_runner:run_for(20),
    Measurement = ar_sync_sim_runner:run_for(20),
    assert_greater_than(
        ar_sync_sim_runner:metric(
            {client_errors_by_peer, ?PEER_UNLIMITED}, Measurement),
        0,
        #{ peer => ?PEER_UNLIMITED, metric => client_errors_by_peer }),
    %% One in forty workers returns an error after occupying the same time as a
    %% success, so the ideal useful rate is 39/40 of nominal. Retain the normal
    %% 95% utilization contract against that achievable rate.
    ExpectedUsefulCPS = MaxServeCPS * (ErrorEvery - 1) / ErrorEvery,
    assert_metric_utilization(stored_cps, ExpectedUsefulCPS, Measurement).

%% Scenario: A single peer reaches its full serving capacity while syncing a
%% fragmented local layout.
%%
%% Context:
%% - One low-latency peer holds all needed data and no other peer competes for
%%   work.
%% - Local need alternates by chunk to exercise fragmented scheduling.
%%
%% Timeline:
%% - Let discovery and concurrency growth settle, then measure the peer alone.
%%
%% Contract:
%% - C1: A queued single-peer workload grows enough fetching concurrency to use
%%   the peer's full serving rate.
%%
%% Verification:
%% - V1: Stored throughput reaches 95% of the peer's configured capacity.
single_peer_saturation_test_() ->
    ar_sync_sim_runner:setup_sim(fun test_single_peer_saturation/0, 300).

test_single_peer_saturation() ->
    MaxServeCps = 100,
    Peers = #{ ?PEER_UNLIMITED => #sim_peer{ max_serve_cps = MaxServeCps } },
    %% Retain one end-to-end scenario with the adversarial alternating-chunk
    %% local layout; other scenarios model the contiguous need of a fresh store.
    ar_sync_sim_runner:start_sim(#sim_world{ peers = Peers, local_data_layout = fragmented }),
    %% Thirty-two simulated seconds cover the scheduler evidence horizon.
    ar_sync_sim_runner:run_for(32),
    %% Ten seconds contain 1000 chunks at full rate, enough to average the
    %% fragmented store layout without extending the scheduler warmup.
    Measurement = ar_sync_sim_runner:run_for(20),
    assert_metric_utilization(stored_cps, MaxServeCps, Measurement).

%% Scenario: A peer near its outbound request quota remains usable when it is
%% the only source of needed data.
%%
%% Context:
%% - The only candidate peer holds all useful work and is marked throttled, so
%%   no unthrottled alternative exists.
%% - This is the fallback case; normal selection prefers unthrottled peers when
%%   any are available.
%%
%% Timeline:
%% - Warm discovery and concurrency growth, then measure service.
%%
%% Contract:
%% - C1: When every candidate peer is throttled, selection falls back to the
%%   full candidate set rather than stopping sync.
%%
%% Verification:
%% - V1: Stored throughput reaches 95% of the peer's serving capacity.
single_throttled_peer_keeps_progressing_test_() ->
    ar_sync_sim_runner:setup_sim(
        fun test_single_throttled_peer_keeps_progressing/0, 300).

test_single_throttled_peer_keeps_progressing() ->
    MaxServeCps = 100,
    Peers = #{
        ?PEER_UNLIMITED => #sim_peer{
            max_serve_cps = MaxServeCps,
            is_throttled = true
        }
    },
    ar_sync_sim_runner:start_sim(#sim_world{ peers = Peers }),
    %% Ten seconds cover the fixed sweep-range warm wait; ten more let the
    %% scheduler grow the only peer's request cap before measurement.
    ar_sync_sim_runner:run_for(20),
    Measurement = ar_sync_sim_runner:run_for(20),
    assert_metric_utilization(stored_cps, MaxServeCps, Measurement).

%% Scenario: A peer first gains serving capacity and then becomes slower to
%% respond without losing that capacity.
%%
%% Context:
%% - One peer starts with a low rate and short response latency.
%%
%% Timeline:
%% - Increase serving capacity fourfold and measure it, then increase response
%%   latency fourfold and measure the unchanged serving rate again.
%%
%% Contract:
%% - C1: The scheduler uses newly available serving capacity.
%% - C2: It adds enough concurrency to retain that rate after latency grows.
%%
%% Verification:
%% - V1: The post-capacity-change window reaches 95% of the higher rate.
%% - V2: The post-latency-change window also reaches 95% of the higher rate.
peer_capacity_and_latency_growth_test_() ->
    ar_sync_sim_runner:setup_sim(
        fun test_peer_capacity_and_latency_growth/0, 400).

test_peer_capacity_and_latency_growth() ->
    InitialServeCPS = 8,
    IncreasedServeCPS = 32,
    InitialLatencyMS = 2 * ?SIM_SUBSTEP_MS,
    IncreasedLatencyMS = 4 * InitialLatencyMS,
    Peer = #sim_peer{
        max_serve_cps = InitialServeCPS,
        latency_ms = InitialLatencyMS
    },
    Peers = #{ ?PEER_UNLIMITED => Peer },
    World = #sim_world{ peers = Peers },
    %% Production observations keep both the half-second and two-second request
    %% latencies inside a complete scheduler sample.
    ar_sync_scheduler:override_tick_interval_ms(10_000),
    ar_sync_sim_runner:start_sim(World),
    %% Four observations establish the initial eight chunks/s operating state.
    ar_sync_sim_runner:run_for(4),
    FasterPeer = Peer#sim_peer{ max_serve_cps = IncreasedServeCPS },
    ar_sync_sim_runner:update_world(World#sim_world{ peers = Peers#{
        ?PEER_UNLIMITED := FasterPeer
    } }),
    %% At half-second request latency, the faster rate needs 16 in-flight
    %% requests. Four observations cover the one eight-request increase beyond
    %% the initial cap and reaching that concurrency.
    ar_sync_sim_runner:run_for(4),
    CapacityMeasurement = ar_sync_sim_runner:run_for(2),
    assert_metric_utilization(stored_cps, IncreasedServeCPS, CapacityMeasurement),
    ar_sync_sim_runner:update_world(World#sim_world{ peers = Peers#{
        ?PEER_UNLIMITED := FasterPeer#sim_peer{
            latency_ms = IncreasedLatencyMS
        }
    } }),
    %% At 32 chunks/s and two-second latency, the peer needs about 64 in-flight
    %% requests. Eight observations cover six eight-request increases from the
    %% faster rate's minimum useful concurrency plus restoring the required
    %% in-flight concurrency.
    ar_sync_sim_runner:run_for(8),
    LatencyMeasurement = ar_sync_sim_runner:run_for(2),
    assert_metric_utilization(stored_cps, IncreasedServeCPS, LatencyMeasurement).

%% Scenario: Sync from one fast peer recovers after the local HTTP client's
%% concurrency limit changes from unusable to sufficient.
%%
%% Context:
%% - The local HTTP client initially permits no active requests to the peer,
%%   then permits enough concurrency to reach the peer's serving capacity.
%%
%% Timeline:
%% - Observe the forced failure phase, raise the HTTP limit, wait for recovery,
%%   then measure useful service.
%%
%% Contract:
%% - C1: Sync returns to the peer's serving capacity once the local HTTP client
%%   permits sufficient concurrency.
%%
%% Verification:
%% - V1: The zero-limit phase records client errors.
%% - V2: Recovered stored throughput reaches 95% of capacity and recovered
%%   client errors do not outnumber successful responses.
client_error_recovery_with_http_headroom_test_() ->
    ar_sync_sim_runner:setup_sim(
        fun test_client_error_recovery_with_http_headroom/0, 400).

test_client_error_recovery_with_http_headroom() ->
    MaxServeCps = 100,
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
    %% At 100 chunks/s and 250 ms latency, about 25 concurrent requests are
    %% needed to reach serving capacity. A limit of 30 permits full throughput
    %% with modest headroom.
    ar_sync_sim_runner:update_world(World#sim_world{ peers = Peers#{
        ?PEER_UNLIMITED := Peer#sim_peer{ http_inflight_limit = 30 }
    } }),
    %% Twenty ticks are well beyond the short client-error recovery memory.
    ar_sync_sim_runner:run_for(20),
    Measurement = ar_sync_sim_runner:run_for(10),
    Errors = ar_sync_sim_runner:metric(
        {client_errors_by_peer, ?PEER_UNLIMITED}, Measurement),
    Served = ar_sync_sim_runner:metric(
        {served_by_peer, ?PEER_UNLIMITED}, Measurement),
    assert_metric_utilization(stored_cps, MaxServeCps, Measurement),
    %% Client errors and successes have equal latency in this scenario. Failures
    %% must not consume more request time than useful delivery.
    assert_at_most(Errors, Served,
        #{ peer => ?PEER_UNLIMITED, metric => client_errors_by_peer }).

%% Scenario: A formerly fast peer slows sharply while three sibling peers keep
%% their original rates.
%%
%% Context:
%% - One fast peer and three equal rate-limited peers initially serve together.
%%
%% Timeline:
%% - Establish the fast peer's high-rate state, reduce its rate and increase its
%%   latency, then measure after stale work settles.
%%
%% Contract:
%% - C1: Work assigned at the old rate does not starve unchanged siblings.
%% - C2: The changed peer remains useful near its new capacity.
%%
%% Verification:
%% - V1: The fast peer reaches 95% of its initial capacity before the change.
%% - V2: Each unchanged peer reaches 90% of capacity and aggregate throughput
%%   reaches the sum of those floors plus the changed peer's floor.
%% - V3: The changed peer reaches 95% of its new capacity without exceeding it
%%   by more than measurement quantization.
fast_peer_turns_slow_test_() ->
    ar_sync_sim_runner:setup_sim(fun test_fast_peer_turns_slow/0, 300).

test_fast_peer_turns_slow() ->
    InitialFastCPS = 300,
    SiblingCPS = 60,
    Peer = #sim_peer{ max_serve_cps = InitialFastCPS },
    Peers = #{
        ?PEER_UNLIMITED => Peer,
        ?PEER_LIMITED_1 => #sim_peer{
            max_serve_cps = SiblingCPS, limited = true },
        ?PEER_LIMITED_2 => #sim_peer{
            max_serve_cps = SiblingCPS, limited = true },
        ?PEER_LIMITED_3 => #sim_peer{
            max_serve_cps = SiblingCPS, limited = true }
    },
    World = #sim_world{ peers = Peers },
    ar_sync_sim_runner:start_sim(World),
    %% Establish the peer's high-rate state before reducing its serving capacity.
    ar_sync_sim_runner:run_for(32),
    InitialMeasurement = ar_sync_sim_runner:run_for(10),
    assert_metric_utilization(
        {cps_by_peer, ?PEER_UNLIMITED}, InitialFastCPS, InitialMeasurement),
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
    %% provide 190 chunks/s. Ongoing rate-limit probes permit 90% from each
    %% unchanged peer; the collapsed peer must retain 95% of its new capacity.
    MinimumUnchangedPeerCPS = 0.9 * SiblingCPS,
    MinimumCollapsedPeerCPS =
        ?MIN_STEADY_STATE_UTILIZATION * CollapsedCps,
    MinimumAggregateCPS = 3 * MinimumUnchangedPeerCPS
        + MinimumCollapsedPeerCPS,
    assert_at_least(ar_sync_sim_runner:metric(stored_cps, Measurement),
        MinimumAggregateCPS, #{ metric => stored_cps }),
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
        MinimumCollapsedPeerCPS,
        #{ peer => ?PEER_UNLIMITED, metric => cps }).

%% Scenario: A peer recovers useful allocation after a prolonged timeout-only
%% period beside a healthy peer.
%%
%% Context:
%% - One high-capacity peer stays healthy while a smaller peer times out for much
%%   longer than normal scheduler adaptation periods.
%%
%% Timeline:
%% - Confirm the peer is unavailable before its recovery boundary, then let it
%%   recover and measure both peers together.
%%
%% Contract:
%% - C1: Prolonged failure does not permanently remove a peer from exploration.
%% - C2: Aggregate service returns to the combined healthy capacity.
%%
%% Verification:
%% - V1: The outage produces timeouts and less than one chunk/s from that peer.
%% - V2: The recovered peer reaches 90% of its own capacity.
%% - V3: Stored throughput reaches 95% of combined capacity.
starved_peer_recovers_test_() ->
    ar_sync_sim_runner:setup_sim(fun test_starved_peer_recovers/0, 300).

test_starved_peer_recovers() ->
    HealthyCPS = 200,
    RecoveredCps = 40,
    Peers = #{
        ?PEER_UNLIMITED => #sim_peer{ max_serve_cps = HealthyCPS },
        ?PEER_TIMEOUT => #sim_peer{
            max_serve_cps = RecoveredCps,
            limited = true,
            %% Tick indices are zero-based, so 0..39 is exactly 40 ticks.
            failure_policy = fun(Tick, _RequestSequence) ->
                case Tick < 40 of
                    true -> timeout;
                    false -> none
                end
            end
        }
    },
    ar_sync_sim_runner:start_sim(#sim_world{ peers = Peers }),
    %% Thirty-nine ticks exceed the scheduler evidence horizon while ending
    %% strictly before tick 40 recovers.
    OutageMeasurement = ar_sync_sim_runner:run_for(20),
    Timeouts = ar_sync_sim_runner:metric(
        {timed_out_by_peer, ?PEER_TIMEOUT}, OutageMeasurement),
    assert_greater_than(Timeouts, 0,
        #{ peer => ?PEER_TIMEOUT, metric => timed_out_by_peer }),
    StarvedCps = ar_sync_sim_runner:metric(
        {cps_by_peer, ?PEER_TIMEOUT}, ar_sync_sim_runner:run_for(19)),
    assert_less_than(StarvedCps, 1,
        #{ peer => ?PEER_TIMEOUT, metric => cps }),
    %% Tick 40 begins recovery. Forty control intervals cover a complete
    %% scheduler evidence horizon with probe headroom; twenty more average
    %% dispatch/refill boundaries for the lower-rate recovered peer.
    ar_sync_sim_runner:run_for(40),
    Measurement = ar_sync_sim_runner:run_for(20),
    CpsByPeer = ar_sync_sim_runner:metric(cps_by_peer, Measurement),
    %% The always-healthy and recovered peers provide their combined capacity.
    assert_metric_utilization(
        stored_cps, HealthyCPS + RecoveredCps, Measurement),
    RecoveredPeerCps = maps:get(?PEER_TIMEOUT, CpsByPeer),
    %% A 90% peer floor distinguishes useful recovery from residual starvation;
    %% the aggregate 95% assertion above catches broader throughput loss.
    assert_at_least(
        RecoveredPeerCps,
        0.9 * RecoveredCps,
        #{ peer => ?PEER_TIMEOUT, metric => cps }).

%% Scenario: All local completion paths stall and later recover while a mixed
%% peer population remains available.
%%
%% Context:
%% - Six heterogeneous peers share a limited chunk cache.
%%
%% Timeline:
%% - Warm at full rate, stop all local completions long enough to fill the chunk
%%   cache, then restore completions and measure recovery.
%%
%% Contract:
%% - C1: Once chunk-cache capacity is fully committed during the local stall,
%%   fetch throughput settles near zero.
%% - C2: Once the local stall is removed, aggregate throughput recovers and
%%   every peer resumes useful service.
%%
%% Verification:
%% - V1: Cached and inflight chunks reach the cache limit, and settled fetch
%%   throughput during the stall is at most 5% of peer capacity.
%% - V2: Recovered stored throughput reaches 95% of aggregate capacity and every
%%   slower peer reaches at least 80% of its own capacity.
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
    World = #sim_world{
        peers = Peers,
        %% 316 MiB provides 1264 chunk slots before the 64 MiB interval-cache
        %% floor removes 256. The remaining 1008 slots divide evenly into six
        %% 168-chunk store shares, so stalled stores can fill the global cache.
        node_config = #{ [sync, cache_size] => 316 }
    },
    ar_sync_sim_runner:start_sim(World),
    %% Forty warmup ticks cover the 32-tick scheduler adaptation period.
    ar_sync_sim_runner:run_for(40),
    ar_sync_sim_runner:update_world(World#sim_world{
        %% Zero disables writes across all stores.
        store_write_cps = 0
    }),
    %% At 470 chunks/s the 1008-chunk cache represents about 2.1 seconds of
    %% delivered work. Ten ticks cover several bounded cache-fill durations.
    ar_sync_sim_runner:run_for(10),
    %% Five additional ticks measure the stalled state. Five percent permits at
    %% most 23.5 chunks/s of stragglers from the 470 chunks/s peer capacity.
    FrozenMeasurement = ar_sync_sim_runner:run_for(5),
    ?assert(ar_sync_deps:is_chunk_cache_full()),
    FrozenCps = lists:sum(maps:values(
        ar_sync_sim_runner:metric(cps_by_peer, FrozenMeasurement))),
    assert_at_most(FrozenCps, 0.05 * capacity(Peers)),
    ar_sync_sim_runner:update_world(World),
    %% Five ticks allow the delayed writes to complete and fetching to restart.
    ar_sync_sim_runner:run_for(5),
    %% Twenty ticks provide 400 chunks from the slowest 20 chunks/s peer. Its 10%
    %% margin is 40 chunks, allowing two one-second capacity buckets to cross the
    %% measurement boundary without hiding persistent post-stall starvation.
    Measurement = ar_sync_sim_runner:run_for(20),
    CpsByPeer = ar_sync_sim_runner:metric(cps_by_peer, Measurement),
    assert_metric_utilization(stored_cps, capacity(Peers), Measurement),
    %% The aggregate retains the 95% throughput contract above. A four-fifths
    %% per-peer floor separately requires every queue to recover most of its
    %% capacity instead of leaving a small peer starved near zero.
    Minimum60CPS = 0.8 * 60,
    assert_at_least(
        maps:get(?PEER_LIMITED_1, CpsByPeer),
        Minimum60CPS,
        #{ peer => ?PEER_LIMITED_1, metric => cps }),
    assert_at_least(
        maps:get(?PEER_LIMITED_2, CpsByPeer),
        Minimum60CPS,
        #{ peer => ?PEER_LIMITED_2, metric => cps }),
    Minimum40CPS = 0.8 * 40,
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
        0.8 * 20,
        #{ peer => ?PEER_LIMITED_5, metric => cps }).

%% Scenario: Several consistently slow stores share a chunk cache with
%% healthy stores.
%%
%% Context:
%% - Four of six stores complete slowly but never stall; two remain unbounded.
%% - A heterogeneous peer population can otherwise fill the chunk cache.
%%
%% Timeline:
%% - Run long enough for the slow stores to fill the cache if claims are not
%%   isolated, then measure aggregate progress.
%%
%% Contract:
%% - C1: Accumulation across several slow stores cannot fill the chunk cache.
%% - C2: Healthy stores preserve the peers' aggregate useful throughput.
%%
%% Verification:
%% - V1: The chunk cache is not full before or after measurement.
%% - V2: Stored throughput reaches 95% of aggregate peer capacity.
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
    %% 564 MiB resolves to a cache holding 2000 chunks: large enough to carry the
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
    %% Twenty seconds contain at least 800 chunks at the smallest 40 chunks/s
    %% rate and keep one tick boundary from deciding the utilization result.
    Measurement = ar_sync_sim_runner:run_for(20),
    assert_metric_utilization(stored_cps, capacity(Peers), Measurement),
    ?assertNot(ar_sync_deps:is_chunk_cache_full()).

%% Scenario: New healthy peer/store paths become available after older store
%% paths have stalled under chunk-cache pressure.
%%
%% Context:
%% - Six independent peer/store paths exist; four warm first and later stop
%%   completing writes while two initially reject all fetches.
%% - All paths share a deliberately limited chunk cache.
%%
%% Timeline:
%% - Warm the first four paths, stall their stores, allow their old claims to
%%   accumulate, then enable and measure the remaining two paths.
%%
%% Contract:
%% - C1: Stalled store paths cannot monopolize capacity needed by newly usable
%%   healthy paths.
%% - C2: Both healthy stores make material progress.
%% - C3: Several stalled stores cannot fill the shared chunk cache.
%%
%% Verification:
%% - V1: Every initially healthy path reaches 95% of its serving capacity.
%% - V2: Every stalled store accumulates pending writes before new paths open.
%% - V3: The healthy stores together reach 95% of their combined capacity.
%% - V4: Each healthy store reaches at least one quarter of an equal share.
%% - V5: The shared chunk cache remains below its global capacity throughout
%%   the stalled-store and recovered measurement phases.
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
                    %% Zero HTTP slots keeps the peer unavailable without
                    %% withholding its metadata from the sweeper.
                    Peer#sim_peer{ http_inflight_limit = 0 };
                false ->
                    Peer
            end
        end,
        Peers),
    %% Four independent peer/store paths first warm at 50 chunks/s, then their
    %% stores stall. The other two peer/store paths begin accepting requests only
    %% after the warm peers have had enough time to fill the chunk cache.
    HealthyStores = [StoreID || {_N, {StoreID, _Range}} <- HealthyPeerStoreRanges],
    World = #sim_world{
        peers = InitiallyUnavailablePeers,
        %% A 564 MiB sync budget resolves to a cache holding 2000 chunks. Four
        %% old one-quarter claim allowances can fill it. The six storage modules
        %% give each low-pressure path a 2000 / 6 = 333-chunk eligibility
        %% threshold.
        node_config = #{ [sync, cache_size] => 564 }
    },
    ar_sync_sim_runner:start_sim(World),
    %% Twenty ticks let each peer demonstrate its full serving capacity before
    %% its store stalls.
    ar_sync_sim_runner:run_for(20),
    InitialMeasurement = ar_sync_sim_runner:run_for(10),
    lists:foreach(
        fun({N, {_StoreID, _Range}}) ->
            PeerID = {10, 2, 0, N, 1984},
            assert_metric_utilization(
                {cps_by_peer, PeerID}, PeerCPS, InitialMeasurement)
        end,
        SlowPeerStoreRanges),
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
    ?assertNot(ar_sync_deps:is_chunk_cache_full()),
    lists:foreach(
        fun(StoreID) ->
            assert_greater_than(
                ar_sync_sim_world:chunk_cache_size(StoreID),
                0,
                #{ store => StoreID, metric => chunk_cache_size })
        end,
        SlowStores),
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
        HealthyStores),
    ?assertNot(ar_sync_deps:is_chunk_cache_full()).

%% Scenario: A heterogeneous peer population shares a binding node-wide
%% download limit.
%%
%% Context:
%% - Fast, high-latency, rate-limited, and periodically timing-out peers can
%%   collectively exceed the configured budget.
%%
%% Timeline:
%% - Warm through two flaky-peer cycles, then measure one complete cycle.
%%
%% Contract:
%% - C1: The population nearly fills the budget without exceeding it.
%% - C2: No peer monopolizes the budget and every peer class remains active.
%%
%% Verification:
%% - V1: Both timeout and 429 paths occur during the scenario.
%% - V2: Stored throughput reaches 95% of budget and served throughput remains
%%   below the one-chunk/s boundary allowance.
%% - V3: Every peer stays below 85% of budget and above its starvation floor.
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

%% Scenario: One peer supplies six independently limited remote store paths.
%%
%% Context:
%% - Every remote path and destination store has the same finite rate while the
%%   peer-wide limit has excess capacity.
%%
%% Timeline:
%% - Warm discovery and per-store request depth, then measure all paths together.
%%
%% Contract:
%% - C1: Requests use the remote store paths' combined capacity.
%% - C2: Work is spread across stores rather than concentrated on a subset.
%%
%% Verification:
%% - V1: Stored throughput reaches 95% of aggregate store capacity and served
%%   throughput does not exceed its boundary-adjusted ceiling.
%% - V2: Every store remains within the default 90% equal-share floor.
single_peer_store_spread_test_() ->
    ar_sync_sim_runner:setup_sim(fun test_single_peer_store_spread/0, 400).

test_single_peer_store_spread() ->
    StoreCps = 4,
    %% At four chunks/s per store and two seconds of latency, each store needs
    %% about eight concurrent requests to use its remote read capacity.
    Peers = #{ ?PEER_UNLIMITED => #sim_peer{
        max_serve_cps = 96,
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
    %% Thirty simulated seconds cover discovery and the five eight-request
    %% increases needed beyond the initial cap to serve all six store paths.
    ar_sync_sim_runner:run_for(30),
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

%% Scenario: One footprint peer supplies six independently limited destination
%% stores through a small shared entropy cache.
%%
%% Context:
%% - Every destination store has the same finite write rate and the peer-wide
%%   network path has excess capacity.
%% - The entropy cache has one fair slot per store plus limited global headroom.
%%
%% Timeline:
%% - Warm through discovery and several footprint transitions, then measure
%%   repeated partial assignments across all stores.
%%
%% Contract:
%% - C1: Partial assignments for one store do not monopolize enough entropy
%%   slots to starve other stores.
%% - C2: The peer uses the stores' combined write capacity.
%%
%% Verification:
%% - V1: Every store remains within the default 90% equal-share floor.
%% - V2: Stored throughput reaches 95% of aggregate store write capacity.
single_footprint_peer_store_spread_test_() ->
    ar_sync_sim_runner:setup_sim(
        fun test_single_footprint_peer_store_spread/0, 500).

test_single_footprint_peer_store_spread() ->
    use_footprint_size(16),
    StoreCPS = 2,
    %% Six stores at two chunks/s provide twelve chunks/s. The peer's larger
    %% network limit ensures the store paths determine expected throughput.
    AggregateStoreCPS = ?SIM_STORES * StoreCPS,
    Peer = #sim_peer{
        max_serve_cps = 48,
        latency_ms = 2000,
        sync_kinds = [footprint]
    },
    ar_sync_sim_runner:start_sim(#sim_world{
        peers = #{?PEER_UNLIMITED => Peer},
        %% Three remote chunks/s is 3/2 of the destination's write rate, giving
        %% the source store 50% headroom. This lets it refill ordinary footprint
        %% handoff gaps while destination-store writes remain the bottleneck.
        remote_store_cps = 3,
        store_write_cps = StoreCPS,
        node_config = #{
            %% Sixteen chunks make a four-MiB footprint. Thirty-two MiB provides eight
            %% slots: one fair slot per store plus two slots of global headroom
            %% that repeated work for one store cannot consume.
            [packing, entropy, cache_size] => 32
        }
    }),
    %% At two seconds of latency the six store paths need about 24 active
    %% requests. Thirty seconds cover two eight-request increases, discovery,
    %% and at least three complete footprint transitions per store.
    ar_sync_sim_runner:run_for(30),
    %% Thirty measured seconds contain sixty writes and about four complete
    %% footprint transitions per store, exposing repeated partial assignments.
    Measurement = ar_sync_sim_runner:run_for(30),
    assert_chunks_spread_across_stores(Measurement),
    assert_metric_utilization(stored_cps, AggregateStoreCPS, Measurement).

%% Scenario: Footprint writes rotate among three pairs of stores while every
%% store retains the same average write capacity.
%%
%% Context:
%% - One footprint peer has excess capacity over six destination stores.
%% - Each store writes one three-second batch and then pauses for two seconds;
%%   a different pair writes in each phase.
%%
%% Timeline:
%% - Warm through discovery and rate adaptation, then measure many complete
%%   three-phase cycles.
%%
%% Contract:
%% - C1: Short per-store pauses do not reduce aggregate useful throughput.
%% - C2: Rotating write headroom does not skew long-term progress by store.
%%
%% Verification:
%% - V1: Stored throughput reaches 95% of aggregate average write capacity.
%% - V2: Every store remains within the default 90% equal-share floor.
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
    %% One hundred twenty one-second ticks cover metadata discovery, capacity
    %% growth, and the footprint-transition settling observed by this bursty
    %% scenario. The 60-second measurement contains twenty complete
    %% three-second burst cycles and makes endpoint backlog less than five
    %% percent of total capacity.
    ar_sync_sim_runner:run_for(120),
    Measurement = ar_sync_sim_runner:run_for(60),
    assert_chunks_spread_across_stores(Measurement),
    assert_metric_utilization(stored_cps, AggregateStoreCPS, Measurement).

%% Scenario: Delayed fetch waves must not amplify a transient local backlog into
%% a persistent throughput collapse.
%%
%% Context:
%% - One footprint peer has excess capacity over six finite-rate local
%%   completion paths.
%% - The simulator reduces a store's completion rate after its fetched backlog
%%   exceeds a generous pressure threshold.
%%
%% Timeline:
%% - Establish full throughput at short latency, increase fetch latency to five
%%   seconds, and verify the peer reaches its full rate.
%% - Add peer headroom and finite local completion rates, then measure recurring
%%   response waves under backlog pressure.
%%
%% Contract:
%% - C1: The scheduler grows enough concurrency to use the delayed peer's full
%%   serving capacity.
%% - C2: Each store keeps making a fair share of progress under delayed waves.
%% - C3: Backlog pressure does not trigger a sustained aggregate collapse.
%%
%% Verification:
%% - V1: Delayed serving and stored throughput each reach 95% of peer capacity.
%% - V2: Under finite local completion, stored throughput reaches 95% of
%%   aggregate completion capacity.
%% - V3: Every store remains within the default 90% equal-share floor.
%% - V4: Every individual pressured observation retains at least 70% of normal
%%   aggregate completion capacity.
%%
%% Modeling note:
%% - The completion rate represents the entire local path after fetch, not raw
%%   disk bandwidth. Its synthetic slowdown makes excessive backlog observable
%%   through throughput; it does not claim that production disks slow this way.
delayed_fetch_waves_do_not_amplify_store_backlog_test_() ->
    ar_sync_sim_runner:setup_sim(
        fun test_delayed_fetch_waves_do_not_amplify_store_backlog/0, 500).

test_delayed_fetch_waves_do_not_amplify_store_backlog() ->
    ar_sync_sim_world:use_mainnet_replica_2_9_sizes(),
    ar_sync_scheduler:override_tick_interval_ms(10_000),
    StoreCPS = 4,
    AggregateStoreCPS = ?SIM_STORES * StoreCPS,
    PeerCPS = 100,
    AssignmentAllowanceSeconds = 11,
    %% The production pipeline permits five seconds of cached writes, five
    %% seconds of delayed fetches, and one admission-boundary second. Trigger
    %% the synthetic pressure penalty only after that 44-chunk hard bound.
    BacklogPressureThreshold = AssignmentAllowanceSeconds * StoreCPS,
    PressuredCompletionCPS = 1,
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
        %% This phase isolates delayed-peer saturation from finite local paths.
        max_serve_cps = PeerCPS,
        latency_ms = InitialLatencyMS,
        %% A complete footprint arrives every 250 ms, keeping
        %% discovery outside this fetched-backlog pressure scenario.
        chunk_interval_latency_ms = ?SIM_SUBSTEP_MS,
        sync_kinds = [footprint]
    },
    Peers = #{?PEER_UNLIMITED => Peer},
    World = #sim_world{
        peers = Peers,
        node_config = #{
            %% Twelve mainnet footprint slots let every store overlap one
            %% transition; this isolates fetched-backlog pressure from entropy.
            [packing, entropy, cache_size] => 3072
        }
    },
    ar_sync_sim_runner:start_sim(World),
    %% Twelve observations establish full throughput before latency creates
    %% larger completion waves. Five-second responses need about 500 active
    %% requests at 100 chunks/s, over sixty times the initial exploration cap.
    ar_sync_sim_runner:run_for(12),
    DelayedPeer = Peer#sim_peer{ latency_ms = DelayedLatencyMS },
    DelayedWorld = World#sim_world{ peers = Peers#{
        ?PEER_UNLIMITED := DelayedPeer
    } },
    ar_sync_sim_runner:update_world(DelayedWorld),
    %% One hundred sixty observations cover the conservative high-latency
    %% growth horizon used by the former standalone saturation case. Forty
    %% measured observations retain its store-spread contract at one-third of
    %% the former measurement length.
    ar_sync_sim_runner:run_for(160),
    SaturationMeasurement = ar_sync_sim_runner:run_for(40),
    assert_chunks_spread_across_stores(SaturationMeasurement),
    assert_metric_utilization(
        {cps_by_peer, ?PEER_UNLIMITED},
        PeerCPS,
        SaturationMeasurement),
    assert_metric_utilization(
        stored_cps, PeerCPS, SaturationMeasurement),

    %% A twofold peer margin now makes finite local completion the bottleneck.
    PressurePeer = DelayedPeer#sim_peer{
        max_serve_cps = 2 * AggregateStoreCPS
    },
    TransitionWorld = DelayedWorld#sim_world{
        peers = Peers#{ ?PEER_UNLIMITED := PressurePeer }
    },
    ar_sync_sim_runner:update_world(TransitionWorld),
    %% Let high-rate requests finish and the scheduler observe the lower peer
    %% ceiling before finite local completion makes backlog pressure meaningful.
    ar_sync_sim_runner:run_for(32),
    PressureWorld = TransitionWorld#sim_world{
        peers = Peers#{ ?PEER_UNLIMITED := PressurePeer },
        %% Each remote source has 25% headroom over its local completion path.
        remote_store_cps = 5 * StoreCPS div 4,
        store_write_cps = StoreCompletionCPS
    },
    ar_sync_sim_runner:update_world(PressureWorld),
    %% Forty observations settle completion-rate sampling before ten individual
    %% observations expose recurring pressure without hiding a collapsed wave.
    ar_sync_sim_runner:run_for(40),
    Measurements = [ar_sync_sim_runner:run_for(1)
        || _ <- lists:seq(1, 10)],
    Measurement = combine_measurements(Measurements),
    assert_chunks_spread_across_stores(Measurement),
    assert_metric_utilization(stored_cps, AggregateStoreCPS, Measurement),
    lists:foreach(
        fun(DelayedMeasurement) ->
            assert_at_least(
                ar_sync_sim_runner:metric(stored_cps, DelayedMeasurement),
                0.7 * AggregateStoreCPS,
                #{ metric => stored_cps })
        end,
        Measurements).

%% Scenario: Local completion capacity rises after the scheduler has observed a
%% lower batched rate.
%%
%% Context:
%% - One delayed peer feeds six stores whose completions arrive in four-second
%%   batches.
%% - The initial batches expose a lower average rate than the later batches.
%%
%% Timeline:
%% - Establish the low observation, increase each store's batch size, allow
%%   concurrency to recover, then measure complete recovered batches.
%%
%% Contract:
%% - C1: A low local-completion observation does not become a permanent ceiling.
%% - C2: Every store participates in the recovered rate.
%%
%% Verification:
%% - V1: The initial observation reaches its configured low rate and remains
%%   materially below the recovered rate.
%% - V2: Total recovered writes reach the 95% aggregate floor, minus one allowed
%%   measurement-boundary completion.
%% - V3: Every store remains within the default 90% equal-share floor.
%%
%% Modeling note:
%% - The changing completion rate represents temporary contention anywhere in
%%   the end-to-end local path, not an abrupt change in physical disk speed.
low_local_completion_observation_does_not_limit_recovery_test_() ->
    ar_sync_sim_runner:setup_sim(
        fun test_low_local_completion_observation_does_not_limit_recovery/0,
        500).

test_low_local_completion_observation_does_not_limit_recovery() ->
    StoreCPS = 5,
    AggregateStoreCPS = ?SIM_STORES * StoreCPS,
    BatchPeriod = 4,
    %% Twelve chunks per four seconds is three chunks/s, or 60% of the later
    %% five chunks/s observation.
    LowObservationBatchSize = 12,
    RecoveredBatchSize = StoreCPS * BatchPeriod,
    %% Fifteen low-rate batches provide sixty seconds for the delayed request
    %% pipeline to settle before the completion rate changes.
    RecoveryTick = 60,
    StoreCompletionCPS = fun(_StoreID, Tick) ->
        case Tick rem BatchPeriod of
            0 when Tick < RecoveryTick -> LowObservationBatchSize;
            0 -> RecoveredBatchSize;
            _ -> 0
        end
    end,
    Peer = #sim_peer{
        %% Four seconds of response latency requires about 120 active requests
        %% to use the six stores' combined 30 chunks/s capacity.
        max_serve_cps = 120,
        latency_ms = 4000
    },
    ar_sync_sim_runner:start_sim(#sim_world{
        peers = #{?PEER_UNLIMITED => Peer},
        %% Serving at 7/5 of the recovered local rate gives each remote source
        %% 40% headroom, so recovered local completions set expected throughput.
        remote_store_cps = 7 * StoreCPS div 5,
        store_write_cps = StoreCompletionCPS
    }),
    %% Ten four-second batches warm the initial completion path. The final five
    %% low-rate batches are measured before the simulated local path recovers.
    LowMeasurementSeconds = 5 * BatchPeriod,
    LowWarmupSeconds = RecoveryTick - LowMeasurementSeconds,
    ar_sync_sim_runner:run_for(LowWarmupSeconds),
    LowMeasurement = ar_sync_sim_runner:run_for(LowMeasurementSeconds),
    ExpectedLowCPS = ?SIM_STORES * LowObservationBatchSize / BatchPeriod,
    LowStoredCPS = ar_sync_sim_runner:metric(stored_cps, LowMeasurement),
    assert_at_least(
        LowStoredCPS,
        ?MIN_STEADY_STATE_UTILIZATION * ExpectedLowCPS,
        #{ metric => initial_stored_cps }),
    %% The midpoint between the configured 60% low rate and full recovery
    %% distinguishes a real low observation from an accidentally recovered one.
    assert_less_than(
        LowStoredCPS,
        0.8 * AggregateStoreCPS,
        #{ metric => initial_stored_cps }),
    %% Forty seconds cover ten full-capacity batches and fourteen eight-request
    %% increases from the initial cap to the concurrency required by four-second
    %% request latency.
    ar_sync_sim_runner:run_for(40),
    %% Twenty seconds average five complete batches from every store.
    Measurement = ar_sync_sim_runner:run_for(20),
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

%% Scenario: Four fast peers share an unadvertised downlink bottleneck.
%%
%% Context:
%% - The peers' combined serving capacity greatly exceeds a shared link whose
%%   limit is not exposed to the scheduler.
%%
%% Timeline:
%% - Let requests queue and allocation settle behind the link, then measure a
%%   long interval that smooths link-capacity quantization.
%%
%% Contract:
%% - C1: Useful throughput fills but does not exceed the hidden link capacity.
%% - C2: Every peer keeps making progress even if the split is uneven.
%% - C3: The hidden bottleneck does not cause unbounded active HTTP work.
%%
%% Verification:
%% - V1: Stored throughput reaches 95% of link capacity and served throughput
%%   stays below its boundary-adjusted ceiling.
%% - V2: Every peer serves at least one chunk/s.
%% - V3: Final HTTP inflight work stays within eleven seconds of link capacity.
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
    %% Forty ticks cover the scheduler evidence horizon while leaving enough
    %% carry-over to exceed the ceiling transiently.
    ar_sync_sim_runner:run_for(40),
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
    %% Across thirty measured ticks, four peers can add at most
    %% 8 requests * 4 peers * 30 ticks = 960. Eleven link-capacity seconds are
    %% 1100 requests, leaving 140 requests (1.4 seconds) for the queue already
    %% present at the measurement boundary while rejecting unbounded growth.
    MaxOutstandingSeconds = 11,
    MaxHTTPInflight = MaxOutstandingSeconds * LinkCps,
    assert_at_most(TotalHTTPInflight, MaxHTTPInflight,
        #{ metric => total_http_inflight }).

%% Scenario: Many footprint peer/store fronts compete for fewer entropy slots
%% while all peers remain healthy.
%%
%% Context:
%% - Six equal peers expose thirty-six fronts to an entropy cache sized for
%%   steady work plus limited transition headroom.
%% - Entropy misses are expensive enough to expose assignment churn.
%%
%% Timeline:
%% - Warm discovery, concurrency, and footprint bindings, then measure across
%%   many footprint and dispatch boundaries.
%%
%% Contract:
%% - C1: Queued work on another front does not evict unfinished productive
%%   footprint work often enough to reduce aggregate throughput.
%%
%% Verification:
%% - V1: Stored throughput reaches 95% of combined peer capacity.
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

%% Scenario: A dominant footprint peer slows after occupying entropy slots just
%% as five healthy peers become available.
%%
%% Context:
%% - One initially fast footprint peer warms alone while five equal footprint
%%   peers advertise data but cannot accept requests.
%% - Entropy capacity is finite but task admission has ample headroom.
%%
%% Timeline:
%% - Let the first peer occupy the working set, enable the five peers while
%%   slowing the first, then measure after bindings and allocation recover.
%%
%% Contract:
%% - C1: Entropy assignments earned by the slowed peer do not prevent healthy
%%   peers from using their combined serving capacity.
%%
%% Verification:
%% - V1: The initially dominant peer reaches 95% of its original capacity.
%% - V2: The five healthy peers together reach 95% of combined capacity.
slow_footprint_peer_preserves_healthy_capacity_test_() ->
    ar_sync_sim_runner:setup_sim(
        fun test_slow_footprint_peer_preserves_healthy_capacity/0, 500).

test_slow_footprint_peer_preserves_healthy_capacity() ->
    use_footprint_size(16),
    SlowPeer = {10, 6, 6, 1, 1984},
    HealthyPeerIDs = [{10, 6, 6, N, 1984} || N <- lists:seq(2, 6)],
    InitialSlowPeerCPS = 30,
    HealthyPeerCPS = 6,
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
            %% Compact footprints keep the same six-peer binding competition
            %% while making entropy generation proportional to this contract.
            [sync, cache_size] => 564,
            [packing, entropy, cache_size] => 32
        }
    },
    ar_sync_sim_runner:start_sim(World),
    %% Thirty-two seconds cover the scheduler horizon. The next ten verify that
    %% the 30 chunks/s
    %% peer is actively occupying footprint slots before healthy peers open.
    ar_sync_sim_runner:run_for(32),
    InitialMeasurement = ar_sync_sim_runner:run_for(10),
    assert_metric_utilization(
        {cps_by_peer, SlowPeer}, InitialSlowPeerCPS, InitialMeasurement),
    ar_sync_sim_runner:update_world(World#sim_world{
        peers = Peers#{
            SlowPeer => InitialSlowPeer#sim_peer{
                max_serve_cps = 1,
                latency_ms = 10_000,
                limited = true
            }
        }
    }),
    %% One 32-tick scheduler evidence horizon lets healthy sources rotate the
    %% footprint bindings and settle before the steady measurement.
    ar_sync_sim_runner:run_for(32),
    Measurement = ar_sync_sim_runner:run_for(10),
    CpsByPeer = ar_sync_sim_runner:metric(cps_by_peer, Measurement),
    HealthyCPS = lists:sum([
        maps:get(Peer, CpsByPeer, 0) || Peer <- HealthyPeerIDs
    ]),
    %% Five healthy peers at 6 chunks/s provide 30 chunks/s.
    assert_at_least(HealthyCPS,
        ?MIN_STEADY_STATE_UTILIZATION
            * length(HealthyPeerIDs) * HealthyPeerCPS).

%% Scenario: Two footprint peers compete for one store whose claim headroom is
%% smaller than a complete footprint.
%%
%% Context:
%% - Both equal peers advertise only the same target store.
%% - Per-store headroom rounds up to one complete footprint reservation.
%%
%% Timeline:
%% - Warm discovery, footprint binding, and completion-driven admission, then
%%   measure both peers on the target store.
%%
%% Contract:
%% - C1: One whole-footprint reservation does not permanently consume the
%%   store's claim headroom and exclude the other peer.
%% - C2: Only the target store receives work.
%%
%% Verification:
%% - V1: Each peer reaches 95% of its capacity and aggregate writes reach 95%
%%   of their combined capacity.
%% - V2: All measured writes belong to the target store.
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
    %% The ten-second sweep warm gate plus twenty-five ticks for metadata,
    %% footprint binding, and peer control settle before measurement. Twenty
    %% measurement ticks contain 300 chunks per 15 chunks/s peer.
    ar_sync_sim_runner:run_for(35),
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

%% Scenario: Byte and footprint sources contribute different chunks from one
%% overlapping footprint.
%%
%% Context:
%% - A byte-only peer advertises the first chunk while a footprint-only peer
%%   advertises that chunk plus nine following chunks.
%%
%% Timeline:
%% - Start with both sparse advertisements and run through discovery and storage
%%   of the complete ten-chunk set.
%%
%% Contract:
%% - C1: Overlap does not make either representation suppress independently
%%   useful chunks from the other.
%%
%% Verification:
%% - V1: The byte peer serves at least one chunk, the footprint peer serves at
%%   least nine, and exactly ten chunks are stored in total.
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

%% Scenario: Slow active work from a bound footprint overlaps discovery of later
%% healthy byte work for the same store.
%%
%% Context:
%% - A footprint peer's complete detailed range is cached but remains
%%   unpromoted before another source becomes available for the same store.
%% - A healthy byte peer advertises abundant non-overlapping work farther ahead
%%   in the same store.
%%
%% Timeline:
%% - Cache the footprint range first, introduce the later byte range, then
%%   measure the healthy peer while the old range remains relevant.
%%
%% Contract:
%% - C1: Unmaterialized footprint intervals do not consume ordinary claim
%%   headroom and block later healthy work.
%%
%% Verification:
%% - V1: The slow peer exposes one complete cached footprint before later work
%%   is introduced.
%% - V2: The healthy byte peer and target store each reach 95% of the healthy
%%   peer's expected contribution.
unpromoted_footprint_work_does_not_block_store_test_() ->
    ar_sync_sim_runner:setup_sim(
        fun test_unpromoted_footprint_work_does_not_block_store/0, 500).

test_unpromoted_footprint_work_does_not_block_store() ->
    ar_sync_sim_world:use_mainnet_replica_2_9_sizes(),
    TargetStore = first_store(),
    StoreRange = first_store_range(),
    {StoreStart, StoreEnd} = StoreRange,
    SlowFootprintPeer = {10, 6, 5, 1, 1984},
    HealthyBytePeer = {10, 6, 5, 2, 1984},
    HealthyPeerCPS = 100,
    FootprintSize = ar_replica_2_9:get_footprint_size(),
    %% Advertise the complete first physical footprint so the coarse metadata
    %% has enough share to select it deterministically before the source update.
    FirstFootprintIntervals = footprint_chunk_intervals(
        StoreStart, 1, FootprintSize),
    SlowPeer = #sim_peer{
        max_serve_cps = HealthyPeerCPS,
        %% Thirty-nine seconds keeps the initial child fetches active during
        %% healthy-peer discovery while remaining below the 40-second timeout.
        latency_ms = 39_000,
        sync_kinds = [footprint],
        sync_availability = {intervals, FirstFootprintIntervals},
        footprint_coverage = exact
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
    FullWorld = #sim_world{
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
    %% Keep the healthy peer's metadata visible while preventing successful
    %% fetches. Forty seconds cover the sweep warm gate, detailed metadata, and
    %% the scheduler evidence horizon before that path becomes available.
    InitialWorld = FullWorld#sim_world{
        peers = #{
            SlowFootprintPeer => SlowPeer,
            HealthyBytePeer => HealthyPeer#sim_peer{
                http_inflight_limit = 0
            }
        }
    },
    ar_sync_sim_runner:start_sim(InitialWorld),
    ar_sync_sim_runner:run_for(40),
    #sim_snapshot{ chunk_interval_requests_by_peer = MetadataRequests } =
        ar_sync_sim_world:snapshot(),
    assert_greater_than(maps:get(SlowFootprintPeer, MetadataRequests), 0,
        #{ peer => SlowFootprintPeer,
            metric => chunk_interval_requests }),
    {[CachedPeerRange], ok} = ar_sync_discovery:cached_peer_ranges(
        TargetStore,
        [SlowFootprintPeer],
        StoreStart,
        StoreStart,
        StoreEnd),
    #peer_range{
        peer = SlowFootprintPeer,
        footprint = #footprint{},
        intervals = CachedIntervals
    } = CachedPeerRange,
    ?assertEqual(
        FootprintSize * ?DATA_CHUNK_SIZE,
        ar_intervals:sum(CachedIntervals)),
    ar_sync_sim_runner:update_world(FullWorld),
    %% Forty ticks let the newly available path's cap recover from its initial
    %% client errors before steady throughput is measured.
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

%% Scenario: One peer serves small useful groups spread across many footprints.
%%
%% Context:
%% - Each advertised footprint contains only a small exact subset of chunks.
%% - Metadata and entropy capacity are sufficient for multiple concurrent
%%   footprints across all stores.
%%
%% Timeline:
%% - Warm detailed metadata and concurrency growth, then measure sustained
%%   sparse-footprint service.
%%
%% Contract:
%% - C1: Sparse footprints progress concurrently enough to use the peer's
%%   serving capacity.
%%
%% Verification:
%% - V1: Stored throughput reaches 95% of peer capacity.
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
    %% ample useful work beyond cold-start readahead.
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
            %% detailed-metadata capacity above the peer's 120 chunks/s serving
            %% rate.
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
            %% requests needed for 120 chunks/s without constraining it.
            [packing, entropy, cache_size] => 16384
        }
    }),
    %% Fifty seconds cover detailed metadata and peer-cap growth. Twenty
    %% measurement seconds retain the stable sparse-footprint rate.
    ar_sync_sim_runner:run_for(50),
    Measurement = ar_sync_sim_runner:run_for(20),
    assert_metric_utilization(stored_cps, PeerCPS, Measurement).

%% Scenario: A detailed footprint-metadata request remains pending while later
%% byte work is available for the same store.
%%
%% Context:
%% - One footprint peer advertises a frontier chunk but its detailed metadata
%%   response cannot complete during the scenario.
%% - A healthy byte peer holds abundant work several sweep ranges later.
%%
%% Timeline:
%% - Leave the frontier request pending while readahead reaches the healthy
%%   range, then measure the healthy peer and target store.
%%
%% Contract:
%% - C1: Pending detailed metadata does not stall discovery or service of later
%%   usable work for the store.
%%
%% Verification:
%% - V1: The healthy peer and target store each reach 95% of the healthy peer's
%%   expected contribution before a stale metadata response could complete.
%% - V2: The stale detailed-metadata request was issued and remains pending.
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
    WarmupMeasurement = ar_sync_sim_runner:run_for(40),
    assert_chunk_interval_request_pending(StalePeer, WarmupMeasurement),
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

%% Scenario: Useful footprints are separated by repeated empty detailed-metadata
%% responses inside coarse peer advertisements.
%%
%% Context:
%% - One footprint peer advertises only the last footprint in each coarse bucket
%%   for every store, leaving a fixed run of empty footprints before each hit.
%% - The metadata endpoint is responsive but every request remains asynchronous.
%%
%% Timeline:
%% - Traverse several useful waves during warmup, then measure long enough to
%%   require continued discovery of additional waves.
%%
%% Contract:
%% - C1: Repeated empty gaps do not make detailed traversal slower than the
%%   configured serving rate.
%% - C2: Every store discovers useful work during the finite measurement.
%%
%% Verification:
%% - V1: Stored throughput reaches 95% of peer capacity.
%% - V2: Every store stores at least one chunk; equal shares are not required.
%% - V3: The fixture places one complete physical footprint at each bucket tail,
%%   separated by exactly eight empty footprints.
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
    %% Twelve useful footprints per store provide 12 * 6 * 4 = 288 chunks,
    %% more than the 100 seconds * 2 chunks/s = 200 this scenario can use.
    UsefulFootprintsPerStore = 12,
    AvailableIntervals = [
        Interval
        || {_StoreID, {StoreStart, _StoreEnd}}
                <- ar_sync_sim_world:store_ranges(),
            FirstFootprintOffset <- useful_bucket_tail_footprint_offsets(
                StoreStart,
                UsefulFootprintsPerStore,
                FootprintSize,
                NetworkBucketSize,
                EmptyFootprintsPerGap),
            Interval <- footprint_offset_intervals(
                FirstFootprintOffset, FootprintSize)
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

%% Scenario: One healthy footprint peer serves while four peers' detailed
%% metadata requests remain pending.
%%
%% Context:
%% - All five peers advertise footprint data, but four metadata endpoints cannot
%%   respond within the scenario.
%%
%% Timeline:
%% - Warm the healthy peer while slow requests remain pending, then measure its
%%   service before any slow response can complete.
%%
%% Contract:
%% - C1: Ready cached availability is usable without waiting for every candidate
%%   peer's detailed metadata.
%%
%% Verification:
%% - V1: The healthy peer reaches 95% of its serving capacity before any slow
%%   metadata response could complete.
%% - V2: Every slow peer's detailed-metadata request was issued and remains
%%   pending.
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
    WarmupMeasurement = ar_sync_sim_runner:run_for(30),
    lists:foreach(
        fun(SlowPeer) ->
            assert_chunk_interval_request_pending(
                SlowPeer, WarmupMeasurement)
        end,
        SlowPeers),
    %% Twenty seconds contain 2000 chunks at the healthy peer's 100 chunks/s
    %% serving capacity while every slow peer remains metadata-pending.
    Measurement = ar_sync_sim_runner:run_for(20),
    assert_metric_utilization(
        {cps_by_peer, HealthyPeer}, HealthyPeerCPS, Measurement).

%% Scenario: Byte work shares one store with footprint work that occupies the
%% only entropy slot.
%%
%% Context:
%% - A slow footprint peer and a faster byte peer advertise the same target
%%   store through independent representations.
%% - The entropy cache holds exactly one active footprint while the task cache
%%   has room for both source classes.
%%
%% Timeline:
%% - Keep the footprint slot occupied through warmup, then measure both peers.
%%
%% Contract:
%% - C1: Exhausted entropy capacity does not suppress independently fetchable
%%   byte work.
%% - C2: Footprint-mode syncing also remains active.
%%
%% Verification:
%% - V1: The byte peer reaches 95% of its serving capacity.
%% - V2: The footprint peer reaches 95% of its serving capacity.
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

%% Scenario: One footprint-serving peer remains productive among many known
%% peers that advertise no sync data.
%%
%% Context:
%% - A single peer can serve useful footprint work across every store.
%% - Two hundred gossip peers are known but expose no usable sync availability.
%%
%% Timeline:
%% - Warm discovery and peer control with the full known-peer set, then measure
%%   the serving peer across all stores.
%%
%% Contract:
%% - C1: Non-serving peers do not dilute detailed-metadata job capacity needed by
%%   the serving peer.
%% - C2: Useful work reaches every store.
%%
%% Verification:
%% - V1: Stored throughput reaches 95% of the serving peer's capacity.
%% - V2: Every store makes material progress above a 25% equal-share floor.
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
    %% The ten-second sweep warm gate plus twenty ticks for metadata and peer
    %% control settle despite 200 known non-serving peers. Twenty measured
    %% ticks then contain 2000 chunks at the peer's 100 chunks/s capacity.
    ar_sync_sim_runner:run_for(30),
    Measurement = ar_sync_sim_runner:run_for(20),
    %% Only the serving peer contributes capacity; the 200 gossip-only peers
    %% must not reduce its 100 chunks/s serving capacity.
    assert_metric_utilization(stored_cps, 100, Measurement),
    assert_chunks_spread_across_stores(Measurement, 0.25),
    ok.

%% Scenario: One peer supplies scarce chunk-interval metadata concurrently to
%% all six stores.
%%
%% Context:
%% - Each successful metadata request exposes a finite chunk range after one
%%   second.
%% - The endpoint can complete one request per store concurrently; additional
%%   inflight requests remain unavailable for the scenario.
%%
%% Timeline:
%% - Warm detailed metadata distribution across stores, then measure continued
%%   metadata-fed writes.
%%
%% Contract:
%% - C1: Metadata work is spread across stores without overloading the endpoint
%%   with duplicate work.
%% - C2: Detailed metadata supplies useful data near its aggregate capacity.
%%
%% Verification:
%% - V1: Every store stores at least one chunk during measurement.
%% - V2: Stored throughput reaches 90% of the six concurrent metadata paths.
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
    %% Ninety percent permits scheduler boundaries but rejects a starved mode.
    assert_at_least(ar_sync_sim_runner:metric(stored_cps, Measurement),
        0.90 * MetadataCPS, #{ metric => stored_cps }),
    assert_chunks_spread_across_stores(Measurement, 0),
    ok.

%% Scenario: Metadata readahead across many peers and both representations
%% remains bounded around current store frontiers.
%%
%% Context:
%% - Twenty-four peers advertise the same sparse ranges in byte and footprint
%%   form.
%% - Readahead can offer more detailed-metadata jobs than the worker bound, while
%%   peer and cache capacity exceed the stores' finite write rates.
%%
%% Timeline:
%% - Run through two complete byte/footprint readahead depths under sustained
%%   metadata pressure, then measure every store.
%%
%% Contract:
%% - C1: Obsolete metadata work does not starve current store frontiers.
%% - C2: Detailed metadata continuously supplies every eligible store at its
%%   configured write rate.
%% - C3: The scenario exercises metadata demand beyond the bounded pending-job
%%   capacity rather than merely configuring enough theoretical fan-out.
%%
%% Verification:
%% - V1: More than 1024 detailed-metadata requests are observed before the
%%   throughput window.
%% - V2: Stored throughput reaches 95% of aggregate store write capacity.
%% - V3: Every store remains within the default 90% equal-share floor.
discovery_backlog_preserves_store_throughput_test_() ->
    ar_sync_sim_runner:setup_sim(
        fun test_discovery_backlog_preserves_store_throughput/0, 600).

test_discovery_backlog_preserves_store_throughput() ->
    %% Twenty-four peers * six stores * forty byte/footprint readahead entries
    %% can offer 5760 detailed-metadata jobs, far beyond the 1024-job bound.
    PeerCount = 24,
    PeerCPS = 100,
    %% One 64-chunk metadata response every second exposes 64 chunks/s.
    %% Requiring half that rate per store retains the removed overlapping-peer
    %% case's 192 chunks/s aggregate contract while adding both representations.
    StoreCPS = 32,
    ChunksPerRange = 64,
    %% One hundred and twenty-eight sparse ranges per store contain
    %% 128 * 64 = 8192 chunks, well above the 80 seconds * 32 chunks/s
    %% that any one store can write during this scenario.
    RangeCount = 128,
    SharedIntervals = [
        {StoreStart + RangeIndex * ?QUERY_RANGE_STEP_SIZE
                + ChunksPerRange * ?DATA_CHUNK_SIZE,
            StoreStart + RangeIndex * ?QUERY_RANGE_STEP_SIZE}
        || {_StoreID, {StoreStart, _StoreEnd}}
                <- ar_sync_sim_world:store_ranges(),
            RangeIndex <- lists:seq(0, RangeCount - 1)
    ],
    Peers = maps:from_list([
        {{10, 5, 1, ID, 1984}, #sim_peer{
            %% Peer capacity is deliberately far above the six stores * 32
            %% chunks/s = 192 chunks/s write ceiling.
            max_serve_cps = PeerCPS,
            sync_kinds = [byte, footprint],
            sync_availability = {intervals, SharedIntervals},
            footprint_coverage = exact,
            %% One simulated second keeps metadata asynchronous and makes
            %% readahead offer work faster than the 100 workers can finish it.
            chunk_interval_latency_ms = 1_000
        }}
        || ID <- lists:seq(1, PeerCount)
    ]),
    ar_sync_sim_runner:start_sim(#sim_world{
        peers = Peers,
        store_write_cps = StoreCPS,
        node_config = #{
            %% 32768 chunks hold over 170 seconds at the aggregate 192
            %% chunks/s write ceiling, so cache pressure cannot set throughput.
            [sync, cache_size] => 32768,
            %% Sixty-four mainnet-sized entropy entries keep footprint work
            %% from making entropy availability the limiting resource.
            [packing, entropy, cache_size] => 16384
        }
    }),
    %% Forty seconds let the mixed byte/footprint sweep advance beyond the
    %% 1024-job pending bound while retaining pressure for measurement.
    WarmupMeasurement = ar_sync_sim_runner:run_for(40),
    MetadataRequests = lists:sum(maps:values(ar_sync_sim_runner:metric(
        chunk_interval_requests_by_peer, WarmupMeasurement))),
    assert_greater_than(MetadataRequests, 1024,
        #{ metric => chunk_interval_requests }),
    %% Forty seconds contain 1280 writes per store at 32 chunks/s. The 10%
    %% spread allowance is 128 chunks, exactly two 64-chunk sparse ranges,
    %% while remaining large enough for bounded range-boundary skew.
    Measurement = ar_sync_sim_runner:run_for(40),
    %% Six stores * 32 chunks/s = 192 chunks/s. The 95% utilization floor
    %% requires 182.4 chunks/s; the same 90% floor per store rejects starvation.
    ExpectedCPS = ?SIM_STORES * StoreCPS,
    assert_chunks_spread_across_stores(Measurement),
    assert_metric_utilization(stored_cps, ExpectedCPS, Measurement).

%% Scenario: Each store's needed data is held by a different peer.
%%
%% Context:
%% - Six equal peers each advertise exactly one distinct store.
%% - Every peer requires substantially more in-flight requests than its initial
%%   concurrency cap to use its serving capacity.
%%
%% Timeline:
%% - Warm discovery and concurrency growth for all six independent paths, then
%%   measure them together.
%%
%% Contract:
%% - C1: Metadata and scheduling for one peer/store path do not delay the others.
%% - C2: The independent paths combine near their aggregate serving capacity.
%%
%% Verification:
%% - V1: Every store makes material progress above a 50% equal-share floor.
%% - V2: Stored throughput reaches 95% of combined peer capacity.
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

%% Scenario: A stalled peer advertises each store's frontier while a healthy peer
%% holds abundant work in later ranges.
%%
%% Context:
%% - The stalled peer advertises one frontier chunk per store but has zero
%%   serving capacity.
%% - A healthy peer advertises work after the following empty sweep range.
%%
%% Timeline:
%% - Leave the frontier requests unresolved while sweeping into the later healthy
%%   ranges, then measure the healthy peer.
%%
%% Contract:
%% - C1: Unresolved frontier work does not hide later healthy capacity.
%% - C2: Every store advances beyond its stalled frontier.
%%
%% Verification:
%% - V1: Stored throughput reaches 95% of the healthy peer's capacity.
%% - V2: The cumulative snapshot records positive progress for every store; the
%%   stalled peer cannot contribute any of those writes.
%% - V3: All six stalled frontier fetches are active before later work wins.
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
    %% The zero-capacity requests cannot complete before their forty-second
    %% timeout. At twenty seconds, all six frontier fetches must be active.
    BlockerMeasurement = ar_sync_sim_runner:run_for(20),
    StalledInflight = maps:get(
        StalledPeer,
        ar_sync_sim_runner:metric(
            final_http_inflight_by_peer, BlockerMeasurement)),
    assert_at_least(StalledInflight, ?SIM_STORES,
        #{ peer => StalledPeer, metric => http_inflight }),
    %% Thirty more seconds cover the scheduler horizon after discovery reaches
    %% the healthy ranges; twenty measured seconds smooth serving limits.
    ar_sync_sim_runner:run_for(30),
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

%% @doc Combine adjacent measurements while retaining their outer snapshots.
combine_measurements([FirstMeasurement | _] = Measurements) ->
    LastMeasurement = lists:last(Measurements),
    #sim_measurement{
        duration_seconds = lists:sum([
            Seconds
            || #sim_measurement{ duration_seconds = Seconds } <- Measurements
        ]),
        before_snapshot = FirstMeasurement#sim_measurement.before_snapshot,
        after_snapshot = LastMeasurement#sim_measurement.after_snapshot
    }.

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
    footprint_offset_intervals(FirstFootprintOffset, ChunkCount).

%% @doc Return byte intervals for consecutive footprint-record offsets.
footprint_offset_intervals(FirstFootprintOffset, ChunkCount) ->
    FootprintOffsets = lists:seq(FirstFootprintOffset,
        FirstFootprintOffset + ChunkCount - 1),
    lists:map(
        fun(FootprintOffset) ->
            ChunkEnd = ar_footprint_record:
                get_padded_offset_from_footprint_offset(FootprintOffset),
            {ChunkEnd, ChunkEnd - ?DATA_CHUNK_SIZE}
        end,
        FootprintOffsets).

%% @doc Return and validate physical footprints at successive bucket tails.
useful_bucket_tail_footprint_offsets(StoreStart, Count, FootprintSize,
        NetworkBucketSize, EmptyFootprintsPerGap) ->
    {FirstChunkEnd, _FirstChunkStart} = single_chunk_interval(StoreStart, 1),
    StoreFirstOffset = ar_footprint_record:get_offset(FirstChunkEnd),
    %% Simulator store boundaries are coarse-bucket boundaries in footprint
    %% space. Keeping this explicit prevents byte-space fixture drift.
    ?assertEqual(0, (StoreFirstOffset - 1) rem NetworkBucketSize),
    FirstBucketStart = StoreFirstOffset - 1,
    FirstUsefulOffset = FirstBucketStart
        + NetworkBucketSize - FootprintSize + 1,
    UsefulOffsets = [
        FirstUsefulOffset + N * NetworkBucketSize
        || N <- lists:seq(0, Count - 1)
    ],
    lists:foreach(
        fun(Offset) ->
            ?assertEqual(
                NetworkBucketSize - FootprintSize,
                (Offset - 1) rem NetworkBucketSize)
        end,
        UsefulOffsets),
    Gaps = [
        NextOffset - Offset - FootprintSize
        || {Offset, NextOffset} <- lists:zip(
            lists:droplast(UsefulOffsets), tl(UsefulOffsets))
    ],
    ?assertEqual(
        lists:duplicate(Count - 1,
            EmptyFootprintsPerGap * FootprintSize),
        Gaps),
    UsefulOffsets.

%% @doc Return one byte interval covering consecutive sweep steps within a
%% store range.
sweep_chunk_interval({StoreStart, StoreEnd}, StartStep, StepCount) ->
    Step = ar_sync_cursor:query_range_step_size(),
    Start = StoreStart + StartStep * Step,
    End = min(StoreEnd, Start + StepCount * Step),
    {End, Start}.

%% @doc Use a compact footprint large enough to amortize metadata handoffs.
use_footprint_size(ChunkCount) ->
    EntropySize = ChunkCount * ?DATA_CHUNK_SIZE div ?SUB_CHUNK_COUNT,
    ar_replica_2_9:override_entropy_size(EntropySize).

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

%% @doc Assert that a measured metric reaches nearly all its expected value.
assert_metric_utilization(Metric, ExpectedValue, Measurement) ->
    ActualValue = ar_sync_sim_runner:metric(Metric, Measurement),
    MinimumValue = ?MIN_STEADY_STATE_UTILIZATION * ExpectedValue,
    assert_at_least_with_substep_allowance(
        ActualValue, MinimumValue, Measurement, #{ metric => Metric }).

%% @doc Assert a peer rate with one simulated clock quantum of boundary slack.
assert_rate_at_least(RateMetric, Peer, ExpectedRate, Measurement) ->
    RatesByPeer = ar_sync_sim_runner:metric(RateMetric, Measurement),
    ActualRate = maps:get(Peer, RatesByPeer),
    assert_at_least_with_substep_allowance(
        ActualRate, ExpectedRate, Measurement,
        #{ peer => Peer, metric => RateMetric }).

%% @doc Assert a measured value with one clock quantum of boundary slack.
assert_at_least_with_substep_allowance(
        Actual, Expected, Measurement, Context) ->
    DurationSeconds = ar_sync_sim_runner:metric(
        duration_seconds, Measurement),
    BoundaryAllowance = Expected * ?SIM_SUBSTEP_MS
        / 1000 / DurationSeconds,
    assert_at_least(
        Actual,
        Expected - BoundaryAllowance,
        maps:merge(Context, #{
            expected_before_boundary_allowance => Expected,
            boundary_allowance => BoundaryAllowance
        })).

%% @doc Assert that detailed metadata was requested and remains unresolved.
assert_chunk_interval_request_pending(Peer, Measurement) ->
    Requests = ar_sync_sim_runner:metric(
        final_chunk_interval_requests_by_peer, Measurement),
    assert_greater_than(maps:get(Peer, Requests), 0,
        #{ peer => Peer, metric => chunk_interval_requests }),
    Inflight = ar_sync_sim_runner:metric(
        final_chunk_interval_inflight_by_peer, Measurement),
    assert_greater_than(maps:get(Peer, Inflight), 0,
        #{ peer => Peer, metric => chunk_interval_inflight }).

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

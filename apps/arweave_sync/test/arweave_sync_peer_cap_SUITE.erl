-module(arweave_sync_peer_cap_SUITE).
-test_category([fast]).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("arweave/include/ar.hrl").
-include("arweave_sync_peer_cap.hrl").

suite() -> [{timetrap, {seconds, 60}}].

all() ->
    [
        failure_pressure_cuts_cap,
        small_failure_pressure_preserves_concurrency,
        baseline_window_precedes_probe,
        increase_judged_on_gain,
        cut_judged_on_loss,
        partial_gain_keeps_measured_baseline,
        small_gain_keeps_measured_capacity,
        partial_gain_preserves_probe_growth,
        brief_slowdowns_do_not_end_probe,
        forty_second_slowdown_does_not_hide_gain,
        sustained_slowdown_still_ends_probe,
        isolated_burst_does_not_extend_probe,
        batched_completions_still_grow_concurrency,
        recurring_slowdowns_do_not_stop_growth,
        settled_probes_preserve_throughput,
        cut_floors_at_seed,
        probes_rest_after_a_losing_cut
    ].

%%====================================================================
%% Test cases
%%====================================================================

%% @doc Failure pressure cuts the cap by its worker-time share and restarts
%% measurement at the cut cap.
failure_pressure_cuts_cap(_Config) ->
    Control0 = #cap_control{cap = 100},
    %% Ten percent worker-time pressure asks for 15% of the 105-request cap:
    %% remove fifteen whole requests, not the fractional sixteenth.
    PressureCut = arweave_sync_peer_cap:update(
        sample(110.0), 0.1, true, #cap_control{cap = 105}
    ),
    ?assertEqual(
        #cap_control{cap = 90, step = 12, skip = ?GOODPUT_TRANSITION_SAMPLES},
        PressureCut
    ),
    %% One hundred percent failure pressure reaches the 50% cut clamp.
    FullCut = arweave_sync_peer_cap:update(undefined, 1.0, true, Control0),
    ?assertEqual(50, FullCut#cap_control.cap),
    MinimumCut = arweave_sync_peer_cap:update(
        undefined, 1.0, true, #cap_control{cap = ?CONCURRENCY_CAP_MIN}
    ),
    ?assertEqual(?CONCURRENCY_CAP_MIN, MinimumCut#cap_control.cap),
    %% A cut abandons a running probe, cutting from the probed cap.
    Probe = add_samples(
        100.0, ?PROBE_WINDOW_SAMPLES, #cap_control{cap = 100, step = 13}
    ),
    ?assertEqual(113, Probe#cap_control.cap),
    ProbeCut = arweave_sync_peer_cap:update(sample(100.0), 0.1, true, Probe),
    ?assertMatch(
        #cap_control{cap = 97, phase = hold, skip = ?GOODPUT_TRANSITION_SAMPLES},
        ProbeCut
    ),
    %% Nine two-second successes and one 250 ms 429 yield 1.37% worker-time
    %% pressure. Scaling by 1.5 cuts a cap of 100 by 2.05%, rounding to 98.
    FastRejectControl = arweave_sync_peer_cap:update(
        sample(100.0), 250 / 18250, true, Control0
    ),
    ?assertEqual(98, FastRejectControl#cap_control.cap).

%% @doc Small failure pressure cannot round a fractional cut up to a request.
small_failure_pressure_preserves_concurrency(_Config) ->
    %% One four-second failure among twenty equal-duration completions asks
    %% for a 7.5% cut: 0.6 requests at the eight-request exploration cap.
    Control = #cap_control{cap = 8},
    SmallCut = arweave_sync_peer_cap:update(sample(100.0), 4000 / 80000, true, Control),
    ?assertEqual(8, SmallCut#cap_control.cap),
    ?assertEqual(hold, SmallCut#cap_control.phase),
    %% Ten percent pressure asks for 1.2 requests, so one is removed.
    LargerCut = arweave_sync_peer_cap:update(sample(100.0), 0.1, true, Control),
    ?assertEqual(7, LargerCut#cap_control.cap),
    %% An all-failure interval must still halve this small cap.
    FullCut = arweave_sync_peer_cap:update(undefined, 1.0, true, Control),
    ?assertEqual(4, FullCut#cap_control.cap).

%% @doc A held cap measures a full window of driven samples before
%% probing, and the sample straddling a cap change is skipped.
baseline_window_precedes_probe(_Config) ->
    %% A fresh peer doubles from the seed.
    Fresh = add_samples(100.0, ?PROBE_WINDOW_SAMPLES, #cap_control{}),
    ?assertMatch(
        #cap_control{phase = up, cap = 2 * ?CONCURRENCY_CAP_INITIAL}, Fresh
    ),
    Partial = add_samples(
        100.0, ?PROBE_WINDOW_SAMPLES - 1, #cap_control{cap = 100, step = 13}
    ),
    ?assertMatch(#cap_control{cap = 100, phase = hold}, Partial),
    %% Undriven or unsampled samples do not count.
    ?assertEqual(
        Partial,
        arweave_sync_peer_cap:update(sample(100.0), 0.0, false, Partial)
    ),
    ?assertEqual(
        Partial,
        arweave_sync_peer_cap:update(undefined, 0.0, true, Partial)
    ),
    %% The last sample of the window starts a probe with an increase of
    %% an eighth.
    Probe = add_samples(100.0, 1, Partial),
    ?assertMatch(
        #cap_control{
            phase = up,
            cap = 113,
            baseline_cap = 100,
            baseline_goodput = 100.0,
            skip = ?GOODPUT_TRANSITION_SAMPLES
        },
        Probe
    ),
    Skipped = add_samples(500.0, 1, Probe),
    ?assertMatch(#cap_control{skip = 0, window_samples = []}, Skipped).

%% @doc Productive steps retain the measured cap and size the next increase.
increase_judged_on_gain(_Config) ->
    Probe = up_step(100, 100.0),
    ?assertMatch(#cap_control{cap = 113, baseline_cap = 100}, Probe),
    %% A 14% gain covers the whole 13% step, so the cap stays at 113 and the
    %% next step doubles: 113 + 26 = 139.
    Full = add_samples(114.0, ?PROBE_WINDOW_SAMPLES, Probe),
    ?assertMatch(
        #cap_control{
            phase = up,
            step = 26,
            baseline_cap = 113,
            baseline_goodput = 114.0,
            cap = 139
        },
        Full
    ),
    %% Nothing is decided before the window completes...
    Pending = add_samples(108.0, ?PROBE_WINDOW_SAMPLES - 1, Probe),
    ?assertMatch(#cap_control{phase = up, cap = 113, baseline_cap = 100}, Pending),
    %% An 8% gain exceeds half of the 13% cap increase, so double the next
    %% step while retaining the actual 113-request measurement baseline.
    Partial = add_samples(108.0, 1, Pending),
    ?assertMatch(#cap_control{
        phase = up, baseline_cap = 113, baseline_goodput = 108.0, step = 26
    }, Partial),
    %% A 3% gain retains 113 too, but the next step is ceil(113 / 8) = 15.
    Small = add_samples(103.0, ?PROBE_WINDOW_SAMPLES, Probe),
    ?assertMatch(
        #cap_control{
            phase = up, cap = 128, baseline_cap = 113,
            baseline_goodput = 103.0, step = 15
        },
        Small
    ),
    %% One flat window returns to 100 and steps down by ceil(100 / 8) = 13.
    Flat = add_samples(100.0, ?PROBE_WINDOW_SAMPLES, Probe),
    ?assertMatch(
        #cap_control{
            phase = down, cap = 87, baseline_cap = 100, baseline_goodput = 100.0
        },
        Flat
    ).

%% @doc After a step down, the lower cap stays only if goodput held, and the
%% probe steps down again with a doubled step; a step down that costs any
%% goodput returns the cap to where it started and ends the probe.
cut_judged_on_loss(_Config) ->
    Probe = down_step(200, 100.0),
    ?assertMatch(#cap_control{phase = down, cap = 175, baseline_cap = 200}, Probe),
    %% The next cut doubles: 175 - 50 = 125.
    Held = add_samples(100.0, ?PROBE_WINDOW_SAMPLES, Probe),
    ?assertMatch(
        #cap_control{phase = down, step = 50, baseline_cap = 175, cap = 125}, Held
    ),
    %% A step down that raises goodput stays the same way.
    Better = add_samples(110.0, ?PROBE_WINDOW_SAMPLES, Probe),
    ?assertMatch(#cap_control{phase = down, baseline_cap = 175, cap = 125}, Better),
    %% A 13% loss returns the cap to 200 and rests.
    Restored = add_samples(87.0, ?PROBE_WINDOW_SAMPLES, Probe),
    ?assertMatch(
        #cap_control{phase = hold, cap = 200, skip = ?PROBE_REST_SAMPLES},
        Restored
    ),
    %% So does a small loss.
    Small = add_samples(98.0, ?PROBE_WINDOW_SAMPLES, Probe),
    ?assertMatch(#cap_control{phase = hold, cap = 200}, Small).

partial_gain_keeps_measured_baseline(_Config) ->
    %% Rounded live rates: 19.3 at cap 8 and 34.7 at cap 16. The old
    %% interpolation associated the latter measurement with an untested 14.
    Probe = up_step(8, 19.3, 8),
    Next = add_samples(34.7, ?PROBE_WINDOW_SAMPLES, Probe),
    ?assertMatch(#cap_control{
        phase = up, cap = 32, baseline_cap = 16, baseline_goodput = 34.7
    }, Next),
    %% A further gain to 44.4 is less than half of doubling 16 to 32.
    %% Retain 32 and the sixteen-request step, rather than shrinking either.
    Continued = add_samples(44.4, ?PROBE_WINDOW_SAMPLES,
        skip_transition(Next)),
    ?assertMatch(#cap_control{
        phase = up, cap = 48, baseline_cap = 32, baseline_goodput = 44.4
    }, Continued).

small_gain_keeps_measured_capacity(_Config) ->
    %% A 2.6% gain at 21 used to round 17 * 35.8 / 34.9 back to 17,
    %% misclassifying an improvement as no gain and cutting to 14.
    Probe = up_step(17, 34.9, 4),
    Next = add_samples(35.8, ?PROBE_WINDOW_SAMPLES, Probe),
    ?assertMatch(#cap_control{
        phase = up, cap = 25, baseline_cap = 21, baseline_goodput = 35.8
    }, Next).

partial_gain_preserves_probe_growth(_Config) ->
    %% The live doubling from eight to sixteen requests increased a noisy
    %% window from 19.3 to 21.8 MiB/s. It must not shrink the next step to two
    %% requests, whose small gain is easily hidden by another slowdown.
    Next = add_samples(21.8, ?PROBE_WINDOW_SAMPLES, up_step(8, 19.3, 8)),
    ?assertMatch(
        #cap_control{
            phase = up, baseline_cap = 16, cap = 24, step = 8
        },
        Next
    ).

brief_slowdowns_do_not_end_probe(_Config) ->
    %% Rounded live samples at cap 22: the mean is below the cap-18 baseline
    %% of 37.6, although four of six samples show a useful improvement.
    Probe = up_step(18, 37.6, 4),
    FirstMinute = lists:foldl(
        fun(Rate, Control) ->
            arweave_sync_peer_cap:update(sample(Rate), 0.0, true, Control)
        end,
        Probe,
        [41.0, 40.3, 39.5, 33.8, 39.8, 25.6]
    ),
    Next = add_samples(40.0, ?PROBE_WINDOW_SAMPLES - 6, FirstMinute),
    ?assertMatch(#cap_control{phase = up, baseline_cap = 22}, Next),
    ?assert(Next#cap_control.cap > 22).

forty_second_slowdown_does_not_hide_gain(_Config) ->
    %% Four production ticks are forty seconds: less than half a window.
    Probe = add_samples(10.0, 4, up_step(18, 37.6, 4)),
    Next = add_samples(40.0, ?PROBE_WINDOW_SAMPLES - 4, Probe),
    ?assertMatch(#cap_control{phase = up, baseline_cap = 22}, Next).

sustained_slowdown_still_ends_probe(_Config) ->
    %% Every sample loses 20% of the baseline: this is not a brief dip.
    Next = add_samples(80.0, ?PROBE_WINDOW_SAMPLES, up_step(100, 100.0)),
    ?assertMatch(#cap_control{phase = down, baseline_cap = 100}, Next).

isolated_burst_does_not_extend_probe(_Config) ->
    %% One tenfold burst among otherwise flat samples implies no new capacity.
    Probe = add_samples(
        100.0,
        ?PROBE_WINDOW_SAMPLES - 1,
        up_step(100, 100.0)
    ),
    Next = add_samples(1000.0, 1, Probe),
    ?assertMatch(#cap_control{phase = down, baseline_cap = 100}, Next).

batched_completions_still_grow_concurrency(_Config) ->
    %% Requests slower than a ten-second tick can complete in batches with
    %% two empty ticks between them. Those zero samples are not saturation.
    Baseline = lists:flatten(lists:duplicate(4, [0.0, 0.0, 8.0])),
    Probe = lists:foldl(
        fun(Rate, Control) ->
            arweave_sync_peer_cap:update(sample(Rate), 0.0, true, Control)
        end,
        arweave_sync_peer_cap:new(),
        Baseline
    ),
    ?assertMatch(#cap_control{phase = up, cap = 16}, Probe),
    Higher = lists:flatten(lists:duplicate(4, [0.0, 0.0, 16.0])),
    Next = lists:foldl(
        fun(Rate, Control) ->
            arweave_sync_peer_cap:update(sample(Rate), 0.0, true, Control)
        end,
        skip_transition(Probe),
        Higher
    ),
    ?assertMatch(#cap_control{phase = up, baseline_cap = 16}, Next),
    ?assert(Next#cap_control.cap > 16).

recurring_slowdowns_do_not_stop_growth(_Config) ->
    %% A 24 chunks/s peer needs 48 requests: 48 / (0.5 + 48/32) = 24.
    %% Thirty seconds of triple latency every four minutes leaves 11/12
    %% of its ordinary capacity. Exercise every ten-second phase offset in
    %% the four-minute cycle, not just one alignment with probe boundaries.
    lists:foreach(
        fun(Offset) ->
            Rates = recurring_rates(Offset),
            Mean = lists:sum(Rates) / length(Rates),
            ?assert(
                Mean >= 0.95 * 24 * 11 / 12,
                #{offset => Offset, mean_cps => Mean}
            )
        end,
        lists:seq(0, 23)
    ).

settled_probes_preserve_throughput(_Config) ->
    %% Include a ten-request serving need, where rounding a downward step
    %% costs 20%, and a 400-request need with slow parallel responses.
    lists:foreach(
        fun({Capacity, LatencySeconds}) ->
            RateFun = fun(_Tick, Cap) -> min(Capacity, Cap / LatencySeconds) end,
            %% After twenty production minutes of warmup, measure 100 minutes:
            %% multiple complete probe/rest cycles, including their costly cuts.
            Rates = measured_rates(RateFun, 600),
            ?assert(
                lists:sum(Rates) / length(Rates) >= 0.95 * Capacity,
                #{capacity => Capacity, latency_seconds => LatencySeconds}
            )
        end,
        [{40, 0.25}, {100, 0.25}, {100, 4}]
    ).

%% @doc A cut never steps below the exploration seed; a probe at the seed
%% ends after its increase.
cut_floors_at_seed(_Config) ->
    NearSeed = add_samples(100.0, ?PROBE_WINDOW_SAMPLES,
        up_step(9, 100.0)),
    ?assertMatch(
        #cap_control{phase = down, cap = ?CONCURRENCY_CAP_INITIAL}, NearSeed
    ),
    AtSeed = add_samples(
        100.0,
        ?PROBE_WINDOW_SAMPLES,
        up_step(?CONCURRENCY_CAP_INITIAL, 100.0)
    ),
    ?assertMatch(
        #cap_control{
            phase = hold,
            cap = ?CONCURRENCY_CAP_INITIAL,
            skip = ?GOODPUT_TRANSITION_SAMPLES
        },
        AtSeed
    ).

%% @doc A probe whose cut cost goodput rests before measuring the next probe's
%% baseline, which then starts that probe with an increase.
probes_rest_after_a_losing_cut(_Config) ->
    Held = add_samples(87.0, ?PROBE_WINDOW_SAMPLES, down_step(200, 100.0)),
    ?assertMatch(
        #cap_control{phase = hold, cap = 200, skip = ?PROBE_REST_SAMPLES}, Held
    ),
    Waiting = add_samples(100.0, ?PROBE_REST_SAMPLES, Held),
    ?assertMatch(
        #cap_control{phase = hold, cap = 200, skip = 0, window_samples = []},
        Waiting
    ),
    Next = add_samples(100.0, ?PROBE_WINDOW_SAMPLES, Waiting),
    ?assertMatch(#cap_control{phase = up, cap = 225, baseline_cap = 200}, Next).

%%====================================================================
%% Helpers
%%====================================================================

recurring_rates(Offset) ->
    RateFun = fun(Tick, Cap) ->
        NormalRate = min(24.0, Cap / (0.5 + Cap / 32)),
        case (Tick + Offset) rem 24 >= 21 of
            true -> NormalRate / 3;
            false -> NormalRate
        end
    end,
    %% Two complete four-minute slowdown cycles after warmup.
    measured_rates(RateFun, 48).

measured_rates(RateFun, MeasurementTicks) ->
    %% 120 ten-second production ticks allow twenty minutes of warmup.
    WarmTicks = 120,
    {_, Rates} = lists:foldl(
        fun(Tick, {Control, Acc}) ->
            Cap = arweave_sync_peer_cap:cap(Control),
            Rate = RateFun(Tick, Cap),
            Next = arweave_sync_peer_cap:update(sample(Rate), 0.0, true, Control),
            Rates =
                case Tick >= WarmTicks of
                    true -> [Rate | Acc];
                    false -> Acc
                end,
            {Next, Rates}
        end,
        {arweave_sync_peer_cap:new(), []},
        lists:seq(0, WarmTicks + MeasurementTicks - 1)
    ),
    Rates.

%% @doc A one-second sample at Rate bytes/ms.
sample(Rate) ->
    {Rate * 1000, 1000}.

%% @doc Apply Count productive, driven one-second samples at Rate.
add_samples(Rate, Count, Control) ->
    lists:foldl(
        fun(_, Acc) ->
            arweave_sync_peer_cap:update(sample(Rate), 0.0, true, Acc)
        end,
        Control,
        lists:seq(1, Count)
    ).

%% @doc Measure Cap at Rate and start an increase, skipping its transition.
up_step(Cap, Rate) ->
    up_step(Cap, Rate, ceil(Cap / ?CONCURRENCY_CAP_STEP_DIVISOR)).

up_step(Cap, Rate, Step) ->
    skip_transition(
        add_samples(Rate, ?PROBE_WINDOW_SAMPLES, #cap_control{cap = Cap, step = Step})
    ).

%% @doc Measure Cap at Rate, find no gain above it, and start a cut, skipping
%% its transition.
down_step(Cap, Rate) ->
    skip_transition(add_samples(Rate, ?PROBE_WINDOW_SAMPLES,
        up_step(Cap, Rate))).

skip_transition(#cap_control{skip = Skip} = Control) ->
    add_samples(0.0, Skip, Control).

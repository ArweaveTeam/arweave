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
        overshoot_scales_cap_to_gain,
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
        100.0, ?GOODPUT_WINDOW_SAMPLES, #cap_control{cap = 100, step = 13}
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
    Fresh = add_samples(100.0, ?GOODPUT_WINDOW_SAMPLES, #cap_control{}),
    ?assertMatch(
        #cap_control{phase = up, cap = 2 * ?CONCURRENCY_CAP_INITIAL}, Fresh
    ),
    Partial = add_samples(
        100.0, ?GOODPUT_WINDOW_SAMPLES - 1, #cap_control{cap = 100, step = 13}
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
    ?assertMatch(#cap_control{skip = 0, window_samples = 0}, Skipped).

%% @doc After a step up, the cap moves to the starting cap scaled by the goodput
%% gain. If goodput rose by at least half as much as the cap did, the probe
%% steps up again with a doubled step; if by less, the probe ends; if not at
%% all, the probe steps down.
increase_judged_on_gain(_Config) ->
    Probe = up_step(100, 100.0),
    ?assertMatch(#cap_control{cap = 113, baseline_cap = 100}, Probe),
    %% A 14% gain covers the whole 13% step, so the cap stays at 113 and the
    %% next step doubles: 113 + 26 = 139.
    Full = add_samples(114.0, ?GOODPUT_WINDOW_SAMPLES, Probe),
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
    Pending = add_samples(108.0, ?GOODPUT_WINDOW_SAMPLES - 1, Probe),
    ?assertMatch(#cap_control{phase = up, cap = 113, baseline_cap = 100}, Pending),
    %% ...then an 8% gain moves the cap to 108, more than half the step, and
    %% the probe steps up again from 108.
    Partial = add_samples(108.0, 1, Pending),
    ?assertMatch(#cap_control{phase = up, baseline_cap = 108, step = 26}, Partial),
    %% A 3% gain moves the cap to 103, less than half the step: the peer needs
    %% no less, so the probe ends without a step down, and the next probe, one
    %% baseline window later, checks the new cap.
    Flat = add_samples(103.0, ?GOODPUT_WINDOW_SAMPLES, Probe),
    ?assertMatch(
        #cap_control{
            phase = hold, cap = 103, step = 13, skip = ?GOODPUT_TRANSITION_SAMPLES
        },
        Flat
    ),
    %% Unchanged goodput leaves the cap at 100 and steps down by an eighth,
    %% against the same baseline.
    ?assertMatch(
        #cap_control{
            phase = down, cap = 87, baseline_cap = 100, baseline_goodput = 100.0
        },
        add_samples(100.0, ?GOODPUT_WINDOW_SAMPLES, Probe)
    ).

%% @doc After a step down, the lower cap stays only if goodput held, and the
%% probe steps down again with a doubled step; a step down that costs any
%% goodput returns the cap to where it started and ends the probe.
cut_judged_on_loss(_Config) ->
    Probe = down_step(200, 100.0),
    ?assertMatch(#cap_control{phase = down, cap = 175, baseline_cap = 200}, Probe),
    %% The next cut doubles: 175 - 50 = 125.
    Held = add_samples(100.0, ?GOODPUT_WINDOW_SAMPLES, Probe),
    ?assertMatch(
        #cap_control{phase = down, step = 50, baseline_cap = 175, cap = 125}, Held
    ),
    %% A step down that raises goodput stays the same way.
    Better = add_samples(110.0, ?GOODPUT_WINDOW_SAMPLES, Probe),
    ?assertMatch(#cap_control{phase = down, baseline_cap = 175, cap = 125}, Better),
    %% A 13% loss returns the cap to 200 and rests.
    Restored = add_samples(87.0, ?GOODPUT_WINDOW_SAMPLES, Probe),
    ?assertMatch(
        #cap_control{phase = hold, cap = 200, skip = ?PROBE_REST_SAMPLES},
        Restored
    ),
    %% So does a small loss.
    Small = add_samples(98.0, ?GOODPUT_WINDOW_SAMPLES, Probe),
    ?assertMatch(#cap_control{phase = hold, cap = 200}, Small).

%% @doc After a doubled step past the peer's saturation point, the cap moves to
%% the starting cap scaled by the goodput gain, and the probe ends.
overshoot_scales_cap_to_gain(_Config) ->
    Probe = up_step(18, 72.0, 18),
    ?assertMatch(#cap_control{cap = 36, baseline_cap = 18}, Probe),
    %% Doubling to 36 raised goodput 39% to the peer's 100 capacity, which
    %% 18 * 100 / 72 = 25 requests already reach: less than half the step.
    Settled = add_samples(100.0, ?GOODPUT_WINDOW_SAMPLES, Probe),
    ?assertMatch(
        #cap_control{phase = hold, cap = 25, skip = ?GOODPUT_TRANSITION_SAMPLES},
        Settled
    ).

%% @doc A cut never steps below the exploration seed; a probe at the seed
%% ends after its increase.
cut_floors_at_seed(_Config) ->
    NearSeed = add_samples(100.0, ?GOODPUT_WINDOW_SAMPLES, up_step(9, 100.0)),
    ?assertMatch(
        #cap_control{phase = down, cap = ?CONCURRENCY_CAP_INITIAL}, NearSeed
    ),
    AtSeed = add_samples(
        100.0,
        ?GOODPUT_WINDOW_SAMPLES,
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
    Held = add_samples(87.0, ?GOODPUT_WINDOW_SAMPLES, down_step(200, 100.0)),
    ?assertMatch(
        #cap_control{phase = hold, cap = 200, skip = ?PROBE_REST_SAMPLES}, Held
    ),
    Waiting = add_samples(100.0, ?PROBE_REST_SAMPLES, Held),
    ?assertMatch(
        #cap_control{phase = hold, cap = 200, skip = 0, window_samples = 0},
        Waiting
    ),
    Next = add_samples(100.0, ?GOODPUT_WINDOW_SAMPLES, Waiting),
    ?assertMatch(#cap_control{phase = up, cap = 225, baseline_cap = 200}, Next).

%%====================================================================
%% Helpers
%%====================================================================

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
        add_samples(Rate, ?GOODPUT_WINDOW_SAMPLES, #cap_control{cap = Cap, step = Step})
    ).

%% @doc Measure Cap at Rate, find no gain above it, and start a cut, skipping
%% its transition.
down_step(Cap, Rate) ->
    skip_transition(add_samples(Rate, ?GOODPUT_WINDOW_SAMPLES, up_step(Cap, Rate))).

skip_transition(#cap_control{skip = Skip} = Control) ->
    add_samples(0.0, Skip, Control).


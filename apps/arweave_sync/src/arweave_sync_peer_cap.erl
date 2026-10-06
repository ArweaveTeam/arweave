%%% @doc A peer's concurrency cap: how many fetches this node runs against the
%%% peer at once. arweave_sync_peer calls update/4 once per tick with the peer's
%%% goodput sample, its failure pressure and whether the peer was driven.
%%% arweave_sync_peer_cap.hrl defines the terms used here (tick, goodput
%%% sample, driven, window, step, probe).
%%%
%%% - This node sets each peer's cap based on that peer's goodput and failures.
%%%   A tick changes the cap in at most one way: a failure cut or a probe step.
%%% - Failure pressure is the share of the peer's fetch time spent on 429s,
%%%   503s, timeouts and client errors. It cuts the cap in proportion
%%%   (?FAILURE_CUT_GAIN, at most ?MAX_TICK_CUT per tick), whether or not the
%%%   peer is driven, and restarts the probe.
%%% - Probing runs only while a peer is driven; a peer that is not driven keeps
%%%   its cap. A probe measures a baseline window, then steps the cap up and
%%%   measures a window at the new cap. If the window's goodput rises, keep
%%%   that measured cap and probe higher. A window without a gain starts a
%%%   step down, which keeps the lower cap only if goodput holds.
%%%   The next step is twice as large when goodput rose by at least half as
%%%   much as the cap did, in proportion, or when a step down held goodput.
%%%   Smaller gains keep the step, at least one eighth of the current cap.
%%% - A probe ends when a step down costs goodput, or when the cap cannot step
%%%   below ?CONCURRENCY_CAP_INITIAL. After a step down that costs goodput,
%%%   the cap returns to where the step started and stays there for
%%%   ?PROBE_REST_SAMPLES goodput samples before the next probe.
%%% - A new peer starts at ?CONCURRENCY_CAP_INITIAL, and its first probe
%%%   doubles from there. Failure cuts can go lower, to ?CONCURRENCY_CAP_MIN.
%%% - Probing cannot see competition: when several clients share a saturated
%%%   peer, each one gains goodput by queueing deeper, at the others' expense.
%%%   Only the serving side can stop that, with 429s from its rate limits and
%%%   503s for chunk requests it cannot start within a few seconds.
-module(arweave_sync_peer_cap).

-export([new/0, update/4, cap/1, phase/1]).
-export_type([control/0]).

-include("arweave_sync_peer_cap.hrl").

-opaque control() :: #cap_control{}.

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Return the cap control for a peer this node has not measured yet.
new() ->
    #cap_control{}.

%% @doc Return how many fetches this node may run against the peer at once.
cap(#cap_control{ cap = Cap }) ->
    Cap.

%% @doc Return the probe's phase: hold, up or down.
phase(#cap_control{ phase = Phase }) ->
    Phase.

%% @doc Return the cap control after one tick: any failure pressure cuts the
%% cap; otherwise, if the peer is driven, the tick's goodput sample goes to the
%% probe.
%%
%% Sample: the goodput sample, {FetchedBytes, ElapsedMs} since the previous
%% tick, or undefined. When the peer is driven, the sample counts even if no
%% fetch completed, because the peer's requests are still in flight.
update(Sample, FailurePressure, Driven, Control) ->
    case FailurePressure > 0.0 of
        true ->
            Cut = min(?MAX_TICK_CUT, ?FAILURE_CUT_GAIN * FailurePressure),
            %% Round the cut down: at a small cap, rounding it up would remove
            %% a whole request for every occasional error.
            Cap = max(?CONCURRENCY_CAP_MIN,
                ceil(Control#cap_control.cap * (1.0 - Cut))),
            %% Start a new probe from the reduced cap, beginning with a
            %% baseline window.
            #cap_control{ cap = Cap, step = base_step(Cap),
                skip = ?GOODPUT_TRANSITION_SAMPLES };
        false when Driven, Sample =/= undefined ->
            add_probe_sample(Sample, Control);
        false ->
            Control
    end.

%%%===================================================================
%%% Probe.
%%%===================================================================

%% @doc Add a goodput sample to the probe's window, unless the probe is still
%% skipping samples after a cap change. A full window is used by phase:
%% - hold: the window is the baseline, so step the cap up;
%% - up or down: the window measured the stepped cap, so finish the step.
add_probe_sample(_Sample, #cap_control{skip = Skip} = Control) when
    Skip > 0
->
    Control#cap_control{skip = Skip - 1};
add_probe_sample(
    {Bytes, Ms},
    #cap_control{window_samples = Samples} = Control
) ->
    Samples2 = [Bytes / Ms | Samples],
    Control2 = Control#cap_control{window_samples = Samples2},
    case length(Samples2) < ?PROBE_WINDOW_SAMPLES of
        true ->
            Control2;
        false ->
            case Control2#cap_control.phase of
                hold -> start_phase(up, window_goodput(Control2), Control2);
                up -> finish_up(Control2);
                down -> finish_down(Control2)
            end
    end.

%% @doc Start an up or down phase: move the cap one step in that direction, so
%% the new cap can be compared with the old one. A cap at
%% ?CONCURRENCY_CAP_INITIAL has no lower cap to try, so a step down from there
%% ends the probe.
%%
%% BaselineGoodput: the goodput measured at the cap before the step.
start_phase(Phase, BaselineGoodput,
        #cap_control{ cap = Cap, step = Step } = Control) ->
    case step(Phase, Cap, Step) of
        Cap ->
            hold(Cap, ?GOODPUT_TRANSITION_SAMPLES, Control);
        StepCap ->
            reset_window(Control#cap_control{
                phase = Phase,
                cap = StepCap,
                skip = ?GOODPUT_TRANSITION_SAMPLES,
                baseline_cap = Cap,
                baseline_goodput = BaselineGoodput
            })
    end.

step(up, Cap, Step) ->
    Cap + Step;
step(down, Cap, _Step) when Cap =< ?CONCURRENCY_CAP_INITIAL ->
    Cap;
step(down, Cap, Step) ->
    max(?CONCURRENCY_CAP_INITIAL, Cap - Step).

%% @doc Return a probe's first step.
base_step(Cap) ->
    ceil(Cap / ?CONCURRENCY_CAP_STEP_DIVISOR).

%% @doc Return the step after a successful one.
next_step(Step, Cap) ->
    min(2 * Step, Cap).

%% @doc Keep a productive step, or step down if the window gained nothing.
finish_up(Control) ->
    #cap_control{
        cap = StepCap,
        step = Step,
        baseline_cap = BaselineCap,
        baseline_goodput = BaselineGoodput
    } = Control,
    StepGoodput = window_goodput(Control),
    case StepGoodput > BaselineGoodput of
        true ->
            %% Keep the cap that produced this goodput. Even a small goodput
            %% gain may require the full increase in concurrency.
            Step2 =
                case
                    2 * (StepGoodput - BaselineGoodput) * BaselineCap >=
                        BaselineGoodput * (StepCap - BaselineCap)
                of
                    true -> next_step(Step, StepCap);
                    false -> max(Step, base_step(StepCap))
                end,
            start_phase(up, StepGoodput, Control#cap_control{step = Step2});
        false ->
            start_phase(down, BaselineGoodput, Control#cap_control{
                cap = BaselineCap, step = base_step(BaselineCap)
            })
    end.

%% @doc Finish a step down. If goodput held, the lower cap is kept and the next
%% step down is twice as large. If goodput dropped, the cap returns to where
%% the step started and rests before the next probe, so such costly steps stay
%% rare. Cutting the cap below the peer's saturation point costs goodput, while
%% extra concurrency above it costs nothing, so the cap errs on the high side
%% when readings are noisy.
finish_down(Control) ->
    #cap_control{ cap = Cap, step = Step, baseline_cap = BaselineCap,
        baseline_goodput = BaselineGoodput } = Control,
    StepGoodput = window_goodput(Control),
    case StepGoodput >= BaselineGoodput of
        true ->
            start_phase(down, StepGoodput, Control#cap_control{
                step = next_step(Step, Cap) });
        false ->
            hold(BaselineCap, ?PROBE_REST_SAMPLES, Control)
    end.

%% @doc End the probe at Cap and start measuring the next probe's baseline.
%%
%% Skip: the number of goodput samples to drop before the baseline; at least
%% one, because a probe always ends at a different cap from the one it last
%% measured.
hold(Cap, Skip, Control) ->
    reset_window(Control#cap_control{
        phase = hold,
        cap = Cap,
        step = base_step(Cap),
        skip = Skip,
        baseline_cap = undefined,
        baseline_goodput = undefined
    }).

%% @doc Estimate the serving rate reached in at least two window samples.
window_goodput(#cap_control{window_samples = Samples}) ->
    %% Ignore brief slowdowns and require more than one fast burst to show
    %% extra capacity. Keep progress visible when slow requests complete in
    %% batches separated by empty ticks.
    %% Throughput accounting and failure pressure include every request.
    lists:nth(length(Samples) - 1, lists:sort(Samples)).

reset_window(Control) ->
    Control#cap_control{window_samples = []}.

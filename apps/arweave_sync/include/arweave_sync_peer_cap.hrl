%%%===================================================================
%%% Failure cuts.
%%%===================================================================

%% Failure cuts never take the cap below CONCURRENCY_CAP_MIN, so even a failing
%% peer keeps one fetch running.
-define(CONCURRENCY_CAP_MIN, 1).

%% A failure cut removes failure pressure times FAILURE_CUT_GAIN of the cap, up
%% to MAX_TICK_CUT. For example, losing a third of fetch time to failures
%% halves the cap.
-define(FAILURE_CUT_GAIN, 1.5).

%% One tick cuts at most MAX_TICK_CUT of the cap, so that a burst of failures
%% cannot wipe out the cap all at once.
-define(MAX_TICK_CUT, 0.5).

%%%===================================================================
%%% Probing.
%%%===================================================================

%% Terms:
%% - tick: one scheduler control step, ten seconds in production;
%% - goodput sample: the chunk bytes fetched from a peer during one tick and the
%%   tick's length; its goodput is the bytes per millisecond;
%% - driven: at some point during the tick, work waited in the peer's queue
%%   while the peer was at its concurrency cap and the download limit and
%%   chunk cache had room, so the peer, not its supply of work, set its
%%   goodput;
%% - window: GOODPUT_WINDOW_SAMPLES goodput samples from ticks where the peer
%%   was driven; the window's goodput is its total bytes over its total time;
%% - step: one move of the cap up or down; the probe judges a step by comparing
%%   the window at the new cap with the window before it;
%% - probe: steps that try a higher cap and, if that gains nothing, a lower
%%   one. A probe whose step down costs goodput is followed by a rest.

%% A peer this node has not measured yet starts at CONCURRENCY_CAP_INITIAL:
%% low enough to be polite to an unknown peer, yet high enough to measure the
%% peer's goodput. A step down never goes below this cap.
-define(CONCURRENCY_CAP_INITIAL, 8).

%% A window holds GOODPUT_WINDOW_SAMPLES goodput samples. The probe judges each
%% step by a window's goodput, and the peer queue is sized from the mean
%% goodput of the peer's last GOODPUT_WINDOW_SAMPLES goodput samples.
-define(GOODPUT_WINDOW_SAMPLES, 6).

%% After a cap change, the probe skips GOODPUT_TRANSITION_SAMPLES goodput
%% samples, so
%% that fetches started under the old cap can finish before the new cap is
%% measured.
-define(GOODPUT_TRANSITION_SAMPLES, 1).

%% A probe's first step moves the cap up or down by
%% 1/CONCURRENCY_CAP_STEP_DIVISOR of the cap: a cap of 24 steps to 27 or 21. The
%% step doubles in size (27, then 33) when goodput rose by at least half as much
%% as the cap did (a 1/16 gain on the first step), or when a step down held
%% goodput.
-define(CONCURRENCY_CAP_STEP_DIVISOR, 8).

%% After a step down that cost goodput, the probe rests for PROBE_REST_SAMPLES
%% goodput samples: seven windows, so the cap holds for eight windows (eight
%% minutes at the production tick) before the next probe. When the cap already
%% matches what the peer can serve, every probe ends in such a step, and the
%% rest keeps the cost of those probes under about 1.5% of goodput.
-define(PROBE_REST_SAMPLES, 7 * ?GOODPUT_WINDOW_SAMPLES).

%% A peer's cap and probing state.
-record(cap_control, {
    cap = ?CONCURRENCY_CAP_INITIAL,
    %% hold: the current probe is measuring a baseline; up or down: the
    %% current probe is measuring a higher or lower cap to compare goodput
    %% against the previous cap.
    phase = hold,
    %% Move the cap by `step` requests. For a new peer the step equals the
    %% initial cap, so the first step up doubles the cap.
    step = ?CONCURRENCY_CAP_INITIAL,
    %% The number of goodput samples to skip before the window starts: one
    %% after a cap change, or PROBE_REST_SAMPLES after a step down that cost
    %% goodput.
    skip = 0,
    %% While probing: the cap the step started from, and the goodput measured
    %% there.
    baseline_cap = undefined,
    baseline_goodput = undefined,
    %% The totals for the window the probe is measuring.
    window_bytes = 0,
    window_ms = 0,
    window_samples = 0
}).

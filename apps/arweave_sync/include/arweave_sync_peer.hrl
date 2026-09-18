%%% State shared with focused tests.

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

%% Field order is dispatch precedence because records use Erlang term ordering.
-record(source_priority, {
    peer_load,
    store_load,
    peer
}).

-ifndef(AR_SYNC_SIM_HRL).
-define(AR_SYNC_SIM_HRL, true).

%% Simulated time advances in this quantum; peer latency must align with it so
%% worker wakeups and round trips remain deterministic.
-define(SIM_SUBSTEP_MS, 250).
%% Detailed chunk-interval requests default to four substeps, or one simulated
%% second.
-define(SIM_METADATA_LATENCY_MS, (4 * ?SIM_SUBSTEP_MS)).
%% Simulated stores span three mainnet partitions. This is fixed world topology,
%% not a scenario control.
-define(SIM_STORES, 6).
-define(SIM_STORE_PARTITIONS, 3).
-define(SIM_STORE_SIZE, (?SIM_STORE_PARTITIONS * ?MAINNET_PARTITION_SIZE)).
%% The base is divisible by the store size so config conversion preserves the
%% declared ranges exactly.
-define(SIM_STORE_BASE, (99 * ?MAINNET_PARTITION_SIZE)).

%% A simulated peer's initial and runtime configuration.
-record(sim_peer, {
    %% Serving capacity, chunks per simulated second. Requests above this limit
    %% receive HTTP 429 when rate limiting is enabled; otherwise they wait for the
    %% next simulated second.
    max_serve_cps = 0 :: non_neg_integer(),
    %% Round-trip latency: constant, or a function of the peer's in-flight
    %% request count at the moment the request arrives. The function form is how
    %% a peer that degrades under parallel load is modelled — being overwhelmed
    %% shows up as a longer round trip, not as a separate capacity dial. It can
    %% express a peer whose delivery flattens once its parallelism is saturated
    %% (`ServiceMs * max(1, ceil(Inflight / Slots))'), and equally one whose
    %% delivery DECLINES past a point, as seek thrash does.
    %%
    %% Distinct from the latency max_serve_cps already induces: that models
    %% queueing at US and recedes when the caller reduces its depth, whereas
    %% this can model the peer's own service time, which does not.
    latency_ms = 250 :: pos_integer() | fun((non_neg_integer()) -> pos_integer()),
    %% Whether requests above max_serve_cps receive HTTP 429 instead of waiting.
    limited = false :: boolean(),
    %% Whether peer selection sees this peer as near its outbound quota.
    selection_throttled = false :: boolean(),
    %% Per-peer maximum concurrent HTTP chunk requests. A fetch above this limit
    %% returns {error, client_error}; infinity disables the simulated constraint.
    http_inflight_limit = infinity :: non_neg_integer() | infinity,
    %% Network release advertised by the metadata endpoints.
    release = 100 :: non_neg_integer(),
    %% Metadata representations advertised by this peer.
    sync_kinds = [byte] :: [byte | footprint],
    %% Data advertised through the peer's metadata endpoints. Store selectors
    %% resolve to bounded prefixes, keeping topology scenarios declarative.
    %% Exact intervals support sparse and frontier contracts; `all` covers every
    %% store's bounded prefix.
    sync_availability = all :: all |
        {stores, [term()]} |
        {intervals, [{non_neg_integer(), non_neg_integer()}]},
    %% Whether a footprint endpoint reports every chunk in an advertised
    %% footprint or only the chunks named by exact advertised intervals.
    footprint_coverage = full :: full | exact,
    %% Response time for detailed byte and footprint availability requests.
    %% This is independent of chunk-fetch latency because the endpoints may
    %% have different storage and caching behavior.
    chunk_interval_latency_ms = ?SIM_METADATA_LATENCY_MS :: pos_integer() |
        fun((non_neg_integer()) -> pos_integer()),
    %% Optional request outcome selected by scheduler tick and request sequence.
    failure_policy = undefined :: undefined |
        fun((non_neg_integer(), pos_integer()) ->
            none | reject | timeout | client_error |
            {reject | timeout | client_error, pos_integer()})
}).

    %% Initial definition of a simulated sync world.
-record(sim_world, {
    node_config = #{} :: map(),
    peers = #{} :: #{term() => #sim_peer{}},
    local_data_layout = contiguous :: contiguous | fragmented,
    %% Time spent generating the entropy working set for one source footprint.
    %% Zero leaves entropy generation unmodelled for scenarios that do not
    %% exercise source locality.
    entropy_generation_ms = 0 :: non_neg_integer(),
    link_capacity_cps = infinity :: non_neg_integer() | infinity,
    remote_store_cps = infinity :: non_neg_integer() | infinity,
    %% Maximum completed writes per store and simulated second. A function may
    %% change the rate by scheduler tick; zero models a stalled store.
    store_write_cps = infinity :: non_neg_integer() | infinity |
        fun((term(), non_neg_integer()) -> non_neg_integer() | infinity)
}).

%% Mutable outputs sampled from a simulated world.
-record(sim_snapshot, {
    served_by_peer = #{} :: map(),
    rejected_by_peer = #{} :: map(),
    timed_out_by_peer = #{} :: map(),
    client_errors_by_peer = #{} :: map(),
    http_inflight_by_peer = #{} :: map(),
    %% Completed chunk writes provide the per-store progress signal used by
    %% store-distribution scenarios.
    chunks_stored_by_store = #{} :: map()
}).

%% Snapshots bounding one measured simulation interval.
-record(sim_measurement, {
    duration_seconds :: pos_integer(),
    before_snapshot :: #sim_snapshot{},
    after_snapshot :: #sim_snapshot{}
}).

-endif.

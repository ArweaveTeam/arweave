%%% State shared with focused tests.

%% Enough queued work to cover transient write variance without letting a slow
%% store monopolize the chunk cache.
-define(WRITE_QUEUE_TARGET_DURATION_MS, 5000).

%% A delayed fetch can occupy five seconds before entering the five-second
%% write queue. One additional second covers strict admission boundaries and
%% scheduler/completion-wave quantization.
-define(ASSIGNMENT_TARGET_DURATION_MS, 11_000).

%% Smooth write-completion bursts over roughly two drain-rate observations.
%% The first continuously backlogged interval seeds the estimate directly.
-define(DRAIN_RATE_ALPHA, 0.5).

%% Measure across at least one production scheduler interval so batched writes
%% are not interpreted as alternating zero-rate and burst-rate samples.
-define(DRAIN_SAMPLE_MIN_MS, 10_000).

%% Keep a small probe available after a zero or very low drain-rate sample.
%% Twenty-five chunks bound recovery work while leaving enough requests to
%% observe whether a stalled store has resumed draining.
-define(MIN_CLAIM_LIMIT, 25).

-record(store_state, {
    store_id,
    %% Persistent work retained across dispatch passes.
    work_queue = gb_sets:new(),
    claimed = ar_intervals:new(),
    %% Concrete tasks that can enter the fetch/write pipeline.
    claimed_chunks = 0,
    %% Whole-footprint credits retained only until a reservation binds.
    reservation_chunks = 0,
    queued_peer_counts = #{},
    queued_task_count = 0,
    completed_writes_since_sample = 0,
    observed_write_count = undefined,
    drain_sample_started_ms = undefined,
    drain_sample_starved = false,
    drain_rate = undefined
}).

%% One store's mutable snapshot during a scheduler dispatch pass.
-record(store_dispatch, {
    state,
    %% Tasks already assigned to peer queues but not yet fetching.
    bound_count = 0,
    fetching_count = 0,
    writing_count = 0,
    cached_chunk_count = 0,
    %% Drain-derived limit for chunks that have reached the local cache.
    cache_limit = 0,
    %% Hard fair share for cached chunks plus fetches that can become cached.
    pipeline_limit = 0,
    disk_ready = false,
    %% Dispatch-pass copy plus bound footprints eligible during this pass.
    work_queue = gb_sets:new(),
    %% Peers offering queued tasks or bound footprint work to this store.
    peers = sets:new()
}).

%% Field order is dispatch precedence because records use Erlang term ordering.
-record(store_priority, {
    fetching_count,
    active_count,
    store_id
}).

%% Concrete chunks precede bound footprints and queued footprints at the same
%% position.
-record(work_priority, {
    position,
    kind_rank
}).

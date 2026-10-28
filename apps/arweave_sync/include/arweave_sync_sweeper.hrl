%% After a sweep completes, the sweeper waits SWEEP_RESTART_DELAY_MS before
%% starting the next one, so a fully synced store does not spin the CPU and
%% fill the log.
-ifdef(AR_TEST).
-define(SWEEP_RESTART_DELAY_MS, 1_000).
-else.
-define(SWEEP_RESTART_DELAY_MS, 10_000).
-endif.

%% When the store's claim limit blocks a claim, the sweeper retries after
%% BLOCKED_RETRY_DELAY_MS.
-define(BLOCKED_RETRY_DELAY_MS, 200).

%% The byte and footprint sweep queues hold up to BYTE_SWEEP_QUEUE_MAX_LENGTH
%% and FOOTPRINT_SWEEP_QUEUE_MAX_LENGTH ranges. Discovery fetches peer metadata
%% for every queued range, so these set how far ahead of the claims discovery
%% works: eight 1 GB byte steps, or 32 footprints of 256 MiB (8 GiB).
-define(BYTE_SWEEP_QUEUE_MAX_LENGTH, 8).

-define(FOOTPRINT_SWEEP_QUEUE_MAX_LENGTH, 32).

%% After queueing a range, the sweeper waits SWEEP_RANGE_WARM_WAIT_MS before
%% claiming it. This gives discovery time to fetch the range's peer metadata.
%% The claim uses whatever the cache holds by then; responses that arrive
%% later still help the next sweep.
-define(SWEEP_RANGE_WARM_WAIT_MS, 10_000).

%% Footprint ranges are queued in batches, so their SWEEP_RANGE_WARM_WAIT_MS
%% waits end at about the same time. After claiming a footprint range, the
%% sweeper waits SWEEP_RANGE_CADENCE_MS before claiming the next one if
%% discovery has not cached that range's metadata yet. This keeps the sweeper
%% from claiming the whole batch before it knows the ranges' sources.
-define(SWEEP_RANGE_CADENCE_MS, 1_000).

%% A range whose claim skipped chunks because their sources had no room is
%% parked, and the sweeper tries to claim those chunks again every
%% PARKED_RANGE_RETRY_DELAY_MS. A store holds at most a peer's share of its
%% claim limit for each peer, which a peer serving one chunk per second
%% drains in about twelve seconds, so a second keeps such a peer busy.
-define(PARKED_RANGE_RETRY_DELAY_MS, 1_000).

%% The sweeper saves its cursor offsets at most every
%% SWEEP_CURSOR_SAVE_INTERVAL_MS, so a restart resumes the sweep about where it
%% stopped instead of at the store start.
-define(SWEEP_CURSOR_SAVE_INTERVAL_MS, 10_000).

-define(CHUNK_PATH, "/chunk2").

%% One range in a sweep queue. When `unsynced_range' is none, the store needs
%% nothing in the range, and the entry only moves the cursor forward in queue
%% order. Otherwise the entry holds one #unsynced_range{}, the time discovery
%% was asked for the range's peer metadata, where a partial claim left off,
%% and the first chunk a claim skipped because its sources had no room. A
%% parked range also holds the time to claim it again.
%%
%% A byte range covers at most one byte sweep step, plus any chunks missing
%% from the footprint that contains the step's first needed byte, so that byte
%% and footprint peers can share the work. A footprint range covers one
%% footprint.
-record(sweep_range, {
    mode,
    offset,
    next_offset,
    unsynced_range = none,
    requested_at,
    next_claim_offset = undefined,
    skipped_from = undefined,
    retry_at_ms = undefined
}).

-record(state, {
    store_id,
    range_start = -1 :: integer(),
    range_end = -1 :: integer(),
    weave_size :: undefined | non_neg_integer(),
    disk_pool_threshold :: undefined | non_neg_integer(),
    sync_status = undefined,
    cursor = undefined :: undefined | arweave_sync_cursor:t(),
    readahead_cursor = undefined,
    sweep_queues = #{
        byte => queue:new(),
        footprint => queue:new()
    },
    next_mode = byte,
    %% The parked ranges of each mode: ranges whose claims skipped chunks
    %% because their sources had no room, kept across sweeps.
    parked_ranges = #{byte => [], footprint => []},
    %% The due time and token of the next scheduled sweep. An earlier request
    %% replaces the token, and the sweeper ignores the old timer when it fires.
    next_sweep = undefined :: undefined | {integer(), reference()},
    %% The byte and footprint offsets saved by an earlier run, where the first
    %% sweep resumes.
    resume_offsets = undefined :: undefined | {integer(), integer()},
    %% The offsets last saved and when.
    saved_offsets = undefined :: undefined | {integer(), integer()},
    saved_at_ms = undefined :: undefined | integer()
}).

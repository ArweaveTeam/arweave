%%% State shared with focused tests.

-ifdef(AR_TEST).
-define(SWEEP_RESTART_DELAY_MS, 1_000).
-else.
-define(SWEEP_RESTART_DELAY_MS, 10_000).
-endif.

%% Delay before retrying a range claim that could not be processed.
-define(BLOCKED_RETRY_DELAY_MS, 200).

%% Keep roughly 8 GiB of readahead in each mode: eight 1 GB byte query steps,
%% or thirty-two 256 MiB footprints. Each queue includes its active head.
-define(BYTE_SWEEP_QUEUE_MAX_LENGTH, 8).

-define(FOOTPRINT_SWEEP_QUEUE_MAX_LENGTH, 32).

%% Give discovery ten seconds to warm a non-empty queued range. Once this fixed
%% minimum age is reached, use whatever peer metadata is currently cached;
%% slower responses can populate the cache for a later sweep.
-define(SWEEP_RANGE_WARM_WAIT_MS, 10_000).

%% Thirty-two footprint metadata requests may not all finish during the fixed
%% warm gate when a peer serves them serially. Give a newly exposed missing
%% footprint head one more second without delaying byte or cached work.
-define(SWEEP_RANGE_CADENCE_MS, 1_000).

-define(CHUNK_PATH, "/chunk2").

%% One queued sweep range. `unsynced_range = none' records cursor progress that
%% found no local need; otherwise the range tracks the metadata request and any
%% partially claimed remainder for one actual #unsynced_range{}.
%%
%% A non-empty sweep range contains exactly one unsynced range. Byte mode initially
%% examines at most one query-range grid step (?QUERY_RANGE_STEP_SIZE in
%% production), but may add missing chunks from the containing footprint so
%% byte and footprint peers can share the work; its total intervals are therefore
%% not strictly capped to that step. Footprint mode examines one footprint-cadence
%% step per queued range.
-record(sweep_range, {
    mode,
    offset,
    next_offset,
    unsynced_range = none,
    requested_at,
    next_claim_offset = undefined
}).

-record(state, {
    %% Store identifier this state belongs to.
    store_id,
    %% Start offset of the store's range.
    range_start = -1 :: integer(),
    %% End offset of the store's range.
    range_end = -1 :: integer(),
    %% Byte and footprint bounds refreshed together on each chain-tip update.
    weave_size :: undefined | non_neg_integer(),
    disk_pool_threshold :: undefined | non_neg_integer(),
    %% Mirror of ar_device_lock's view of this module's sync-mode lock.
    sync_status = undefined,
    %% Cursor for the in-progress sweep. `undefined' means the loop hasn't
    %% started its first sweep yet.
    cursor = undefined :: undefined | arweave_sync_cursor:t(),
    %% Cursor at the end of the unsynced ranges already in the sweep queues.
    readahead_cursor = undefined,
    %% Bounded FIFO sweep ranges, keyed by cursor mode.
    sweep_queues = #{
        byte => queue:new(),
        footprint => queue:new()
    },
    next_mode = byte,
    %% Due time and token for the next scheduled sweep. An earlier request
    %% replaces the token; the superseded timer is ignored when it arrives.
    next_sweep = undefined :: undefined | {integer(), reference()}
}).

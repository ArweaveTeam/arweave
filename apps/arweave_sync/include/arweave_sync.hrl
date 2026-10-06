-include_lib("arweave_lib/include/arweave_lib_constants.hrl").

-define(SYNC_BUCKET_CACHE_TABLE, arweave_sync_discovery).
-define(CHUNK_INTERVAL_CACHE_TABLE, arweave_sync_chunk_intervals).

%% The scheduler ticks every TICK_INTERVAL_MS.
-define(TICK_INTERVAL_MS, 10_000).

%% A chunk request times out after FETCH_TIMEOUT_MS. Forty seconds leaves
%% plenty of time for a slow but valid response while still freeing a dead
%% request quickly.
-define(FETCH_TIMEOUT_MS, 40_000).

%% For each sweep range, the sweeper considers up to QUERY_BEST_PEERS_COUNT
%% peers, sampled at random from the peers that advertise data in the range's
%% sync bucket.
-define(QUERY_BEST_PEERS_COUNT, 30).

%% A byte sweep step covers QUERY_RANGE_STEP_SIZE bytes. arweave_sync_discovery
%% fetches peers' byte intervals in steps of the same size, so one peer
%% response covers exactly one sweep step.
-define(QUERY_RANGE_STEP_SIZE, 1_000_000_000).

%%%===================================================================
%%% Pipeline range records.
%%%===================================================================

%% A range of a store that the sweeper found unsynced. The chunk picker matches
%% the range against the intervals that peers advertise.
-record(unsynced_range, {
    kind :: byte | footprint,
    query_offset :: non_neg_integer(),
    %% The byte intervals the store needs.
    intervals :: arweave_lib_intervals:intervals(),
    range_start :: non_neg_integer(),
    range_end :: non_neg_integer(),
    advance :: term()
}).

%% The chunks of one footprint in one store. Entropy differs from peer to peer,
%% so the scheduler binds each active footprint batch to one peer and holds
%% that peer's entropy until the batch's tasks finish fetching.
%%
%% The peer of the active batch is kept in arweave_sync_scheduler's footprints
%% map, not in this record, so each footprint of a store has only one key in
%% that map.
-record(footprint, {
    store_id :: term(),
    partition :: non_neg_integer(),
    footprint :: non_neg_integer()
}).

%% arweave_sync_discovery returns cached peer ranges as these records.
-record(peer_range, {
    store_id :: term(),
    offset :: non_neg_integer(),
    peer :: term(),
    %% Byte intervals, or footprint-record intervals when footprint is set.
    intervals :: arweave_lib_intervals:intervals(),
    footprint :: none | #footprint{}
}).

%% A peer that can serve a task's chunk, by byte offset or by footprint. A
%% footprint reservation keeps the intervals the peer advertised until the
%% scheduler turns them into tasks, one batch at a time.
-record(task_source, {
    peer,
    footprint = none,
    intervals = undefined
}).

%% One chunk to fetch with one /chunk2 request. The task's state is one of:
%% - queued: waiting in a store queue, or in a peer queue once bound to a peer
%% - fetching: a fetch worker is running the request
%% - writing: the fetch succeeded and storage is writing the chunk
%% - write_complete: the write finished before the fetch worker reported its
%%   result
%% The scheduler drops the task and releases its store claim when the fetch
%% fails, or once both the fetch result and the write result have arrived.
-record(task, {
    offset = undefined :: undefined | non_neg_integer(),
    sources = [],
    peer = undefined,
    store_id,
    footprint = none,
    task_ref = undefined,
    state = queued :: queued | fetching | writing | write_complete
}).

%% Pending work for one footprint of a store. A queued reservation keeps every
%% source the chunk picker found. A bound reservation keeps the source the
%% scheduler picked and the intervals that are not yet tasks, while earlier
%% batches of tasks are queued or fetching. A draining reservation lets its
%% current tasks finish but gets no new batch.
-record(footprint_reservation, {
    sources = [],
    peer = undefined,
    store_id,
    footprint,
    active_tasks = 0,
    state = queued :: queued | bound | draining
}).

%% The worker time that fetch attempts take, split by outcome. The scheduler
%% sums these values per peer, so that peer concurrency reacts to how much time
%% failures cost rather than to how many there are.
-record(fetch_timing, {
    productive_ms = 0 :: non_neg_integer(),
    reject_ms = 0 :: non_neg_integer(),
    timeout_ms = 0 :: non_neg_integer(),
    client_error_ms = 0 :: non_neg_integer()
}).

%%%===================================================================
%%% Peer protocol.
%%%===================================================================

%% Share at most this many synced intervals with peers.
-ifdef(AR_TEST).
-define(MAX_SHARED_SYNCED_INTERVALS_COUNT, 20).
-else.
-define(MAX_SHARED_SYNCED_INTERVALS_COUNT, 10_000).
-endif.

%% The upper limit for the size of a sync record in Erlang Term Format.
-define(MAX_ETF_SYNC_RECORD_SIZE, 80 * ?MAX_SHARED_SYNCED_INTERVALS_COUNT).

%% The HTTP timeout for GET /data_sync_record requests.
-define(DATA_SYNC_RECORD_TIMEOUT_MS, 30_000).

%% byte_size(ar_serialize:jsonify(jiffy:encode(#{ packing => "replica_2_9_" ++ binary_to_list(crypto:strong_rand_bytes(32)), intervals => [[integer_to_list(trunc(math:pow(2, 256) - 1)), integer_to_list(trunc(math:pow(2, 256) - 1))] || _ <- lists:seq(1, 512)] }))).
%% 243238
-define(MAX_FOOTPRINT_PAYLOAD_SIZE, 250_000).

%% The upper limit for the size of the sync buckets in Erlang Term Format.
-define(MAX_SYNC_BUCKETS_SIZE, 100_000).

%% Peers support GET /data_sync_record/[start]/[end]/[limit] from release
%% GET_SYNC_RECORD_RIGHT_BOUND_SUPPORT_RELEASE on.
-define(GET_SYNC_RECORD_RIGHT_BOUND_SUPPORT_RELEASE, 83).

%% Peers support these endpoints from release GET_FOOTPRINT_SUPPORT_RELEASE on:
%% GET /footprints/[partition]/[footprint]
%% GET /footprint_buckets
-define(GET_FOOTPRINT_SUPPORT_RELEASE, 91).

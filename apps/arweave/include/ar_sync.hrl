%%% Shared records and constants for the network chunk-sync pipeline.

%%%===================================================================
%%% Discovery cache tables.
%%%===================================================================

-define(SYNC_BUCKET_CACHE_TABLE, ar_sync_discovery).
-define(CHUNK_INTERVAL_CACHE_TABLE, ar_sync_chunk_intervals).

%%%===================================================================
%%% Pipeline range records.
%%%===================================================================

%% A locally needed unsynced range to resolve against peer-advertised intervals.
%% It may not be fetchable yet if every matching peer is throttled or still needs
%% a chunk interval job, but it is not speculative local work.
-record(unsynced_range, {
    kind :: byte | footprint,
    query_offset :: non_neg_integer(),
    %% Local need represented in byte space. A byte sweep range includes both
    %% its byte-record need and the footprint-record need at query_offset.
    intervals :: ar_intervals:intervals(),
    range_start :: non_neg_integer(),
    range_end :: non_neg_integer(),
    advance :: term()
}).

%% The chunks of one replica.2.9 footprint in one storage module. Entropy is
%% peer-specific, so the scheduler binds each active footprint batch to one
%% peer and holds that source's entropy until the batch's tasks finish.
%% A later sweep may bind still-unsynced chunks to another peer.
%%
%% The active batch's peer lives in ar_sync_scheduler's footprints map, not in
%% this record. Keeping it out makes one store/partition/footprint one active
%% footprint key.
-record(footprint, {
    store_id :: term(),
    partition :: non_neg_integer(),
    footprint :: non_neg_integer()
}).

%% Cached peer ranges returned by ar_sync_discovery.
-record(peer_range, {
    store_id :: term(),
    offset :: non_neg_integer(),
    peer :: term(),
    intervals :: ar_intervals:intervals(),
    footprint :: none | #footprint{}
}).

%% A peer and representation that can serve task data. Footprint reservations
%% retain advertised intervals until finite batches are enqueued as tasks.
-record(task_source, {
    peer,
    footprint = none,
    intervals = undefined
}).

%% One executable /chunk2 request.
-record(task, {
    offset = undefined :: undefined | non_neg_integer(),
    sources = [],
    peer = undefined,
    store_id,
    footprint = none,
    task_ref = undefined,
    state = fetching :: queued | fetching | writing | write_complete
}).

%% Deferred work for one local footprint. A queued reservation retains every
%% source known when it was created. A bound reservation retains the selected
%% source and intervals not yet enqueued while finite task
%% batches run. A draining reservation finishes its enqueued tasks but receives
%% no new batch.
-record(footprint_reservation, {
    sources = [],
    peer = undefined,
    store_id,
    footprint,
    active_tasks = 0,
    state = queued
}).

%% Worker time consumed by network fetch attempts. The scheduler aggregates
%% these values by peer so the concurrency controller responds to the cost of
%% failures rather than their raw count.
-record(fetch_timing, {
    productive_ms = 0 :: non_neg_integer(),
    reject_ms = 0 :: non_neg_integer(),
    timeout_ms = 0 :: non_neg_integer(),
    client_error_ms = 0 :: non_neg_integer()
}).

%%%===================================================================
%%% Chunk fetch deadline.
%%%===================================================================

%% Deadline for a single chunk request. Ten times the four-second delivered-work
%% horizon in ar_sync_peer: a chunk request younger than the queue it stands in
%% is alive; one that has outlived it several times over is dead. The
%% BitTorrent-style separation keeps the queue equilibrium far from the death
%% sentence — at the previous flat 120 s a hung request held its worker slot for
%% two minutes.
%%
%% This is also what bounds a fetch in ar_sync_sim_world, so a simulated peer
%% that stops progressing releases its worker and claim on the same schedule a
%% real one does. The two must not drift: the scheduler's claim lifetime is
%% derived from it, and a simulation that released sooner or later than
%% production would model a different pipeline.
-define(FETCH_TIMEOUT_MS, 40_000).

%%%===================================================================
%%% Sweep and peer-query sizing.
%%%===================================================================

%% How many peers with the biggest synced shares in the given bucket to query per bucket
%% per sync job iteration.
-define(QUERY_BEST_PEERS_COUNT, 30).

%% The size of the span of the weave we search at a time. The store sweep
%% walks the unsynced range in increments of this size;
%% ar_sync_discovery fetches byte-mode chunk intervals at the same granularity,
%% so one peer response covers one query range exactly.
-define(QUERY_RANGE_STEP_SIZE, 1_000_000_000). % 1 GB

%%%===================================================================
%%% Peer bucket sizing.
%%%===================================================================

%% The size in bytes of a bucket used to group peers' sync records. When we want to sync
%% an interval, we process it bucket by bucket: for every bucket, a few peers who are known to
%% to have some data there are asked for the intervals they have and check which of them
%% cross the desired interval.
-ifdef(AR_TEST).
-define(NETWORK_DATA_BUCKET_SIZE, 10_000_000). % 10 MB
-else.
-define(NETWORK_DATA_BUCKET_SIZE, 10_000_000_000). % 10 GB
-endif.

%% Similar to ?NETWORK_DATA_BUCKET_SIZE, except for a footprint bucket
%% contains several "footprints" - sets of chunks spread out across the partition.
-ifdef(AR_TEST).
-define(NETWORK_FOOTPRINT_BUCKET_SIZE, 36). % 12 (footprints) * 3 (chunks); ~10 MB
-else.
-define(NETWORK_FOOTPRINT_BUCKET_SIZE, 37888). % 37 (footprints) * 1024 (chunks); ~10 GB
-endif.

%%%===================================================================
%%% Network payload limits.
%%%===================================================================

%% The maximum number of synced intervals shared with peers.
-ifdef(AR_TEST).
-define(MAX_SHARED_SYNCED_INTERVALS_COUNT, 20).
-else.
-define(MAX_SHARED_SYNCED_INTERVALS_COUNT, 10_000).
-endif.

%% The upper limit for the size of a sync record serialized using Erlang Term Format.
-define(MAX_ETF_SYNC_RECORD_SIZE, 80 * ?MAX_SHARED_SYNCED_INTERVALS_COUNT).

%% byte_size(ar_serialize:jsonify(jiffy:encode(#{ packing => "replica_2_9_" ++ binary_to_list(crypto:strong_rand_bytes(32)), intervals => [[integer_to_list(trunc(math:pow(2, 256) - 1)), integer_to_list(trunc(math:pow(2, 256) - 1))] || _ <- lists:seq(1, 512)] }))).
%% 243238
-define(MAX_FOOTPRINT_PAYLOAD_SIZE, 250_000).

%% The upper limit for the size of the serialized (in Erlang Term Format) sync buckets.
-define(MAX_SYNC_BUCKETS_SIZE, 100_000).

%%%===================================================================
%%% Peer protocol releases.
%%%===================================================================

%% The number of the release adding support for the
%% GET /data_sync_record/[start]/[end]/[limit] endpoint.
-define(GET_SYNC_RECORD_RIGHT_BOUND_SUPPORT_RELEASE, 83).

%% The number of the release adding support for endpoints:
%% GET /footprints/[partition]/[footprint]
%% GET /footprint_buckets
-define(GET_FOOTPRINT_SUPPORT_RELEASE, 91).

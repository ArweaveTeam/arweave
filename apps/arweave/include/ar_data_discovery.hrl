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

%% How many peers with the biggest synced shares in the given bucket to query per bucket
%% per sync job iteration.
-define(QUERY_BEST_PEERS_COUNT, 15).

%% The number of the release adding support for the
%% GET /data_sync_record/[start]/[end]/[limit] endpoint.
-define(GET_SYNC_RECORD_RIGHT_BOUND_SUPPORT_RELEASE, 83).

%% The number of the release adding support for endpoints:
%% GET /footprints/[partition]/[footprint]
%% GET /footprint_buckets
-define(GET_FOOTPRINT_SUPPORT_RELEASE, 91).

%% Fine-grained peer interval cache TTL. After this many milliseconds a
%% cached peer response is considered stale and a fresh HTTP call is issued
%% on the next read. Matches the rate-limit cooldown so refresh cadence
%% aligns with the peer's tolerance.
-define(PEER_INTERVAL_CACHE_TTL_MS, 60 * 1000).

%% The size of the span of the weave we search at a time. The planner walks
%% the unsynced range in increments of this size; the directory prefetches
%% peer intervals at the same granularity so one peer response covers one
%% scan step exactly.
-ifdef(AR_TEST).
-define(QUERY_RANGE_STEP_SIZE, 10_000_000). % 10 MB
-else.
-define(QUERY_RANGE_STEP_SIZE, 1_000_000_000). % 1 GB
-endif.

%% How many scan steps ahead of the planner cursor the directory prefetches
%% in normal mode. Keep the cache warm for the planner's next few ticks.
-define(PREFETCH_STEPS_AHEAD, 10).

%% How many footprints ahead of the planner cursor the directory prefetches
%% in footprint mode. Each prefetch is one `GET /footprints/{P}/{F}` call
%% returning all intervals for that footprint from one peer.
-define(PREFETCH_FOOTPRINTS_AHEAD, 10).

%% Upper bound on the number of concurrent peer interval refresh HTTP calls
%% across the directory's refresh worker pool. Distinct from the
%% DATA_DISCOVERY_PARALLEL_PEER_REQUESTS pool that handles sync_buckets /
%% footprint_buckets fetches so the two pools don't starve each other.
-define(MAX_CONCURRENT_INTERVAL_REFRESHES, 10).
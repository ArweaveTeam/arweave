%% The size in bytes of a bucket used to group peers' sync records. When we want to sync
%% an interval, we process it bucket by bucket: for every bucket, a few peers who are known to
%% to have some data there are asked for the intervals they have and check which of them
%% cross the desired interval.
-ifdef(DEBUG).
-define(NETWORK_DATA_BUCKET_SIZE, 10000000).
-else.
-define(NETWORK_DATA_BUCKET_SIZE, 10000000000). % 10 GB
-endif.

%% The maximum number of synced intervals shared with peers.
-ifdef(DEBUG).
-define(MAX_SHARED_SYNCED_INTERVALS_COUNT, 20).
-else.
-define(MAX_SHARED_SYNCED_INTERVALS_COUNT, 10000).
-endif.

%% The upper limit for the size of a sync record serialized using Erlang Term Format.
-define(MAX_ETF_SYNC_RECORD_SIZE, 80 * ?MAX_SHARED_SYNCED_INTERVALS_COUNT).

%% The upper limit for the size of the serialized (in Erlang Term Format) sync buckets.
-define(MAX_SYNC_BUCKETS_SIZE, 100000).

%% How many peers with the biggest synced shares in the given bucket to query per bucket
%% per sync job iteration.
-define(QUERY_BEST_PEERS_COUNT, 30).

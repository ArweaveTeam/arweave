%% The size in bytes of a bucket in sync buckets. The bigger the bucket,
%% the more compact the structure is, but also the higher the number of "misses"
%% encountered when asking peers about the presence of particular chunks.
%% If the serialized buckets do not fit in ?MAX_SYNC_BUCKETS_SIZE, the bucket
%% size is doubled until they fit.
-ifdef(DEBUG).
-define(DEFAULT_SYNC_BUCKET_SIZE, 10000000).
-else.
-define(DEFAULT_SYNC_BUCKET_SIZE, 10000000000). % 10 GB
-endif.

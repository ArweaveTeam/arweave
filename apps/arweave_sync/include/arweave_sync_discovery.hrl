%%% State shared with focused tests.

-ifdef(AR_TEST).
-define(DATA_DISCOVERY_COLLECT_PEERS_FREQUENCY_MS, 2 * 1000).
-define(MAINTENANCE_INTERVAL_MS, 10_000).
-define(SYNC_BUCKET_JOB_INTERVAL_MS, 60_000).
-define(CHUNK_INTERVAL_CACHE_TTL_MS, 3_600_000).
-define(QUERY_SYNC_INTERVALS_COUNT_LIMIT, 10).
-else.
-define(DATA_DISCOVERY_COLLECT_PEERS_FREQUENCY_MS, 4 * 60 * 1000).
-define(MAINTENANCE_INTERVAL_MS, 60_000).
%% 1 hour
-define(SYNC_BUCKET_JOB_INTERVAL_MS, 60 * 60 * 1000).
%% 24 hours
-define(CHUNK_INTERVAL_CACHE_TTL_MS, 24 * 60 * 60 * 1000).
-define(QUERY_SYNC_INTERVALS_COUNT_LIMIT, 1000).
-endif.

%% Byte metadata may paginate, so keep one request per peer and store. A
%% footprint response covers one independent location and is small enough to
%% search several locations concurrently. This matters when the peer and local
%% holdings are both sparse: most successful responses may have no overlap.
-define(MAX_BYTE_JOBS_PER_PEER_STORE, 1).

-define(MAX_FOOTPRINT_JOBS_PER_PEER_STORE, 4).

%% Bound each kind of metadata work so discovery cannot saturate the shared
%% request path and starve chunk fetching.
-define(MAX_DISCOVERY_JOBS_PER_KIND, 200).

%% Peer collection considers at most one thousand current peers per cycle.
-define(MAX_DISCOVERY_PEERS, 1000).

%% A bounded queue keeps detailed metadata close enough to the sweep frontier
%% to be useful within its fixed warming interval.
-define(MAX_PENDING_CHUNK_INTERVAL_JOBS, 1024).

%% Even when the opportunistic queue is full, retain one byte and one footprint
%% job for each of the thirty peers a sweep range may consider. This lets a late
%% store establish metadata work without evicting another store's jobs.
-define(MIN_PENDING_CHUNK_INTERVAL_JOBS_PER_STORE,
    2 * ?QUERY_BEST_PEERS_COUNT
).

%% A normal query range needs about four pages. Sixteen bounds malformed
%% or unusually fragmented responses without constraining expected peers.
-define(MAX_CHUNK_INTERVAL_PAGES, 16).

%% Retain coarse rows for three job intervals so a brief failure does
%% not immediately retire advertised availability.
-define(SYNC_BUCKET_CACHE_TTL_MS, 3 * ?SYNC_BUCKET_JOB_INTERVAL_MS).

%% Drop the oldest tenth of the chunk interval cache when it exceeds its
%% byte budget; still-needed rows are re-warmed on demand.
-define(CHUNK_INTERVAL_CACHE_TRIM_DIVISOR, 10).

%% Discovery work pending or inflight.
-record(discovery_job, {
    key,
    kind,
    peer,
    store_id = undefined,
    mode = undefined,
    start = undefined,
    requested_at = undefined,
    pid = undefined
}).

%% Shared state for sync bucket and chunk interval jobs.
-record(discovery_jobs, {
    %% Exact job key => #discovery_job{}.
    pending = #{},
    %% Key => #discovery_job{ pid = pid() }.
    inflight = #{},
    %% Maximum concurrent jobs in this collection.
    max_inflight = ?MAX_DISCOVERY_JOBS_PER_KIND,
    %% Opportunistic queued-job limit. Detailed metadata may exceed it by the
    %% small per-store guarantee; sync bucket work has one exact job per peer.
    max_pending = ?MAX_DISCOVERY_PEERS
}).

-record(state, {
    %% Authoritative current peer set used to reject cache results from
    %% removed peers.
    tracked_peers = sets:new(),
    jobs = #{
        %% Coarse per-peer jobs. Each job fetches byte and footprint metadata
        %% sequentially, and its exact key limits one job per peer.
        sync_bucket => #discovery_jobs{},
        %% Demand-driven chunk interval jobs. Sync bucket jobs only update
        %% which peers hold which coarse sync buckets; chunk intervals
        %% from /footprints and /data_sync_record are fetched on demand for a
        %% bounded span beginning at a store's sweep frontier. Concurrency is
        %% limited so discovery cannot saturate the shared request path and
        %% starve /chunk2 fetching.
        chunk_interval => #discovery_jobs{
            max_pending = ?MAX_PENDING_CHUNK_INTERVAL_JOBS
        }
    }
}).

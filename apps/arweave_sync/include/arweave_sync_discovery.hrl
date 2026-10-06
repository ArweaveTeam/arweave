-ifdef(AR_TEST).
-define(DATA_DISCOVERY_COLLECT_PEERS_FREQUENCY_MS, 2 * 1000).
-define(MAINTENANCE_INTERVAL_MS, 10_000).
-define(SYNC_BUCKET_JOB_INTERVAL_MS, 60_000).
-define(CHUNK_INTERVAL_CACHE_TTL_MS, 3_600_000).
-define(QUERY_SYNC_INTERVALS_COUNT_LIMIT, 10).
-else.
-define(DATA_DISCOVERY_COLLECT_PEERS_FREQUENCY_MS, 4 * 60 * 1000).
-define(MAINTENANCE_INTERVAL_MS, 60_000).
-define(SYNC_BUCKET_JOB_INTERVAL_MS, 60 * 60 * 1000).
-define(CHUNK_INTERVAL_CACHE_TTL_MS, 24 * 60 * 60 * 1000).
-define(QUERY_SYNC_INTERVALS_COUNT_LIMIT, 1000).
-endif.

%% A byte metadata response can span several pages, so each peer and store
%% gets one byte job at a time. A footprint response covers a single location
%% and is small, so several footprint locations can be fetched at once. This
%% matters when the peer holds little of the data and the store needs little,
%% because most responses then contain no chunk the store needs.
-define(MAX_BYTE_JOBS_PER_PEER_STORE, 1).

-define(MAX_FOOTPRINT_JOBS_PER_PEER_STORE, 4).

%% Limit how many jobs of each kind run at once, so discovery cannot saturate
%% the shared request path and starve chunk fetching.
-define(MAX_DISCOVERY_JOBS_PER_KIND, 200).

-define(MAX_DISCOVERY_PEERS, 1000).

%% A short queue keeps pending chunk interval jobs close to the sweep
%% frontier, so their results are still useful when they arrive.
-define(MAX_PENDING_CHUNK_INTERVAL_JOBS, 1024).

%% Even when the shared queue is full, each store can queue one byte and one
%% footprint job for each of the QUERY_BEST_PEERS_COUNT peers that a sweep
%% range considers. A store that starts late can then queue work without
%% evicting other stores' jobs.
-define(MIN_PENDING_CHUNK_INTERVAL_JOBS_PER_STORE,
    2 * ?QUERY_BEST_PEERS_COUNT
).

%% A normal query range needs about four pages. A limit of 16 cuts off
%% malformed or unusually fragmented responses without affecting normal peers.
-define(MAX_CHUNK_INTERVAL_PAGES, 16).

%% Keep sync bucket rows for three refresh intervals, so a brief failure does
%% not immediately drop a peer's advertised buckets.
-define(SYNC_BUCKET_CACHE_TTL_MS, 3 * ?SYNC_BUCKET_JOB_INTERVAL_MS).

%% Drop the oldest tenth of the chunk interval cache when it exceeds its
%% byte budget. Rows that are still needed are re-warmed on demand.
-define(CHUNK_INTERVAL_CACHE_TRIM_DIVISOR, 10).

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

%% The pending and inflight jobs of one kind.
-record(discovery_jobs, {
    %% Jobs waiting to start, by key.
    pending = #{},
    %% Running jobs by key, with pid set to the job's process.
    inflight = #{},
    max_inflight = ?MAX_DISCOVERY_JOBS_PER_KIND,
    %% The limit on queued jobs. A store with fewer than
    %% MIN_PENDING_CHUNK_INTERVAL_JOBS_PER_STORE queued chunk interval jobs may
    %% go over it. Each peer has at most one sync bucket job.
    max_pending = ?MAX_DISCOVERY_PEERS
}).

-record(state, {
    tracked_peers = sets:new(),
    jobs = #{
        %% Sync bucket jobs, at most one per peer.
        sync_bucket => #discovery_jobs{},
        %% Chunk interval jobs, queued on demand for locations near a store's
        %% sweep frontier.
        chunk_interval => #discovery_jobs{
            max_pending = ?MAX_PENDING_CHUNK_INTERVAL_JOBS
        }
    }
}).

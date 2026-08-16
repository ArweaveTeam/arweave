-ifndef(AR_CONFIG_HRL).
-define(AR_CONFIG_HRL, true).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_verify_chunks.hrl").

%% Convert a legacy cache limit (in chunks) to the new-style MiB option using
%% CEILING division with a floor of 1, so a small legacy count never rounds down
%% to a smaller capacity - or to 0, which is invalid for the positive-integer
%% option. Chunks is always a bound variable at the call sites (no double-eval).
-define(LEGACY_CHUNKS_TO_CACHE_MIB(Chunks),
    max(1, (Chunks + (?MiB div ?DATA_CHUNK_SIZE) - 1) div (?MiB div ?DATA_CHUNK_SIZE))).

%% The polling frequency in seconds.
-define(DEFAULT_POLLING_INTERVAL, 2).

%% The number of processes periodically searching for the latest blocks.
-define(DEFAULT_BLOCK_POLLERS, 10).

%% The number of processes fetching the recent blocks and transactions on join.
-define(DEFAULT_JOIN_WORKERS, 10).

%% The number of disk pool jobs to run. Disk pool jobs scan the disk pool to index
%% no longer pending or orphaned chunks, pack chunks with a sufficient number of confirmations,
%% or remove the abandoned ones.
-define(DEFAULT_DISK_POOL_JOBS, 20).

%% The number of header sync jobs to run. Each job picks the latest not synced
%% block header and downloads it from peers.
-define(DEFAULT_HEADER_SYNC_JOBS, 1).

%% The default expiration time for a data root in the disk pool.
-define(DEFAULT_DISK_POOL_DATA_ROOT_EXPIRATION_TIME_S, 30 * 60).

%% The default size limit for unconfirmed and seeded chunks, per data root.
-ifdef(AR_TEST).
-define(DEFAULT_MAX_DISK_POOL_DATA_ROOT_BUFFER_MB, 50).
-else.
-define(DEFAULT_MAX_DISK_POOL_DATA_ROOT_BUFFER_MB, 10000).
-endif.

%% The default number of duplicate data roots checked for a posted chunk.
-define(DEFAULT_MAX_DUPLICATE_DATA_ROOTS, 20).

%% The default total size limit for unconfirmed and seeded chunks.
-ifdef(AR_TEST).
-define(DEFAULT_MAX_DISK_POOL_BUFFER_MB, 100).
-else.
-define(DEFAULT_MAX_DISK_POOL_BUFFER_MB, 100000).
-endif.

%% The default frequency of checking for the available disk space.
-ifdef(AR_TEST).
-define(DISK_SPACE_CHECK_FREQUENCY_MS, 1000).
-else.
-define(DISK_SPACE_CHECK_FREQUENCY_MS, 30 * 1000).
-endif.

-define(NUM_HASHING_PROCESSES,
    max(1, (erlang:system_info(schedulers_online) - 1))).

-define(MAX_PARALLEL_BLOCK_INDEX_REQUESTS, 1).
-define(MAX_PARALLEL_GET_CHUNK_REQUESTS, 100).
-define(MAX_PARALLEL_GET_AND_PACK_CHUNK_REQUESTS, 1).
-define(MAX_PARALLEL_GET_TX_DATA_REQUESTS, 1).
-define(MAX_PARALLEL_WALLET_LIST_REQUESTS, 1).
-define(MAX_PARALLEL_POST_CHUNK_REQUESTS, 100).
-define(MAX_PARALLEL_GET_SYNC_RECORD_REQUESTS, 10).
-define(MAX_PARALLEL_REWARD_HISTORY_REQUESTS, 1).
-define(MAX_PARALLEL_GET_TX_REQUESTS, 20).
-define(MAX_PARALLEL_GET_DATA_ROOTS_REQUESTS, 1).

%% The number of parallel tx validation processes.
-define(MAX_PARALLEL_POST_TX_REQUESTS, 20).
%% The time in seconds to wait for the available tx validation process before dropping the
%% POST /tx request.
-define(DEFAULT_POST_TX_TIMEOUT, 20).

%% The default value for the maximum number of threads used for nonce limiter chain
%% validation.
-define(DEFAULT_MAX_NONCE_LIMITER_VALIDATION_THREAD_COUNT,
        max(1, (erlang:system_info(schedulers_online) div 2))).

%% The default value for the maximum number of threads used for nonce limiter chain
%% last step validation.
-define(DEFAULT_MAX_NONCE_LIMITER_LAST_STEP_VALIDATION_THREAD_COUNT,
        max(1, (erlang:system_info(schedulers_online) - 1))).

%% Accept a block from the given IP only once in so many milliseconds.
-ifdef(AR_TEST).
-define(DEFAULT_BLOCK_THROTTLE_BY_IP_INTERVAL_MS, 10).
-else.
-define(DEFAULT_BLOCK_THROTTLE_BY_IP_INTERVAL_MS, 1000).
-endif.

%% Accept a block with the given solution hash only once in so many milliseconds.
-ifdef(AR_TEST).
-define(DEFAULT_BLOCK_THROTTLE_BY_SOLUTION_INTERVAL_MS, 10).
-else.
-define(DEFAULT_BLOCK_THROTTLE_BY_SOLUTION_INTERVAL_MS, 2000).
-endif.

-define(DEFAULT_CM_POLL_INTERVAL_MS, 60000).
-define(DEFAULT_CM_BATCH_TIMEOUT_MS, 20).

-define(CHUNK_GROUP_SIZE, (256 * 1024 * 8000)). % 2 GiB.

%% The number of consecutive chunks to read at a time during in-place repacking.
-ifdef(AR_TEST).
-define(DEFAULT_REPACK_BATCH_SIZE, 2).
-else.
-define(DEFAULT_REPACK_BATCH_SIZE, 100).
-endif.

-define(DEFAULT_REPACK_CACHE_SIZE_MB, 4000).

%% default filtering value for the peer list (30days)
-define(CURRENT_PEERS_LIST_FILTER, 30*60*60*24).

%% The default rocksdb databases flush interval, 30 minutes.
-define(DEFAULT_ROCKSDB_FLUSH_INTERVAL_S, 1800).
%% The default rocksdb WAL sync interval, 1 minute.
-define(DEFAULT_ROCKSDB_WAL_SYNC_INTERVAL_S, 60).

%% The number of 2.9 storage modules allowed to prepare the storage at a time.
-ifdef(AR_TEST).
-define(DEFAULT_REPLICA_2_9_WORKERS, 2).
-else.
-define(DEFAULT_REPLICA_2_9_WORKERS, 8).
-endif.

%% The default maximum number of replica 2.9 entropies to cache at a time
%% while syncing data. Each entropy is 256 MiB.
-define(DEFAULT_REPLICA_2_9_ENTROPY_CACHE_SIZE_MB, 4000).

%% The number of packing workers.
-define(DEFAULT_PACKING_WORKERS, erlang:system_info(dirty_cpu_schedulers_online)).

%% The default connection tcp delay when arweave is shutting down
-define(SHUTDOWN_TCP_CONNECTION_TIMEOUT, 30).
-define(SHUTDOWN_TCP_MODE, shutdown).

%% Global socket configuration
-define(DEFAULT_SOCKET_BACKEND, inet).

%% Default Gun HTTP/TCP options
-define(DEFAULT_GUN_HTTP_CLOSING_TIMEOUT, 15_000).
-define(DEFAULT_GUN_HTTP_KEEPALIVE, 60_000).
%% Fixed ceiling on parallel HTTP client connections per peer. The pool (see ar_http)
%% grows toward this as requests arrive and shrinks idle peers back to one, so
%% low-volume peers cost nothing. It is a fixed cap, not a controller: a live sweep
%% showed throughput flat from ~4 connections up (the workload is store/peer-bound,
%% not connection-bound) and higher counts only add load on the peer, so keep it small.
-define(DEFAULT_HTTP_CONNECTIONS_PER_PEER, 8).
-define(DEFAULT_GUN_TCP_DELAY_SEND, false).
-define(DEFAULT_GUN_TCP_KEEPALIVE, true).
-define(DEFAULT_GUN_TCP_LINGER, false).
-define(DEFAULT_GUN_TCP_LINGER_TIMEOUT, 0).
-define(DEFAULT_GUN_TCP_NODELAY, true).
-define(DEFAULT_GUN_TCP_SEND_TIMEOUT_CLOSE, true).
-define(DEFAULT_GUN_TCP_SEND_TIMEOUT, 15_000).

%% The time the cowboy loop handler waits before killing a request handler process.
-define(DEFAULT_HTTP_HANDLER_TIMEOUT_MS, 55000).

%% The default time an HTTP request waits for an endpoint semaphore.
-define(DEFAULT_HTTP_SEMAPHORE_TIMEOUT_MS, 30000).

%% Per-chunk HTTP body read period passed to cowboy_req:read_body/2.
-define(DEFAULT_HTTP_READ_BODY_PERIOD_MS, 15000).

%% Total wall-clock limit for reading the complete request body.
-define(DEFAULT_HTTP_MAX_BODY_READ_TIME_MS,
    ?DEFAULT_HTTP_HANDLER_TIMEOUT_MS - 2000).

%% Default Cowboy HTTP/TCP options
-define(DEFAULT_COWBOY_HTTP_ACTIVE_N, 100).
-define(DEFAULT_COWBOY_HTTP_IDLE_TIMEOUT, 60_000).
-define(DEFAULT_COWBOY_HTTP_INACTIVITY_TIMEOUT, 300_000).
-define(DEFAULT_COWBOY_HTTP_LINGER_TIMEOUT, 1000).
-define(DEFAULT_COWBOY_HTTP_REQUEST_TIMEOUT, 5000).
-define(DEFAULT_COWBOY_TCP_BACKLOG, 1024).
-define(DEFAULT_COWBOY_TCP_DELAY_SEND, false).
-define(DEFAULT_COWBOY_TCP_IDLE_TIMEOUT_SECOND, 10).
-define(DEFAULT_COWBOY_TCP_KEEPALIVE, true).
-define(DEFAULT_COWBOY_TCP_LINGER, false).
-define(DEFAULT_COWBOY_TCP_LINGER_TIMEOUT, 0).
-define(DEFAULT_COWBOY_TCP_MAX_CONNECTIONS, 5000).
-define(DEFAULT_COWBOY_TCP_NODELAY, true).
-define(DEFAULT_COWBOY_TCP_NUM_ACCEPTORS, 500).
-define(DEFAULT_COWBOY_TCP_SEND_TIMEOUT_CLOSE, true).
-define(DEFAULT_COWBOY_TCP_SEND_TIMEOUT, 15_000).
-define(DEFAULT_COWBOY_TCP_LISTENER_SHUTDOWN, 5000).

%% Solution rebasing is enabled in production but disabled under test. The
%% test build forces it off because the rebase code path makes timing-
%% sensitive tests flaky; production needs it for correct fork choice.
%% NB: no test currently exercises the rebase code path with this set to
%% true — see ar_node_worker:apply_block/1 and maybe_rebase/1.
-ifdef(AR_TEST).
-define(ALLOW_REBASE, false).
-else.
-define(ALLOW_REBASE, true).
-endif.

-endif.

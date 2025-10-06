-ifndef(AR_CONFIG_HRL).
-define(AR_CONFIG_HRL, true).

-include_lib("ar.hrl").
-include_lib("ar_p3.hrl").
-include_lib("ar_verify_chunks.hrl").

-record(config_webhook, {
	events = [],
	url = undefined,
	headers = []
}).

%% The polling frequency in seconds.
-define(DEFAULT_POLLING_INTERVAL, 2).

%% The number of processes periodically searching for the latest blocks.
-define(DEFAULT_BLOCK_POLLERS, 10).

%% The number of processes fetching the recent blocks and transactions on join.
-define(DEFAULT_JOIN_WORKERS, 10).

%% The number of data sync jobs to run. Each job periodically picks a range
%% and downloads it from peers.
-ifdef(AR_TEST).
-define(DEFAULT_SYNC_JOBS, 10).
-else.
-define(DEFAULT_SYNC_JOBS, 100).
-endif.

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

%% The number of packing workers.
-define(DEFAULT_PACKING_WORKERS, erlang:system_info(dirty_cpu_schedulers_online)).

%% The default connection tcp delay when arweave is shutting down
-define(SHUTDOWN_TCP_CONNECTION_TIMEOUT, 30).
-define(SHUTDOWN_TCP_MODE, shutdown).

%% Global socket configuration
-define(DEFAULT_SOCKET_BACKEND, inet).

%% Default Gun HTTP/TCP parameters
-define(DEFAULT_GUN_HTTP_CLOSING_TIMEOUT, 15_000).
-define(DEFAULT_GUN_HTTP_KEEPALIVE, 60_000).
-define(DEFAULT_GUN_TCP_DELAY_SEND, false).
-define(DEFAULT_GUN_TCP_KEEPALIVE, true).
-define(DEFAULT_GUN_TCP_LINGER, false).
-define(DEFAULT_GUN_TCP_LINGER_TIMEOUT, 0).
-define(DEFAULT_GUN_TCP_NODELAY, true).
-define(DEFAULT_GUN_TCP_SEND_TIMEOUT_CLOSE, true).
-define(DEFAULT_GUN_TCP_SEND_TIMEOUT, 15_000).

%% Default Cowboy HTTP/TCP parameters
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
-define(DEFAULT_COWBOY_TCP_MAX_CONNECTIONS, 1024).
-define(DEFAULT_COWBOY_TCP_NODELAY, true).
-define(DEFAULT_COWBOY_TCP_NUM_ACCEPTORS, 10).
-define(DEFAULT_COWBOY_TCP_SEND_TIMEOUT_CLOSE, true).
-define(DEFAULT_COWBOY_TCP_SEND_TIMEOUT, 15_000).
-define(DEFAULT_COWBOY_TCP_LISTENER_SHUTDOWN, 5000).

%% @doc Startup options with default values.
-record(config, {
	init = false,
	port = ?DEFAULT_HTTP_IFACE_PORT,
	mine = false,
	verify = false,
	verify_samples = ?SAMPLE_CHUNK_COUNT,
	peers = [],
	block_gossip_peers = [],
	local_peers = [],
	sync_from_local_peers_only = false,
	data_dir = ".",
	log_dir = ?LOG_DIR,
	polling = ?DEFAULT_POLLING_INTERVAL, % Polling frequency in seconds.
	block_pollers = ?DEFAULT_BLOCK_POLLERS,
	auto_join = true,
	join_workers = ?DEFAULT_JOIN_WORKERS,
	diff = ?DEFAULT_DIFF,
	mining_addr = not_set,
	hashing_threads = ?NUM_HASHING_PROCESSES,
	mining_cache_size_mb,
	packing_cache_size_limit,
	data_cache_size_limit,
	post_tx_timeout = ?DEFAULT_POST_TX_TIMEOUT,
	max_emitters = ?NUM_EMITTER_PROCESSES,
	sync_jobs = ?DEFAULT_SYNC_JOBS,
	header_sync_jobs = ?DEFAULT_HEADER_SYNC_JOBS,
	data_sync_request_packed_chunks = false,
	disk_pool_jobs = ?DEFAULT_DISK_POOL_JOBS,
	load_key = not_set,
	disk_space_check_frequency = ?DISK_SPACE_CHECK_FREQUENCY_MS,
	storage_modules = [],
	repack_in_place_storage_modules = [],
	repack_batch_size = ?DEFAULT_REPACK_BATCH_SIZE,
	repack_cache_size_mb = ?DEFAULT_REPACK_CACHE_SIZE_MB,
	start_from_latest_state = false,
	start_from_block,
	internal_api_secret = not_set,
	enable = [],
	disable = [],
	transaction_blacklist_files = [],
	transaction_blacklist_urls = [],
	transaction_whitelist_files = [],
	transaction_whitelist_urls = [],
	requests_per_minute_limit = ?DEFAULT_REQUESTS_PER_MINUTE_LIMIT,
	requests_per_minute_limit_by_ip = #{},
	max_propagation_peers = ?DEFAULT_MAX_PROPAGATION_PEERS,
	max_block_propagation_peers = ?DEFAULT_MAX_BLOCK_PROPAGATION_PEERS,
	webhooks = [],
	disk_pool_data_root_expiration_time = ?DEFAULT_DISK_POOL_DATA_ROOT_EXPIRATION_TIME_S,
	max_disk_pool_buffer_mb = ?DEFAULT_MAX_DISK_POOL_BUFFER_MB,
	max_disk_pool_data_root_buffer_mb = ?DEFAULT_MAX_DISK_POOL_DATA_ROOT_BUFFER_MB,
	semaphores = #{
		get_chunk => ?MAX_PARALLEL_GET_CHUNK_REQUESTS,
		get_and_pack_chunk => ?MAX_PARALLEL_GET_AND_PACK_CHUNK_REQUESTS,
		get_tx_data => ?MAX_PARALLEL_GET_TX_DATA_REQUESTS,
		post_chunk => ?MAX_PARALLEL_POST_CHUNK_REQUESTS,
		get_block_index => ?MAX_PARALLEL_BLOCK_INDEX_REQUESTS,
		get_wallet_list => ?MAX_PARALLEL_WALLET_LIST_REQUESTS,
		get_sync_record => ?MAX_PARALLEL_GET_SYNC_RECORD_REQUESTS,
		post_tx => ?MAX_PARALLEL_POST_TX_REQUESTS,
		get_reward_history => ?MAX_PARALLEL_REWARD_HISTORY_REQUESTS,
		get_tx => ?MAX_PARALLEL_GET_TX_REQUESTS
	},
	disk_cache_size = ?DISK_CACHE_SIZE,
	max_nonce_limiter_validation_thread_count
			= ?DEFAULT_MAX_NONCE_LIMITER_VALIDATION_THREAD_COUNT,
	max_nonce_limiter_last_step_validation_thread_count
			= ?DEFAULT_MAX_NONCE_LIMITER_LAST_STEP_VALIDATION_THREAD_COUNT,
	nonce_limiter_server_trusted_peers = [],
	nonce_limiter_client_peers = [],
	debug = false,
	run_defragmentation = false,
	defragmentation_trigger_threshold = 1_500_000_000,
	defragmentation_modules = [],
	block_throttle_by_ip_interval = ?DEFAULT_BLOCK_THROTTLE_BY_IP_INTERVAL_MS,
	block_throttle_by_solution_interval = ?DEFAULT_BLOCK_THROTTLE_BY_SOLUTION_INTERVAL_MS,
	tls_cert_file = not_set, %% required to enable TLS
	tls_key_file = not_set,  %% required to enable TLS
	http_api_transport_idle_timeout = ?DEFAULT_COWBOY_TCP_IDLE_TIMEOUT_SECOND*1000,
	p3 = #p3_config{},
	coordinated_mining = false,
	cm_api_secret = not_set,
	cm_exit_peer = not_set,
	cm_peers = [],
	cm_poll_interval = ?DEFAULT_CM_POLL_INTERVAL_MS,
	cm_out_batch_timeout = ?DEFAULT_CM_BATCH_TIMEOUT_MS,
	is_pool_server = false,
	is_pool_client = false,
	pool_server_address = not_set,
	pool_api_key = not_set,
	pool_worker_name = not_set,
	packing_workers = ?DEFAULT_PACKING_WORKERS,
	replica_2_9_workers = ?DEFAULT_REPLICA_2_9_WORKERS,
	disable_replica_2_9_device_limit = false,
	%% Undocumented/unsupported options
	chunk_storage_file_size = ?CHUNK_GROUP_SIZE,
	rocksdb_flush_interval_s = ?DEFAULT_ROCKSDB_FLUSH_INTERVAL_S,
	rocksdb_wal_sync_interval_s = ?DEFAULT_ROCKSDB_WAL_SYNC_INTERVAL_S,
	%% openssl (will be removed), fused, hiopt_m4
	vdf = openssl,
	%% Turn on/off the rebasing check. Only disabled in tests.
	allow_rebase = true,

	% Shutdown procedures
	shutdown_tcp_connection_timeout = ?SHUTDOWN_TCP_CONNECTION_TIMEOUT,
	shutdown_tcp_mode = ?SHUTDOWN_TCP_MODE,

	% global socket configuration
	'socket.backend' = ?DEFAULT_SOCKET_BACKEND,

	% gun network stack configuration.
	% these parameters are mainly configured using default
	% values from inet module
	'http_client.http.closing_timeout' = ?DEFAULT_GUN_HTTP_CLOSING_TIMEOUT,
	'http_client.http.keepalive' = ?DEFAULT_GUN_HTTP_KEEPALIVE,
	'http_client.tcp.delay_send' = ?DEFAULT_GUN_TCP_DELAY_SEND,
	'http_client.tcp.keepalive' = ?DEFAULT_GUN_TCP_KEEPALIVE,
	'http_client.tcp.linger' = ?DEFAULT_GUN_TCP_LINGER,
	'http_client.tcp.linger_timeout' = ?DEFAULT_GUN_TCP_LINGER_TIMEOUT,
	'http_client.tcp.nodelay' = ?DEFAULT_GUN_TCP_NODELAY,
	'http_client.tcp.send_timeout_close' = ?DEFAULT_GUN_TCP_SEND_TIMEOUT_CLOSE,
	'http_client.tcp.send_timeout' = ?DEFAULT_GUN_TCP_SEND_TIMEOUT,

	% cowboy network stack configuration.
	% these parameters are mainly configured using default
	% values from inet module
	'http_api.http.active_n' = ?DEFAULT_COWBOY_HTTP_ACTIVE_N,
	'http_api.http.inactivity_timeout' = ?DEFAULT_COWBOY_HTTP_INACTIVITY_TIMEOUT,
	'http_api.http.linger_timeout' = ?DEFAULT_COWBOY_HTTP_LINGER_TIMEOUT,
	'http_api.http.request_timeout' = ?DEFAULT_COWBOY_HTTP_REQUEST_TIMEOUT,
	'http_api.tcp.backlog' = ?DEFAULT_COWBOY_TCP_BACKLOG,
	'http_api.tcp.delay_send' = ?DEFAULT_COWBOY_TCP_DELAY_SEND,
	'http_api.tcp.keepalive' = ?DEFAULT_COWBOY_TCP_KEEPALIVE,
	'http_api.tcp.linger' = ?DEFAULT_COWBOY_TCP_LINGER,
	'http_api.tcp.linger_timeout' = ?DEFAULT_COWBOY_TCP_LINGER_TIMEOUT,
	'http_api.tcp.listener_shutdown' = ?DEFAULT_COWBOY_TCP_LISTENER_SHUTDOWN,
	'http_api.tcp.max_connections' = ?DEFAULT_COWBOY_TCP_MAX_CONNECTIONS,
	'http_api.tcp.nodelay' = ?DEFAULT_COWBOY_TCP_NODELAY,
	'http_api.tcp.num_acceptors' = ?DEFAULT_COWBOY_TCP_NUM_ACCEPTORS,
	'http_api.tcp.send_timeout_close' = ?DEFAULT_COWBOY_TCP_SEND_TIMEOUT_CLOSE,
	'http_api.tcp.send_timeout' = ?DEFAULT_COWBOY_TCP_SEND_TIMEOUT
}).

-endif.

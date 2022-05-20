-ifndef(AR_CONFIG_HRL).
-define(AR_CONFIG_HRL, true).

-include_lib("ar.hrl").

-record(config_webhook, {
	events = [],
	url = undefined,
	headers = []
}).

%% The polling frequency in seconds.
-ifdef(DEBUG).
-define(DEFAULT_POLLING_INTERVAL, 5).
-else.
-define(DEFAULT_POLLING_INTERVAL, 60).
-endif.

%% The number of data sync jobs to run. Each job periodically picks a range
%% and downloads it from peers.
-ifdef(DEBUG).
-define(DEFAULT_SYNC_JOBS, 50).
-else.
-define(DEFAULT_SYNC_JOBS, 100).
-endif.

%% The number of disk pool jobs to run. Disk pool jobs scan the disk pool to index
%% no longer pending or orphaned chunks, pack chunks with a sufficient number of confirmations,
%% or remove the abandoned ones.
-ifdef(DEBUG).
-define(DEFAULT_DISK_POOL_JOBS, 50).
-else.
-define(DEFAULT_DISK_POOL_JOBS, 100).
-endif.

%% The number of header sync jobs to run. Each job picks the latest not synced
%% block header and downloads it from peers.
-define(DEFAULT_HEADER_SYNC_JOBS, 1).

%% The default expiration time for a data root in the disk pool.
-define(DEFAULT_DISK_POOL_DATA_ROOT_EXPIRATION_TIME_S, 2 * 60 * 60).

%% The default size limit for unconfirmed chunks, per data root.
-define(DEFAULT_MAX_DISK_POOL_DATA_ROOT_BUFFER_MB, 50).

%% The default total size limit for unconfirmed chunks.
-ifdef(DEBUG).
-define(DEFAULT_MAX_DISK_POOL_BUFFER_MB, 100).
-else.
-define(DEFAULT_MAX_DISK_POOL_BUFFER_MB, 2000).
-endif.

%% The default frequency of checking for the available disk space.
-define(DISK_SPACE_CHECK_FREQUENCY_MS, 30 * 1000).

-define(NUM_STAGE_ONE_HASHING_PROCESSES,
	max(1, (erlang:system_info(schedulers_online) div 2))).

-define(NUM_STAGE_TWO_HASHING_PROCESSES,
	max(1, (3 * erlang:system_info(schedulers_online) div 4))).

-define(NUM_IO_MINING_THREADS, 10).

-define(MAX_PARALLEL_BLOCK_INDEX_REQUESTS, 1).
-define(MAX_PARALLEL_ARQL_REQUESTS, 10).
-define(MAX_PARALLEL_GATEWAY_ARQL_REQUESTS, infinity).
-define(MAX_PARALLEL_GET_CHUNK_REQUESTS, 100).
-define(MAX_PARALLEL_GET_AND_PACK_CHUNK_REQUESTS, 1).
-define(MAX_PARALLEL_GET_TX_DATA_REQUESTS, 1).
-define(MAX_PARALLEL_WALLET_LIST_REQUESTS, 1).
-define(MAX_PARALLEL_POST_CHUNK_REQUESTS, 100).
-define(MAX_PARALLEL_GET_SYNC_RECORD_REQUESTS, 10).

%% The number of parallel tx validation processes.
-define(MAX_PARALLEL_POST_TX_REQUESTS, 20).
%% The time in seconds to wait for the available tx validation process before dropping the
%% POST /tx request.
-define(DEFAULT_POST_TX_TIMEOUT, 20).

%% The maximum number of chunks per second the node attempts to pack or unpack.
-ifdef(DEBUG).
-define(DEFAULT_PACKING_RATE, 50).
-else.
-define(DEFAULT_PACKING_RATE, 15).
-endif.

%% @doc Startup options with default values.
-record(config, {
	init = false,
	port = ?DEFAULT_HTTP_IFACE_PORT,
	mine = false,
	peers = [],
	block_gossip_peers = [],
	data_dir = ".",
	metrics_dir = ?METRICS_DIR,
	polling = ?DEFAULT_POLLING_INTERVAL, % Polling frequency in seconds.
	auto_join = true,
	diff = ?DEFAULT_DIFF,
	mining_addr = not_set,
	max_miners = 0, % DEPRECATED.
	io_threads = ?NUM_IO_MINING_THREADS,
	stage_one_hashing_threads = ?NUM_STAGE_ONE_HASHING_PROCESSES,
	stage_two_hashing_threads = ?NUM_STAGE_TWO_HASHING_PROCESSES,
	tx_validators,
	post_tx_timeout = ?DEFAULT_POST_TX_TIMEOUT,
	max_emitters = ?NUM_EMITTER_PROCESSES,
	tx_propagation_parallelization, % DEPRECATED.
	sync_jobs = ?DEFAULT_SYNC_JOBS,
	header_sync_jobs = ?DEFAULT_HEADER_SYNC_JOBS,
	disk_pool_jobs = ?DEFAULT_DISK_POOL_JOBS,
	load_key = not_set,
	disk_space,
	disk_space_check_frequency = ?DISK_SPACE_CHECK_FREQUENCY_MS,
	start_from_block_index = false,
	internal_api_secret = not_set,
	enable = [],
	disable = [],
	transaction_blacklist_files = [],
	transaction_blacklist_urls = [],
	transaction_whitelist_files = [],
	transaction_whitelist_urls = [],
	gateway_domain = not_set,
	gateway_custom_domains = [],
	requests_per_minute_limit = ?DEFAULT_REQUESTS_PER_MINUTE_LIMIT,
	requests_per_minute_limit_by_ip = #{},
	max_propagation_peers = ?DEFAULT_MAX_PROPAGATION_PEERS,
	max_block_propagation_peers = ?DEFAULT_MAX_BLOCK_PROPAGATION_PEERS,
	ipfs_pin = false,
	webhooks = [],
	max_connections = 1024,
	max_gateway_connections = 128,
	max_poa_option_depth = 500,
	disk_pool_data_root_expiration_time = ?DEFAULT_DISK_POOL_DATA_ROOT_EXPIRATION_TIME_S,
	max_disk_pool_buffer_mb = ?DEFAULT_MAX_DISK_POOL_BUFFER_MB,
	max_disk_pool_data_root_buffer_mb = ?DEFAULT_MAX_DISK_POOL_DATA_ROOT_BUFFER_MB,
	randomx_bulk_hashing_iterations = 8,
	semaphores = #{
		get_chunk => ?MAX_PARALLEL_GET_CHUNK_REQUESTS,
		get_and_pack_chunk => ?MAX_PARALLEL_GET_AND_PACK_CHUNK_REQUESTS,
		get_tx_data => ?MAX_PARALLEL_GET_TX_DATA_REQUESTS,
		post_chunk => ?MAX_PARALLEL_POST_CHUNK_REQUESTS,
		get_block_index => ?MAX_PARALLEL_BLOCK_INDEX_REQUESTS,
		get_wallet_list => ?MAX_PARALLEL_WALLET_LIST_REQUESTS,
		arql => ?MAX_PARALLEL_ARQL_REQUESTS,
		gateway_arql => ?MAX_PARALLEL_GATEWAY_ARQL_REQUESTS,
		get_sync_record => ?MAX_PARALLEL_GET_SYNC_RECORD_REQUESTS,
		post_tx => ?MAX_PARALLEL_POST_TX_REQUESTS
	},
	disk_cache_size = ?DISK_CACHE_SIZE,
	packing_rate = ?DEFAULT_PACKING_RATE,
	debug = false
}).

-endif.

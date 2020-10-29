-ifndef(AR_CONFIG_HRL).
-define(AR_CONFIG_HRL, true).

-include_lib("ar.hrl").

-record(config_webhook, {
	events = [],
	url = undefined,
	headers = []
}).

%% Start options with default values.
-record(config, {
	init = false,
	port = ?DEFAULT_HTTP_IFACE_PORT,
	mine = false,
	peers = [],
	data_dir = ".",
	metrics_dir = ?METRICS_DIR,
	polling = 0, %% period of time in seconds
	auto_join = true,
	clean = false,
	diff = ?DEFAULT_DIFF,
	mining_addr = false,
	max_miners = ?NUM_MINING_PROCESSES,
	max_emitters = ?NUM_EMITTER_PROCESSES,
	tx_propagation_parallelization = ?TX_PROPAGATION_PARALLELIZATION,
	new_key = false,
	load_key = false,
	disk_space,
	used_space = 0,
	start_from_block_index = false,
	internal_api_secret = not_set,
	enable = [],
	disable = [],
	content_policy_files = [],
	transaction_blacklist_files = [],
	gateway_domain = not_set,
	gateway_custom_domains = [],
	requests_per_minute_limit = ?DEFAULT_REQUESTS_PER_MINUTE_LIMIT,
	max_propagation_peers = ?DEFAULT_MAX_PROPAGATION_PEERS,
	ipfs_pin = false,
	webhooks = [],
	max_connections = 1024,
	max_gateway_connections = 128,
	max_poa_option_depth = 8,
	disk_pool_data_root_expiration_time = ?DISK_POOL_DATA_ROOT_EXPIRATION_TIME_S,
	max_disk_pool_buffer_mb = ?MAX_DISK_POOL_BUFFER_MB,
	max_disk_pool_data_root_buffer_mb = ?MAX_DISK_POOL_DATA_ROOT_BUFFER_MB,
	randomx_bulk_hashing_iterations = 12
}).

-endif.

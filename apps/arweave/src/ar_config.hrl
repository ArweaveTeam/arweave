-ifndef(AR_CONFIG_HRL).
-define(AR_CONFIG_HRL, true).

-include("ar.hrl").

-record(config_webhook, {
	events = [],
	url = undefined,
	headers = []
}).

%% Start options with default values.
-record(config, {
	benchmark = false,
	benchmark_algorithm = not_set,
	port = ?DEFAULT_HTTP_IFACE_PORT,
	init = false,
	mine = false,
	peers = [],
	data_dir = ".",
	polling = false,
	auto_join = true,
	clean = false,
	diff = ?DEFAULT_DIFF,
	mining_addr = false,
	max_miners = ?NUM_MINING_PROCESSES,
	max_emitters = ?NUM_EMITTER_PROCESSES,
	tx_propagation_parallelization = ?TX_PROPAGATION_PARALLELIZATION,
	new_key = false,
	load_key = false,
	pause = true,
	disk_space,
	used_space = 0,
	start_block_index = undefined,
	internal_api_secret = not_set,
	enable = [],
	disable = [],
	content_policy_files = [],
	transaction_blacklist_files = [],
	gateway_domain = not_set,
	gateway_custom_domains = [],
	requests_per_minute_limit = ?DEFAULT_REQUESTS_PER_MINUTE_LIMIT,
	max_propagation_peers = ?DEFAULT_MAX_PROPAGATION_PEERS,
	webhooks = [],
	max_connections = 1024,
	max_gateway_connections = 128,
	max_option_depth = 8
}).

-endif.

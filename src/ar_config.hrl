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
	new_key = false,
	load_key = false,
	pause = true,
	disk_space,
	used_space = 0,
	start_hash_list = undefined,
	auto_update = ar_util:decode(?DEFAULT_UPDATE_ADDR),
	internal_api_secret = not_set,
	enable = [],
	disable = [],
	content_policy_files = [],
	transaction_blacklist_files = [],
	gateway_domain = not_set,
	gateway_custom_domains = [],
	requests_per_minute_limit = ?DEFAULT_REQUESTS_PER_MINUTE_LIMIT,
	ipfs_pin = false,
	ipfs_import = false,
	webhooks = []
}).

-endif.

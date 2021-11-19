-module(ar_config_tests).

-include_lib("arweave/include/ar_config.hrl").
-include_lib("eunit/include/eunit.hrl").

parse_test_() ->
	{timeout, 60, fun parse_config/0}.

parse_config() ->
	ExpectedMiningAddr = ar_util:decode(<<"LKC84RnISouGUw4uMQGCpPS9yDC-tIoqM2UVbUIt-Sw">>),
	{ok, ParsedConfig} = ar_config:parse(config_fixture()),
	?assertMatch(#config{
		init = true,
		port = 1985,
		mine = true,
		peers = [
			{188,166,200,45,1984},
			{188,166,192,169,1984},
			{163,47,11,64,1984},
			{159,203,158,108,1984},
			{159,203,49,13,1984},
			{139,59,51,59,1984},
			{138,197,232,192,1984},
			{46,101,67,172,1984}
		],
		data_dir = "some_data_dir",
		metrics_dir = "metrics_dir",
		polling = 10,
		auto_join = false,
		diff = 42,
		mining_addr = ExpectedMiningAddr,
		max_miners = 43,
		io_threads = 43,
		stage_one_hashing_threads = 27,
		stage_two_hashing_threads = 37,
		max_propagation_peers = 8,
		max_block_propagation_peers = 60,
		tx_validators = 3,
		max_emitters = 4,
		tx_propagation_parallelization = undefined,
		sync_jobs = 10,
		header_sync_jobs = 1,
		load_key = "some_key_file",
		disk_space = 44 * 1024 * 1024 * 1024,
		disk_space_check_frequency = 10 * 1000,
		start_from_block_index = true,
		internal_api_secret = <<"some_very_very_long_secret">>,
		enable = [feature_1, feature_2],
		disable = [feature_3, feature_4],
		transaction_blacklist_files = ["some_blacklist_1", "some_blacklist_2"],
		transaction_blacklist_urls = ["http://some_blacklist_1", "http://some_blacklist_2/x"],
		transaction_whitelist_files = ["some_whitelist_1", "some_whitelist_2"],
		transaction_whitelist_urls = ["http://some_whitelist"],
		gateway_domain = <<"gateway.localhost">>,
		gateway_custom_domains = [<<"domain1.example">>, <<"domain2.example">>],
		webhooks = [
			#config_webhook{
				events = [transaction, block],
				url = <<"https://example.com/hook">>,
				headers = [{<<"Authorization">>, <<"Bearer 123456">>}]
			}
		],
		max_connections = 512,
		max_gateway_connections = 64,
		disk_pool_data_root_expiration_time = 10000,
		max_disk_pool_buffer_mb = 100000,
		max_disk_pool_data_root_buffer_mb = 100000000,
		randomx_bulk_hashing_iterations = 40,
		disk_cache_size = 1024,
		semaphores = #{
			get_chunk := 1,
			get_and_pack_chunk := 2,
			get_tx_data := 3,
			post_chunk := 999,
			get_block_index := 1,
			get_wallet_list := 2,
			arql := 3,
			gateway_arql := 3,
			get_sync_record := 10
		},
		packing_rate = 20
	}, ParsedConfig).

config_fixture() ->
	Dir = filename:dirname(?FILE),
	Path = filename:join(Dir, "ar_config_tests_config_fixture.json"),
	{ok, FileData} = file:read_file(Path),
	FileData.

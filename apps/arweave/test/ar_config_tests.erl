-module(ar_config_tests).

-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("eunit/include/eunit.hrl").

parse_test_() ->
	{timeout, 60, fun parse_config/0}.

parse_config() ->
	ExpectedMiningAddr = ar_util:decode(<<"LKC84RnISouGUw4uMQGCpPS9yDC-tIoqM2UVbUIt-Sw">>),
	{ok, ParsedConfig} = ar_config:parse(config_fixture()),
	ExpectedBlockHash = ar_util:decode(
			<<"lfoR_PyKV6t7Z6Xi2QJZlZ0JWThh0Ke7Zc5Q82CSshUhFGcjiYufP234ph1mVofX">>),
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
		local_peers = [
			{192, 168, 2, 3, 1984},
			{172, 16, 10, 11, 1985}
		],
		block_gossip_peers = [{159,203,158,108,1984}, {150,150,150,150, 1983}],
		data_dir = "some_data_dir",
		log_dir = "log_dir",
		storage_modules = [{?PARTITION_SIZE, 0, unpacked},
				{?PARTITION_SIZE, 2, {spora_2_6, ExpectedMiningAddr}},
				{?PARTITION_SIZE, 100, unpacked},
				{1, 0, unpacked},
				{1000000000000, 14, {spora_2_6, ExpectedMiningAddr}}],
		polling = 10,
		block_pollers = 100,
		auto_join = false,
		join_workers = 9,
		diff = 42,
		mining_addr = ExpectedMiningAddr,
		max_miners = 43,
		hashing_threads = 17,
		data_cache_size_limit = 10000,
		packing_cache_size_limit = 20000,
		mining_server_chunk_cache_size_limit = 3,
		max_propagation_peers = 8,
		max_block_propagation_peers = 60,
		tx_validators = 3,
		post_tx_timeout = 50,
		max_emitters = 4,
		tx_propagation_parallelization = undefined,
		sync_jobs = 10,
		header_sync_jobs = 1,
		disk_pool_jobs = 2,
		requests_per_minute_limit = 2500,
		requests_per_minute_limit_by_ip = #{
			{127, 0, 0, 1} := #{
				chunk := 100000,
				data_sync_record := 1,
				recent_hash_list_diff := 200000,
				default := 100
			}
		},
		disk_space = 44 * 1024 * 1024 * 1024,
		disk_space_check_frequency = 10 * 1000,
		start_from_latest_state = true,
		start_from_block = ExpectedBlockHash,
		internal_api_secret = <<"some_very_very_long_secret">>,
		enable = [feature_1, feature_2],
		disable = [feature_3, feature_4],
		transaction_blacklist_files = ["some_blacklist_1", "some_blacklist_2"],
		transaction_blacklist_urls = ["http://some_blacklist_1", "http://some_blacklist_2/x"],
		transaction_whitelist_files = ["some_whitelist_1", "some_whitelist_2"],
		transaction_whitelist_urls = ["http://some_whitelist"],
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
		packing_rate = 20,
		max_nonce_limiter_validation_thread_count = 2,
		max_nonce_limiter_last_step_validation_thread_count = 3,
		nonce_limiter_server_trusted_peers = ["127.0.0.1", "2.3.4.5", "6.7.8.9:1982"],
		nonce_limiter_client_peers = [<<"2.3.6.7:1984">>, <<"4.7.3.1:1983">>, <<"3.3.3.3">>],
		run_defragmentation = true,
		defragmentation_trigger_threshold = 1_000,
		defragmentation_modules = [
			{?PARTITION_SIZE, 0, unpacked},
			{?PARTITION_SIZE, 2, {spora_2_6, ExpectedMiningAddr}},
			{?PARTITION_SIZE, 100, unpacked},
			{1, 0, unpacked},
			{1000000000000, 14, {spora_2_6, ExpectedMiningAddr}}
		],
		block_throttle_by_ip_interval = 5_000,
		block_throttle_by_solution_interval = 12_000
	}, ParsedConfig).

config_fixture() ->
	{ok, Cwd} = file:get_cwd(),
	Path = filename:join(Cwd, "./apps/arweave/test/ar_config_tests_config_fixture.json"),
	{ok, FileData} = file:read_file(Path),
	FileData.

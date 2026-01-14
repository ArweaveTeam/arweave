-module(ar_config_tests).

-include_lib("ar_consensus.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").
-include_lib("eunit/include/eunit.hrl").

parse_test_() ->
	{timeout, 60, fun test_parse_config/0}.

validate_test_() ->
	[
		{timeout, 60, fun test_validate_repack_in_place/0},
		{timeout, 60, fun test_validate_cm_pool/0},
		{timeout, 60, fun test_validate_storage_modules/0},
		{timeout, 60, fun test_validate_cm/0}
	].

test_parse_config() ->
	ExpectedMiningAddr = ar_util:decode(<<"LKC84RnISouGUw4uMQGCpPS9yDC-tIoqM2UVbUIt-Sw">>),
	{ok, ParsedConfig} = ar_config:parse(config_fixture()),
	ExpectedBlockHash = ar_util:decode(
			<<"lfoR_PyKV6t7Z6Xi2QJZlZ0JWThh0Ke7Zc5Q82CSshUhFGcjiYufP234ph1mVofX">>),
	PartitionSize = ar_block:partition_size(),
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
		sync_from_local_peers_only = true,
		block_gossip_peers = [{159,203,158,108,1984}, {150,150,150,150, 1983}],
		data_dir = "some_data_dir",
		log_dir = "log_dir",
		storage_modules = [{PartitionSize, 0, unpacked},
				{PartitionSize, 2, {spora_2_6, ExpectedMiningAddr}},
				{PartitionSize, 100, unpacked},
				{1, 0, unpacked},
				{1000000000000, 14, {spora_2_6, ExpectedMiningAddr}},
				{PartitionSize, 0, {replica_2_9, ExpectedMiningAddr}}],
		repack_in_place_storage_modules = [
				{{PartitionSize, 1, unpacked}, {spora_2_6, ExpectedMiningAddr}},
				{{1, 1, {spora_2_6, ExpectedMiningAddr}}, unpacked},
				{{PartitionSize,8, {replica_2_9, ExpectedMiningAddr}}, unpacked}],
		repack_batch_size = 200,
		polling = 10,
		block_pollers = 100,
		auto_join = false,
		join_workers = 9,
		diff = 42,
		mining_addr = ExpectedMiningAddr,
		hashing_threads = 17,
		data_cache_size_limit = 10000,
		packing_cache_size_limit = 20000,
		mining_cache_size_mb = 3,
		max_propagation_peers = 8,
		max_block_propagation_peers = 60,
		post_tx_timeout = 50,
		max_emitters = 4,
		replica_2_9_workers = 16,
		disable_replica_2_9_device_limit = true,
		replica_2_9_entropy_cache_size_mb = 2000,
		packing_workers = 25,
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
		'http_api.tcp.max_connections' = 512,
		disk_pool_data_root_expiration_time = 10000,
		max_disk_pool_buffer_mb = 100000,
		max_disk_pool_data_root_buffer_mb = 100000000,
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
		vdf = hiopt_m4,
		max_nonce_limiter_validation_thread_count = 2,
		max_nonce_limiter_last_step_validation_thread_count = 3,
		nonce_limiter_server_trusted_peers = ["127.0.0.1", "2.3.4.5", "6.7.8.9:1982"],
		nonce_limiter_client_peers = [<<"2.3.6.7:1984">>, <<"4.7.3.1:1983">>, <<"3.3.3.3">>],
		run_defragmentation = true,
		defragmentation_trigger_threshold = 1_000,
		defragmentation_modules = [
			{PartitionSize, 0, unpacked},
			{PartitionSize, 2, {spora_2_6, ExpectedMiningAddr}},
			{PartitionSize, 100, unpacked},
			{1, 0, unpacked},
			{1000000000000, 14, {spora_2_6, ExpectedMiningAddr}}
		],
		block_throttle_by_ip_interval = 5_000,
		block_throttle_by_solution_interval = 12_000,
		http_api_transport_idle_timeout = 15_000
	}, ParsedConfig).

config_fixture() ->
	{ok, Cwd} = file:get_cwd(),
	Path = filename:join(Cwd, "./apps/arweave/test/ar_config_tests_config_fixture.json"),
	{ok, FileData} = file:read_file(Path),
	FileData.

test_validate_repack_in_place() ->
	Addr1 = crypto:strong_rand_bytes(32),
	Addr2 = crypto:strong_rand_bytes(32),
	PartitionSize = ar_block:partition_size(),
	?assertEqual(true,
		ar_config:validate_config(#config{
			storage_modules = [],
			repack_in_place_storage_modules = []})),
	?assertEqual(true,
			ar_config:validate_config(#config{
				storage_modules = [{PartitionSize, 0, {spora_2_6, Addr1}}],
				repack_in_place_storage_modules = []})),
	?assertEqual(true,
		ar_config:validate_config(#config{
			storage_modules = [{PartitionSize, 0, {spora_2_6, Addr1}}],
			repack_in_place_storage_modules = [
				{{PartitionSize, 1, {spora_2_6, Addr1}}, {replica_2_9, Addr2}}]})),
	?assertEqual(false,
		ar_config:validate_config(#config{
			storage_modules = [{PartitionSize, 0, {spora_2_6, Addr1}}],
			repack_in_place_storage_modules = [
				{{PartitionSize, 0, {spora_2_6, Addr1}}, {replica_2_9, Addr2}}]})),
	?assertEqual(true,
		ar_config:validate_config(#config{
			storage_modules = [],
			repack_in_place_storage_modules = [
				{{PartitionSize, 0, {replica_2_9, Addr1}}, {replica_2_9, Addr2}}]})),
	?assertEqual(true,
		ar_config:validate_config(#config{
			storage_modules = [],
			repack_in_place_storage_modules = [
				{{PartitionSize, 0, {replica_2_9, Addr1}}, {spora_2_6, Addr2}}]})),
	?assertEqual(true,
		ar_config:validate_config(#config{
			storage_modules = [],
			repack_in_place_storage_modules = [
				{{PartitionSize, 0, {replica_2_9, Addr2}}, unpacked}]})),
	?assertEqual(true,
		ar_config:validate_config(#config{
			storage_modules = [],
			repack_in_place_storage_modules = [
				{{PartitionSize, 0, unpacked}, {replica_2_9, Addr2}}]})),
	?assertEqual(true,
		ar_config:validate_config(#config{
			storage_modules = [],
			repack_in_place_storage_modules = [
				{{PartitionSize, 0, {spora_2_6, Addr1}}, {replica_2_9, Addr2}}]})),
	?assertEqual(true,
		ar_config:validate_config(#config{
			storage_modules = [],
			repack_in_place_storage_modules = [
				{{PartitionSize, 0, unpacked}, {spora_2_6, Addr2}}]})).


test_validate_cm_pool() ->
	?assertEqual(false,
		ar_config:validate_config(
			#config{
				coordinated_mining = true, is_pool_server = true,
				mine = true, cm_api_secret = <<"secret">>})),
	?assertEqual(true,
		ar_config:validate_config(
			#config{
				coordinated_mining = true, is_pool_server = false,
				mine = true, cm_api_secret = <<"secret">>})),
	?assertEqual(true,
		ar_config:validate_config(
			#config{
				coordinated_mining = false, is_pool_server = true,
				mine = true, cm_api_secret = <<"secret">>})),
	?assertEqual(true,
		ar_config:validate_config(
			#config{
				coordinated_mining = false, is_pool_server = false,
				mine = true, cm_api_secret = <<"secret">>})),
	?assertEqual(false,
		ar_config:validate_config(
			#config{is_pool_server = true, is_pool_client = true, mine = true})),
	?assertEqual(true,
		ar_config:validate_config(
			#config{is_pool_server = true, is_pool_client = false})),
	?assertEqual(true,
		ar_config:validate_config(
			#config{is_pool_server = false, is_pool_client = true, mine = true})),
	?assertEqual(true,
		ar_config:validate_config(
			#config{is_pool_server = false, is_pool_client = false, mine = true})),
	?assertEqual(false,
		ar_config:validate_config(
			#config{is_pool_client = true, mine = false})),
	?assertEqual(true,
		ar_config:validate_config(
			#config{is_pool_client = true, mine = true})),
	?assertEqual(true,
		ar_config:validate_config(
			#config{is_pool_client = false, mine = true})),
	?assertEqual(true,
		ar_config:validate_config(
			#config{is_pool_client = false, mine = false})).

test_validate_cm() ->
	?assertEqual(true,
		ar_config:validate_config(
			#config{coordinated_mining = true, mine = true, cm_api_secret = <<"secret">>})),
	?assertEqual(true,
		ar_config:validate_config(
			#config{coordinated_mining = false, mine = false, cm_api_secret = not_set})),
	?assertEqual(false,
		ar_config:validate_config(
			#config{coordinated_mining = true, mine = false, cm_api_secret = <<"secret">>})),
	?assertEqual(false,
		ar_config:validate_config(
			#config{coordinated_mining = true, mine = true, cm_api_secret = not_set})).

		
test_validate_storage_modules() ->
	Addr1 = crypto:strong_rand_bytes(32),
	Addr2 = crypto:strong_rand_bytes(32),
	LegacyPacking = {spora_2_6, Addr1},
	PartitionSize = ar_block:partition_size(),

	Unpacked = {PartitionSize, 0, unpacked},
	Legacy = {PartitionSize, 1, LegacyPacking},
	Replica29 = {PartitionSize, 2, {replica_2_9, Addr1}},

	?assertEqual(true,
		ar_config:validate_config(
			#config{
				storage_modules = [Unpacked, Legacy, Replica29],
				mining_addr = Addr1,
				mine = false})),
	?assertEqual(true,
		ar_config:validate_config(
			#config{
				storage_modules = [Unpacked, Legacy, Replica29],
				mining_addr = Addr2,
				mine = true})),
	?assertEqual(true,
		ar_config:validate_config(
			#config{
				storage_modules = [Unpacked, Legacy],
				mining_addr = Addr1,
				mine = true})),
	?assertEqual(true,
		ar_config:validate_config(
			#config{
				storage_modules = [Unpacked, Replica29],
				mining_addr = Addr1,
				mine = true})),
	?assertEqual(false,
		ar_config:validate_config(
			#config{
				storage_modules = [Legacy, Replica29],
				mining_addr = Addr1,
				mine = true})).

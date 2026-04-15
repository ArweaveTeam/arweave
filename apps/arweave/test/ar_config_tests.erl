-module(ar_config_tests).

-include_lib("ar_consensus.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").
-include_lib("eunit/include/eunit.hrl").

-ifdef(AR_TEST).
-define(M, ar_config).
-endif.

parse_test_() ->
	[
		{timeout, 60, fun test_parse_config/0},
		{timeout, 60, fun test_parse_max_duplicate_data_roots_infinity/0}
	].

validate_test_() ->
	[
		{timeout, 60, fun test_validate_repack_in_place/0},
		{timeout, 60, fun test_validate_cm_pool/0},
		{timeout, 60, fun test_validate_storage_modules/0},
		{timeout, 60, fun test_validate_cm/0},
		{timeout, 60, fun test_peers/0}
	].

test_peers() ->
    	?assertMatch(
		{ok, []},
		?M:parse_peers([], [])),
    	?assertMatch(
		{ok, [{127,0,0,1,1984}]},
		?M:parse_peers([[{127,0,0,1,1984}]], [])),
    	?assertMatch(
		{ok, []},
		?M:parse_peers(["invalid.arweave.xyz"], [])),
	ok.

test_parse_config() ->
	ExpectedMiningAddr = ar_util:decode(<<"LKC84RnISouGUw4uMQGCpPS9yDC-tIoqM2UVbUIt-Sw">>),
	{ok, ParsedConfig} = ar_config:parse(config_fixture()),
	ExpectedBlockHash = ar_util:decode(
			<<"lfoR_PyKV6t7Z6Xi2QJZlZ0JWThh0Ke7Zc5Q82CSshUhFGcjiYufP234ph1mVofX">>),
	PartitionSize = ar_block:partition_size(), 

	?assertMatch(true, ParsedConfig#config.init),
	?assertMatch(1985, ParsedConfig#config.port),
	?assertMatch(true, ParsedConfig#config.mine),
	?assertMatch([
		{188,166,200,45,1984},
		{188,166,192,169,1984},
		{163,47,11,64,1984},
		{159,203,158,108,1984},
		{159,203,49,13,1984},
		{139,59,51,59,1984},
		{138,197,232,192,1984},
		{46,101,67,172,1984}
	], ParsedConfig#config.peers),

	?assertMatch([
		{192, 168, 2, 3, 1984},
		{172, 16, 10, 11, 1985}
	], ParsedConfig#config.local_peers),
	?assertMatch(true, ParsedConfig#config.sync_from_local_peers_only),
	?assertMatch([{159,203,158,108,1984}, {150,150,150,150, 1983}], ParsedConfig#config.block_gossip_peers),
	?assertMatch("some_data_dir", ParsedConfig#config.data_dir),
	?assertMatch("log_dir", ParsedConfig#config.log_dir),
	?assertMatch([
		{PartitionSize, 0, unpacked},
		{PartitionSize, 2, {spora_2_6, ExpectedMiningAddr}},
		{PartitionSize, 100, unpacked},
		{1, 0, unpacked},
		{1000000000000, 14, {spora_2_6, ExpectedMiningAddr}},
		{PartitionSize, 0, {replica_2_9, ExpectedMiningAddr}}
	], ParsedConfig#config.storage_modules),
	?assertMatch([
		{{PartitionSize, 1, unpacked}, {spora_2_6, ExpectedMiningAddr}},
		{{1, 1, {spora_2_6, ExpectedMiningAddr}}, unpacked},
		{{PartitionSize, 8, {replica_2_9, ExpectedMiningAddr}}, unpacked}
	], ParsedConfig#config.repack_in_place_storage_modules),

	?assertMatch(200, ParsedConfig#config.repack_batch_size),
	?assertMatch(10, ParsedConfig#config.polling),
	?assertMatch(100, ParsedConfig#config.block_pollers),
	?assertMatch(false, ParsedConfig#config.auto_join),
	?assertMatch(9, ParsedConfig#config.join_workers),
	?assertMatch(42, ParsedConfig#config.diff),
	?assertMatch(ExpectedMiningAddr, ParsedConfig#config.mining_addr),
	?assertMatch(17, ParsedConfig#config.hashing_threads),
	?assertMatch(10000, ParsedConfig#config.data_cache_size_limit),
	?assertMatch(20000, ParsedConfig#config.packing_cache_size_limit),
	?assertMatch(3, ParsedConfig#config.mining_cache_size_mb),
	?assertMatch(8, ParsedConfig#config.max_propagation_peers),
	?assertMatch(60, ParsedConfig#config.max_block_propagation_peers),
	?assertMatch(50, ParsedConfig#config.post_tx_timeout),
	?assertMatch(4, ParsedConfig#config.max_emitters),
	?assertMatch(16, ParsedConfig#config.replica_2_9_workers),
	?assertMatch(true, ParsedConfig#config.disable_replica_2_9_device_limit),
	?assertMatch(2000, ParsedConfig#config.replica_2_9_entropy_cache_size_mb),
	?assertMatch(25, ParsedConfig#config.packing_workers),
	?assertMatch(10, ParsedConfig#config.sync_jobs),
	?assertMatch(1, ParsedConfig#config.header_sync_jobs),
	?assertMatch(2, ParsedConfig#config.disk_pool_jobs),
	?assertMatch(2500, ParsedConfig#config.requests_per_minute_limit),
	?assertMatch(#{
		{127, 0, 0, 1} := #{
			chunk := 100000,
			data_sync_record := 1,
			recent_hash_list_diff := 200000,
			default := 100
		}
	}, ParsedConfig#config.requests_per_minute_limit_by_ip),
	?assertMatch(10 * 1000, ParsedConfig#config.disk_space_check_frequency),
	?assertMatch(true, ParsedConfig#config.start_from_latest_state),
	?assertMatch(ExpectedBlockHash, ParsedConfig#config.start_from_block),
	?assertMatch(<<"some_very_very_long_secret">>, ParsedConfig#config.internal_api_secret),
	?assertMatch([feature_1, feature_2], ParsedConfig#config.enable),
	?assertMatch([feature_3, feature_4], ParsedConfig#config.disable),
	?assertMatch(["some_blacklist_1", "some_blacklist_2"], ParsedConfig#config.transaction_blacklist_files),
	?assertMatch(["http://some_blacklist_1", "http://some_blacklist_2/x"], ParsedConfig#config.transaction_blacklist_urls),
	?assertMatch(["some_whitelist_1", "some_whitelist_2"], ParsedConfig#config.transaction_whitelist_files),
	?assertMatch(["http://some_whitelist"], ParsedConfig#config.transaction_whitelist_urls),
	?assertMatch([
		#config_webhook{
			events = [transaction, block],
			url = <<"https://example.com/hook">>,
			headers = [{<<"Authorization">>, <<"Bearer 123456">>}]
		}
	], ParsedConfig#config.webhooks),
	?assertMatch(512, ParsedConfig#config.'http_api.tcp.max_connections'),
	?assertMatch(10000, ParsedConfig#config.disk_pool_data_root_expiration_time),
	?assertMatch(100000, ParsedConfig#config.max_disk_pool_buffer_mb),
	?assertMatch(100000000, ParsedConfig#config.max_disk_pool_data_root_buffer_mb),
	?assertMatch(7, ParsedConfig#config.max_duplicate_data_roots),
	?assertMatch(1024, ParsedConfig#config.disk_cache_size),
	?assertMatch(#{
		get_chunk := 1,
		get_and_pack_chunk := 2,
		get_tx_data := 3,
		post_chunk := 999,
		get_block_index := 1,
		get_wallet_list := 2,
		arql := 3,
		gateway_arql := 3,
		get_sync_record := 10
	}, ParsedConfig#config.semaphores),

	?assertMatch(hiopt_m4, ParsedConfig#config.vdf),
	?assertMatch(2, ParsedConfig#config.max_nonce_limiter_validation_thread_count),
	?assertMatch(3, ParsedConfig#config.max_nonce_limiter_last_step_validation_thread_count),
	?assertMatch(["127.0.0.1", "2.3.4.5", "6.7.8.9:1982"], ParsedConfig#config.nonce_limiter_server_trusted_peers),
	?assertMatch([<<"2.3.6.7:1984">>, <<"4.7.3.1:1983">>, <<"3.3.3.3">>], ParsedConfig#config.nonce_limiter_client_peers),
	?assertMatch(true, ParsedConfig#config.run_defragmentation),
	?assertMatch(1_000, ParsedConfig#config.defragmentation_trigger_threshold),
	?assertMatch([
		{PartitionSize, 0, unpacked},
		{PartitionSize, 2, {spora_2_6, ExpectedMiningAddr}},
		{PartitionSize, 100, unpacked},
		{1, 0, unpacked},
		{1000000000000, 14, {spora_2_6, ExpectedMiningAddr}}
	], ParsedConfig#config.defragmentation_modules),
	?assertMatch(5_000, ParsedConfig#config.block_throttle_by_ip_interval),
	?assertMatch(12_000, ParsedConfig#config.block_throttle_by_solution_interval),
	?assertMatch(15_000, ParsedConfig#config.http_api_transport_idle_timeout),

	ok.

test_parse_max_duplicate_data_roots_infinity() ->
	{ok, ParsedConfig} = ar_config:parse(<<"{\"max_duplicate_data_roots\":\"infinity\"}">>),
	?assertEqual(infinity, ParsedConfig#config.max_duplicate_data_roots).

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

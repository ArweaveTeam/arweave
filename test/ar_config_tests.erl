-module(ar_config_tests).
-include("src/ar_config.hrl").
-include_lib("eunit/include/eunit.hrl").

parse_test_() ->
	{timeout, 60, fun parse_config/0}.

parse_config() ->
	ExpectedMiningAddr = ar_util:decode(<<"LKC84RnISouGUw4uMQGCpPS9yDC-tIoqM2UVbUIt-Sw">>),
	ExpectedStartHashList = ar_util:decode(<<"ZFTD3ne7_U3BONHi8O-QpBv0ZQTAXJ2eFIih8dod0f8">>),
	ExpectedUpdateAddr = ar_util:decode(<<"V_zW1A2HeqWFBpwKCjyd8V6eI8S3ia8GOVA6g7YXAdU">>),
	{ok, ParsedConfig} = ar_config:parse(config_fixture()),
	?assertMatch(#config{
		benchmark = true,
		port = 1985,
		init = true,
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
		polling = true,
		auto_join = false,
		clean = true,
		diff = 42,
		mining_addr = ExpectedMiningAddr,
		max_miners = 43,
		new_key = true,
		load_key = "some_key_file",
		pause = true,
		disk_space = 44*1024*1024*1024,
		used_space = _,
		start_hash_list = ExpectedStartHashList,
		auto_update = ExpectedUpdateAddr,
		internal_api_secret = <<"some_very_very_long_secret">>,
		enable = [feature_1, feature_2],
		disable = [feature_3, feature_4],
		content_policy_files = ["some_content_policy_1", "some_content_policy_2"],
		transaction_blacklist_files = ["some_blacklist_1", "some_blacklist_2"],
		gateway_domain = <<"gateway.localhost">>,
		gateway_custom_domains = [<<"domain1.example">>, <<"domain2.example">>],
		webhooks = [
			#config_webhook{
				events = [transaction, block],
				url = <<"https://example.com/hook">>,
				headers = [{<<"Authorization">>, <<"Bearer 123456">>}]
			}
		]
	}, ParsedConfig).

config_fixture() ->
	Dir = filename:dirname(?FILE),
	Path = filename:join(Dir, "ar_config_tests_config_fixture.json"),
	{ok, FileData} = file:read_file(Path),
	FileData.

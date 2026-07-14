%%% @doc Helpers shared by the arweave_config test suites: fixture
%%% access, spec enumeration, and the expected in-memory values the
%%% legacy fixture must produce however it is loaded.
-module(arweave_config_test_util).
-compile([export_all, nowarn_export_all]).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Fixtures
%%====================================================================

read_fixture(Name) ->
	{ok, FileContents} = file:read_file(fixture_path(Name)),
	FileContents.

fixture_path(Name) ->
	BeamPath = case code:which(?MODULE) of
		non_existing -> ?FILE;
		LoadedPath when is_list(LoadedPath) -> LoadedPath
	end,
	filename:join([filename:dirname(BeamPath), "fixtures", Name]).

legacy_fixture() ->
	read_fixture("legacy_config.json").

legacy_fixture_path() ->
	fixture_path("legacy_config.json").

%%====================================================================
%% Spec enumeration
%%====================================================================

enabled_specs() ->
	[
		Spec
		|| Spec <- arweave_config_options_spec:all(),
		   maps:get(enabled, Spec, true) =/= false
	].

%% True when the option key has a templated segment (e.g. {list_item}).
is_wildcard_option(Key) ->
	lists:any(fun({_}) -> true; (_) -> false end, Key).

%%====================================================================
%% Loaded-value assertions
%%====================================================================

%% @doc `#{OptionKey => Value}' for every enabled non-wildcard option
%% except `ExcludedKeys'. Values are read through `arweave_config:get/1',
%% so unset options report their declared default and the snapshot is
%% total — two loads of the same config compare equal key for key.
loaded_option_values(ExcludedKeys) ->
	maps:from_list(
		[{Key, arweave_config:get(Key)}
		 || Spec <- enabled_specs(),
			Key <- [maps:get(option_key, Spec)],
			not is_wildcard_option(Key),
			not lists:member(Key, ExcludedKeys)]).

expected_loaded_values(ConfigLeafMap, Keys) ->
	arweave_config:with_test_config(fun() ->
		ok = arweave_config:load(ConfigLeafMap),
		maps:from_list([{Key, arweave_config:get(Key)} || Key <- Keys])
	end).

assert_loaded_values(Expected) ->
	maps:foreach(
		fun(Key, ExpectedValue) ->
			?assertEqual(ExpectedValue, arweave_config:get(Key))
		end,
		Expected).

assert_values_present(Keys) ->
	lists:foreach(fun assert_value_present/1, Keys).

assert_value_present(Key) ->
	?assertNotEqual(undefined, arweave_config:get(Key)).

%%====================================================================
%% Legacy fixture expectations
%%====================================================================

legacy_mining_address_b64() ->
	<<"LKC84RnISouGUw4uMQGCpPS9yDC-tIoqM2UVbUIt-Sw">>.

legacy_mining_addr() ->
	ar_util:decode(legacy_mining_address_b64()).

%% @doc The in-memory values `legacy_config.json' must produce. Asserted
%% both against a direct legacy load and against a load of the file the
%% converter emits from it, so the two paths are held to one expectation.
assert_legacy_json_values() ->
	?assertEqual(true, arweave_config:get([debug])),
	?assertEqual(1985, arweave_config:get([port])),
	?assertEqual(true, arweave_config:get([genesis, init])),
	?assertEqual(42, arweave_config:get([genesis, difficulty])),
	?assertEqual(true, arweave_config:get([mining, enabled])),
	?assertEqual(legacy_mining_addr(), arweave_config:get([mining, address])),
	?assertEqual(17, arweave_config:get([mining, hashing_threads])),
	?assertEqual(10, arweave_config:get([sync, jobs])),
	?assertEqual(true, arweave_config:get([sync, local_peers_only])),
	?assertEqual(false, arweave_config:get([join, auto])),
	?assertEqual(9, arweave_config:get([join, workers])),
	%% The fixture's trusted-peer list also holds a hostname, whose
	%% resolution depends on DNS. IP peers need none, so assert on one of
	%% those rather than the whole list.
	?assert(lists:member({188,166,200,45,1984},
		arweave_config:get([peers, trusted]))),
	?assertEqual(lists:sort([{192,168,2,3,1984}, {172,16,10,11,1985}]),
		lists:sort(arweave_config:get([peers, local]))),
	?assertEqual(lists:sort([{159,203,158,108,1984}, {150,150,150,150,1983}]),
		lists:sort(arweave_config:get([peers, block_gossip]))),
	?assertEqual(lists:sort([
			{127,0,0,1,1984},
			{2,3,4,5,1984},
			{6,7,8,9,1982}
		]),
		lists:sort(arweave_config:get([peers, vdf_server]))),
	?assertEqual(hiopt_m4, arweave_config:get([vdf, algorithm])),
	?assertEqual(lists:sort(legacy_storage_modules()),
		lists:sort(arweave_config_options_storage_modules:legacy_list())),
	assert_legacy_json_shaped_values(),
	ok.

%% @doc Options whose loaded shape differs from their on-the-wire shape,
%% stated in the canonical Erlang form the node's consumers expect
%% (ar_webhook, ar_tx_blacklist). These need a value written out
%% independently of any loader: a comparison between two loads cannot
%% catch a coercion that is wrong on both sides alike.
assert_legacy_json_shaped_values() ->
	?assertEqual([<<"some_blacklist_1">>, <<"some_blacklist_2">>],
		arweave_config:get([transactions, blocklist, files])),
	?assertEqual([<<"http://some_blacklist_1">>, <<"http://some_blacklist_2/x">>],
		arweave_config:get([transactions, blocklist, urls])),
	?assertEqual([<<"some_whitelist_1">>, <<"some_whitelist_2">>],
		arweave_config:get([transactions, allowlist, files])),
	?assertEqual([<<"http://some_whitelist">>],
		arweave_config:get([transactions, allowlist, urls])),
	?assertEqual(
		[#{
			enabled => true,
			url => <<"https://example.com/hook">>,
			events => [<<"transaction">>, <<"block">>],
			headers => #{<<"Authorization">> => <<"Bearer 123456">>}
		}],
		arweave_config:get([webhooks])),
	ok.

legacy_storage_modules() ->
	PartitionSize = ar_block:partition_size(),
	MiningAddr = legacy_mining_addr(),
	[
		{PartitionSize, 0, unpacked},
		{PartitionSize, 2, {spora_2_6, MiningAddr}},
		{PartitionSize, 100, unpacked},
		{1, 0, unpacked},
		{1000000000000, 14, {spora_2_6, MiningAddr}},
		{PartitionSize, 0, {replica_2_9, MiningAddr}}
	].

%%====================================================================
%% Config file shape assertions
%%====================================================================
%% Minimal smoke test that the data appears to be the correct format.
%% *Not* an exhaustive check of the data.

assert_nested_yaml_shape(FileContents) ->
	?assertMatch({_, _}, binary:match(FileContents, <<"mining:\n">>)),
	?assertMatch({_, _}, binary:match(FileContents, <<"  enabled: true\n">>)),
	?assertEqual(nomatch, binary:match(FileContents, <<"mining.enabled">>)).

assert_nested_json_shape(FileContents) ->
	Decoded = decode_json_map(FileContents),
	?assert(maps:is_key(<<"mining">>, Decoded)),
	?assertNot(maps:is_key(<<"mining.enabled">>, Decoded)),
	Mining = maps:get(<<"mining">>, Decoded),
	?assert(is_map(Mining)),
	?assertEqual(true, maps:get(<<"enabled">>, Mining)).

decode_json_map(FileContents) ->
	Decoded = jiffy:decode(FileContents, [return_maps]),
	true = is_map(Decoded),
	Decoded.

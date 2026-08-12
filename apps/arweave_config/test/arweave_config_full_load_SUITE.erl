%%% @doc Consolidated fixture-driven config loading checks across
%%% JSON/YAML, legacy JSON, CLI, legacy CLI, and env.
-module(arweave_config_full_load_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

suite() ->
    [{timetrap, {seconds, 60}}].

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

init_per_testcase(_TestCase, Config) ->
    ok = arweave_config:start(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = arweave_config:stop().

all() ->
    [
        every_option_is_covered,
        load_empty_json_defaults,
        config_formats_roundtrip,
        load_json_and_yaml,
        load_mixed_dot_and_nested_json_yaml,
        reject_mixed_dot_and_nested_conflicts,
        load_legacy_json,
        load_cli_and_legacy_cli,
        load_env
    ].

%%====================================================================
%% Test cases
%%====================================================================

%% Meta test: every registered option must appear in the full_config.yaml
%% fixture. Fails when a newly added option has no fixture entry.
every_option_is_covered(_Config) ->
    assert_all_options_are_covered(full_config_yaml, full_config_data()).

load_empty_json_defaults(_Config) ->
    arweave_config:with_test_config(fun() ->
        {ok, LeafMap} = arweave_config_format_json:parse(
            arweave_config_test_util:read_fixture("empty_config.json")),
        ok = arweave_config:load(LeafMap),
        %% Assert all option values are set to their declared defaults.
        lists:foreach(
            fun(Spec) ->
                Key = maps:get(option_key, Spec),
                Expected = maps:get(default, Spec),
                Actual = arweave_config:get(Key),
                ?assertEqual(Expected, Actual)
            end,
            defaulted_non_wildcard_specs())
    end),
    ok.

%% Exhaustive check that all config options can be parsed from all
%% supported formats.
config_formats_roundtrip(_Config) ->
    ConfigLeafMap = full_config_data(),
    lists:foreach(
        fun({_Tag, FileContents, Parser, ShapeAssert}) ->
            %% The loaded data looks like the expected format.
            ShapeAssert(FileContents),
            %% Parse the file contents and compare against
            %% the expected "canonical" data.
            {ok, ParsedLeafMap} = Parser(FileContents),
            ?assertEqual(ConfigLeafMap, ParsedLeafMap)
        end,
        config_formats(ConfigLeafMap)),
    ok.

load_json_and_yaml(_Config) ->
    ConfigLeafMap = full_config_data(),
    Expected = arweave_config_test_util:expected_loaded_values(
        ConfigLeafMap, maps:keys(ConfigLeafMap)),
    lists:foreach(
        fun({Tag, Data, Parser, _ShapeAssert}) ->
            arweave_config:with_test_config(fun() ->
                {ok, ParsedLeafMap} = Parser(Data),
                ok = arweave_config:load(ParsedLeafMap),
                assert_all_options_are_covered(Tag, ParsedLeafMap),
                arweave_config_test_util:assert_loaded_values(Expected),
                assert_full_config_values()
            end)
        end,
        config_formats(ConfigLeafMap)),
    ok.

load_mixed_dot_and_nested_json_yaml(_Config) ->
    Scenarios = [
        {fun arweave_config_format_json:parse/1, <<"{
            \"mining.enabled\": true,
            \"mining\": {\"hashing_threads\": 4},
            \"port\": 1985
        }">>},
        {fun arweave_config_format_yaml:parse/1, <<"
\"mining.enabled\": true
mining:
  hashing_threads: 4
port: 1985
">>}
    ],
    lists:foreach(fun({Parser, Data}) ->
        arweave_config:with_test_config(fun() ->
            {ok, LeafMap} = Parser(Data),
            ok = arweave_config:load(LeafMap),
            ?assertEqual(true, arweave_config:get([mining, enabled])),
            ?assertEqual(4, arweave_config:get([mining, hashing_threads])),
            ?assertEqual(1985, arweave_config:get([port]))
        end)
    end, Scenarios),
    ok.

reject_mixed_dot_and_nested_conflicts(_Config) ->
    ?assertMatch(
        {error, #{ reason := conflicting_config_key }},
        arweave_config_format_json:parse(<<"{
            \"mining.enabled\": true,
            \"mining\": {\"enabled\": false}
        }">>)),
    ?assertMatch(
        {error, #{ reason := conflicting_config_key }},
        arweave_config_format_yaml:parse(<<"
\"mining.enabled\": true
mining:
  enabled: false
">>)),
    ok.

load_legacy_json(_Config) ->
    arweave_config:with_test_config(fun() ->
        {ok, _} = arweave_config_format_legacy_json:parse(
            arweave_config_test_util:legacy_fixture()),
        assert_legacy_json_subset()
    end),
    ok.

load_cli_and_legacy_cli(_Config) ->
    ConfigLeafMap = full_config_data(),
    {Args, Keys} = cli_args(ConfigLeafMap),
    assert_cli_shape(Args),
    Expected = arweave_config_test_util:expected_loaded_values(ConfigLeafMap, Keys),
    arweave_config:with_test_config(fun() ->
        {ok, Map} = arweave_config_format_cli:parse(Args),
        ok = arweave_config:load(Map),
        arweave_config_test_util:assert_loaded_values(Expected),
        arweave_config_test_util:assert_values_present(Keys)
    end),
    assert_legacy_cli_surface(),
    ok.

load_env(_Config) ->
    ConfigLeafMap = full_config_data(),
    {Vars, Keys} = env_fixture_values(ConfigLeafMap),
    assert_env_shape(Vars),
    Expected = arweave_config_test_util:expected_loaded_values(ConfigLeafMap, Keys),
    arweave_config:with_test_config(fun() ->
        apply_env(Vars, fun() ->
            Parsed = arweave_config_format_env:parse(),
            ok = arweave_config:load(Parsed),
            arweave_config_test_util:assert_loaded_values(Expected),
            arweave_config_test_util:assert_values_present(Keys)
        end)
    end),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

%% Helpers below build generated config representations and keep the
%% test cases above focused on behavior.

covered_options() ->
    [
        maps:get(option_key, Spec)
        || Spec <- arweave_config_test_util:enabled_specs()
    ].

%% ------------------------------------------------------------------
%% Fixture conversion
%% ------------------------------------------------------------------

%% @doc #{OptionKey=>Value} map covering all enabled options.
full_config_data() ->
    {ok, ConfigLeafMap} = arweave_config_format_yaml:parse(
        arweave_config_test_util:read_fixture("full_config.yaml")),
    ConfigLeafMap.

config_formats(ConfigLeafMap) ->
    [
        {yaml, arweave_config_test_util:read_fixture("full_config.yaml"),
            fun arweave_config_format_yaml:parse/1,
            fun arweave_config_test_util:assert_nested_yaml_shape/1},
        {json, nested_json_data(ConfigLeafMap),
            fun arweave_config_format_json:parse/1,
            fun arweave_config_test_util:assert_nested_json_shape/1},
        {json_dot_only, dotted_json_data(ConfigLeafMap),
            fun arweave_config_format_json:parse/1,
            fun assert_dotted_json_shape/1},
        {yaml_dot_only, dotted_yaml_data(ConfigLeafMap),
            fun arweave_config_format_yaml:parse/1,
            fun assert_dotted_yaml_shape/1}
    ].

nested_json_data(ConfigLeafMap) ->
    {ok, NestedMap} = arweave_config_leaf_map:leaf_map_to_nested(ConfigLeafMap),
    Nested = maps_to_lists(NestedMap),
    {ok, Json} = arweave_config_format_json:encode(Nested),
    Json.

dotted_json_data(ConfigLeafMap) ->
    iolist_to_binary(jiffy:encode(dotted_map(ConfigLeafMap))).

dotted_yaml_data(ConfigLeafMap) ->
    {ok, Yaml} = arweave_config_format_yaml:encode(dotted_map(ConfigLeafMap)),
    Yaml.

%% ------------------------------------------------------------------
%% Config file shape assertions
%% ------------------------------------------------------------------
%% Minimal smoke test that the data appears to be the correct format.
%% *Not* an exhaustive check of the data. The nested-shape assertions
%% live in arweave_config_test_util — the convert suite reuses them.
assert_dotted_json_shape(FileContents) ->
    Decoded = arweave_config_test_util:decode_json_map(FileContents),
    ?assert(maps:is_key(<<"mining.enabled">>, Decoded)),
    ?assert(maps:is_key(<<"mining.hashing_threads">>, Decoded)),
    ?assertNot(maps:is_key(<<"mining">>, Decoded)).

assert_dotted_yaml_shape(FileContents) ->
    ?assertMatch({_, _}, binary:match(FileContents, <<"mining.enabled: true\n">>)),
    ?assertMatch({_, _}, binary:match(FileContents, <<"mining.hashing_threads: 4\n">>)),
    ?assertEqual(nomatch, binary:match(FileContents, <<"mining:\n">>)).

assert_cli_shape(Args) ->
    ?assert(lists:member(<<"--mining.enabled">>, Args)),
    ?assert(lists:member(<<"--mining.hashing_threads">>, Args)),
    ?assert(lists:all(fun is_binary/1, Args)),
    ?assertEqual(false, lists:any(fun contains_config_syntax/1, Args)).

assert_env_shape(Vars) ->
    Names = [env_name(Key) || {Key, _Value} <- Vars],
    ?assert(lists:member(<<"AR_MINING_ENABLED">>, Names)),
    ?assert(lists:member(<<"AR_MINING_HASHING_THREADS">>, Names)),
    ?assert(lists:all(fun(<<"AR_", _/binary>>) -> true; (_) -> false end, Names)),
    ?assert(lists:all(fun({_Key, Value}) -> is_list(Value) end, Vars)).

%% ------------------------------------------------------------------
%% Other assertions
%% ------------------------------------------------------------------

defaulted_non_wildcard_specs() ->
    [
        Spec
        || Spec <- arweave_config_test_util:enabled_specs(),
           maps:is_key(default, Spec),
           not arweave_config_test_util:is_wildcard_option(maps:get(option_key, Spec))
    ].

assert_full_config_values() ->
    ?assertEqual([{1,2,3,4,1984}],
        arweave_config:get([peers, trusted])),
    ?assertEqual([{5,6,7,8,1985}],
        arweave_config:get([peers, block_gossip])),
    ?assertEqual([{192,168,1,2,1984}],
        arweave_config:get([peers, local])),
    ?assertEqual([{9,9,9,9,1984}],
        arweave_config:get([peers, vdf_client])),
    ?assertEqual([{10,10,10,10,1984}],
        arweave_config:get([peers, vdf_server])),
    ?assertEqual([{6,6,6,6,1984}],
        arweave_config:get([peers, cm_peer])),
    ?assertEqual({7,7,7,7,1984},
        arweave_config:get([peers, cm_exit])),
    ?assertEqual([<<"blacklist-a.txt">>, <<"blacklist-b.txt">>],
        arweave_config:get([transactions, blocklist, files])),
    ?assertEqual([<<"http://blocklist.local/list.txt">>],
        arweave_config:get([transactions, blocklist, urls])),
    ?assertEqual([<<"allowlist-a.txt">>],
        arweave_config:get([transactions, allowlist, files])),
    ?assertEqual([<<"http://allowlist.local/list.txt">>],
        arweave_config:get([transactions, allowlist, urls])),
    ?assertEqual(
        [#{
            enabled => true,
            url => <<"https://example.com/hook">>,
            events => [<<"transaction">>, <<"block">>],
            headers => #{<<"Authorization">> => <<"Bearer 123">>}
        }],
        arweave_config:get([webhooks])).

contains_config_syntax(Bin) ->
    binary:match(Bin, [<<"{">>, <<"}">>, <<": ">>]) =/= nomatch.

assert_all_options_are_covered(Tag, Parsed) ->
    lists:foreach(
        fun(Key) ->
            case arweave_config_test_util:is_wildcard_option(Key) of
                false ->
                    ?assert(
                        maps:is_key(Key, Parsed) orelse
                            lists:member(Key, fixture_structural_exclusions()),
                        io_lib:format("~p fixture missing ~p", [Tag, Key]));
                true ->
                    assert_wildcard_option_covered(Tag, Key, Parsed)
            end
        end,
        covered_options()).

assert_wildcard_option_covered(Tag, WildcardOption, Parsed) ->
    ConcreteKeys = maps:get(WildcardOption, wildcard_option_concrete_keys(), []),
    ?assert(
        lists:any(fun(Key) -> maps:is_key(Key, Parsed) end, ConcreteKeys),
        io_lib:format("~p fixture missing wildcard option ~p", [Tag, WildcardOption])).

wildcard_option_concrete_keys() ->
    #{
        [webhooks, {list_item}, enabled] =>
            [[webhooks]],
        [webhooks, {list_item}, url] =>
            [[webhooks]],
        [webhooks, {list_item}, events] =>
            [[webhooks]],
        [webhooks, {list_item}, headers] =>
            [[webhooks]],
        [storage_modules, {list_item}, partition] =>
            [[storage_modules]],
        [storage_modules, {list_item}, range_start] =>
            [[storage_modules]],
        [storage_modules, {list_item}, range_end] =>
            [[storage_modules]],
        [storage_modules, {list_item}, packing_format] =>
            [[storage_modules]],
        [storage_modules, {list_item}, packing_address] =>
            [[storage_modules]],
        [storage_modules, {list_item}, defrag] =>
            [[storage_modules]],
        [repack_modules, {list_item}, partition] =>
            [[repack_modules]],
        [repack_modules, {list_item}, range_start] =>
            [[repack_modules]],
        [repack_modules, {list_item}, range_end] =>
            [[repack_modules]],
        [repack_modules, {list_item}, from_format] =>
            [[repack_modules]],
        [repack_modules, {list_item}, from_address] =>
            [[repack_modules]],
        [repack_modules, {list_item}, to_format] =>
            [[repack_modules]],
        [repack_modules, {list_item}, to_address] =>
            [[repack_modules]]
    }.

%% The concrete expected values live in arweave_config_test_util so the
%% convert suite can hold the converted file to the same expectations.
assert_legacy_json_subset() ->
    arweave_config_test_util:assert_legacy_json_values(),
    assert_legacy_json_fixture_coverage(),
    arweave_config_test_util:assert_values_present(legacy_json_supported_keys()).

assert_legacy_json_fixture_coverage() ->
    {ok, {LegacyPairs}} = ar_serialize:json_decode(
        arweave_config_test_util:legacy_fixture()),
    Keys = sets:from_list([binary_to_atom(K) || {K, _V} <- LegacyPairs]),
    lists:foreach(
        fun({LegacyKey, OptionKey}) ->
            case legacy_json_fixture_covers(LegacyKey, Keys) of
                true ->
                    ok;
                false ->
                    ct:fail({legacy_fixture_missing, LegacyKey, OptionKey})
            end
        end,
        legacy_json_supported_pairs()).

legacy_json_fixture_covers(randomx_jit, Keys) ->
    sets:is_element(disable, Keys);
legacy_json_fixture_covers(randomx_hardware_aes, Keys) ->
    sets:is_element(disable, Keys);
legacy_json_fixture_covers(randomx_large_pages, Keys) ->
    sets:is_element(enable, Keys);
legacy_json_fixture_covers(vdf_compute, Keys) ->
    sets:is_element(enable, Keys) orelse sets:is_element(disable, Keys);
legacy_json_fixture_covers(vdf_is_public_server, Keys) ->
    sets:is_element(enable, Keys);
legacy_json_fixture_covers(vdf_pull, Keys) ->
    sets:is_element(disable, Keys);
legacy_json_fixture_covers(tx_polling_enabled, Keys) ->
    sets:is_element(enable, Keys) orelse sets:is_element(disable, Keys);
legacy_json_fixture_covers(rocksdb_flush_interval_s, Keys) ->
    sets:is_element(rocksdb_flush_interval, Keys);
legacy_json_fixture_covers(rocksdb_wal_sync_interval_s, Keys) ->
    sets:is_element(rocksdb_wal_sync_interval, Keys);
legacy_json_fixture_covers(transaction_blacklist_files, Keys) ->
    sets:is_element(transaction_blacklists, Keys);
legacy_json_fixture_covers(transaction_whitelist_files, Keys) ->
    sets:is_element(transaction_whitelists, Keys);
legacy_json_fixture_covers(pool_worker_name, Keys) ->
    sets:is_element(pool_worker_name, Keys);
legacy_json_fixture_covers(http_api_transport_idle_timeout, Keys) ->
    sets:is_element('http_api.tcp.idle_timeout_seconds', Keys);
legacy_json_fixture_covers('http_api.tcp.max_connections', Keys) ->
    sets:is_element(max_connections, Keys);
legacy_json_fixture_covers(shutdown_tcp_connection_timeout, Keys) ->
    sets:is_element('network.tcp.shutdown.connection_timeout', Keys);
legacy_json_fixture_covers(shutdown_tcp_mode, Keys) ->
    sets:is_element('network.tcp.shutdown.mode', Keys);
legacy_json_fixture_covers('socket.backend', Keys) ->
    sets:is_element('network.socket.backend', Keys);
legacy_json_fixture_covers(auto_join, Keys) ->
    sets:is_element(no_auto_join, Keys);
legacy_json_fixture_covers(disk_cache_size, Keys) ->
    sets:is_element(disk_cache_size_mb, Keys);
legacy_json_fixture_covers(LegacyKey, Keys) ->
    sets:is_element(LegacyKey, Keys).

fixture_structural_exclusions() ->
    [
        %% JSON/YAML cannot represent a scalar at a path and a nested
        %% object below the same path in one document. These parent
        %% booleans are covered by CLI/env and read their defaults in
        %% the full fixture.
        [logging, handlers, debug],
        [logging, handlers, http, api],
        %% List-of-map roots are represented in config files
        %% by their `{list_item}` leaf specs.
        [storage_modules],
        [repack_modules],
        [webhooks]
    ].

%% ------------------------------------------------------------------
%% File and generated-surface helpers
%% ------------------------------------------------------------------

cli_specs(ConfigLeafMap) ->
    [
        Spec
        || Spec <- normalized_enabled_specs(),
           maps:is_key(maps:get(option_key, Spec), ConfigLeafMap),
           not arweave_config_test_util:is_wildcard_option(maps:get(option_key, Spec)),
           not cli_unsupported(Spec)
    ].

cli_args(ConfigLeafMap) ->
    Specs = cli_specs(ConfigLeafMap),
    {
        lists:flatmap(fun(Spec) -> spec_to_cli_args(Spec, ConfigLeafMap) end, Specs),
        [maps:get(option_key, Spec) || Spec <- Specs]
    }.

cli_unsupported(#{ option_key := [config_file] }) ->
    true;
cli_unsupported(#{ option_key := [config, http, listen, address] }) ->
    true;
cli_unsupported(#{ type := list }) ->
    true;
cli_unsupported(#{ type := list_map }) ->
    true;
cli_unsupported(#{ type := logging_template }) ->
    true;
cli_unsupported(_) ->
    false.

normalized_enabled_specs() ->
    {ok, Normalized} = arweave_config_options_spec:normalize_specs(
        arweave_config_options_spec:all()),
    [
        maps:get(maps:get(option_key, Spec), Normalized)
        || Spec <- arweave_config_options_spec:all(),
           maps:get(enabled, Spec, true) =/= false,
           maps:is_key(maps:get(option_key, Spec), Normalized)
    ].

spec_to_cli_args(Spec = #{ option_key := Key, long_argument := Long }, ConfigLeafMap) ->
    Value = maps:get(Key, ConfigLeafMap),
    case {maps:get(type, Spec, undefined), Value} of
        {boolean, true} -> [Long];
        _ -> [Long, to_cli_string(Value)]
    end.

env_fixture_values(ConfigLeafMap) ->
    Specs = env_specs(ConfigLeafMap),
    Vars = lists:filtermap(
        fun(Spec) -> spec_to_env_var(Spec, ConfigLeafMap) end,
        Specs),
    {
        Vars,
        [Key || {Key, _ValueString} <- Vars]
    }.

env_specs(ConfigLeafMap) ->
    [
        Spec
        || Spec <- normalized_enabled_specs(),
           maps:is_key(maps:get(option_key, Spec), ConfigLeafMap),
           not arweave_config_test_util:is_wildcard_option(maps:get(option_key, Spec)),
           not env_unsupported(Spec)
    ].

spec_to_env_var(#{ option_key := Key }, ConfigLeafMap) ->
    case to_env_string(maps:get(Key, ConfigLeafMap)) of
        {ok, ValueString} -> {true, {Key, ValueString}};
        skip -> false
    end.

%% AR_CONFIG_FILE is tested in the bootstrap suite.
env_unsupported(#{ option_key := [config_file] }) ->
    true;
env_unsupported(#{ type := list_map }) ->
    true;
env_unsupported(#{ type := logging_template }) ->
    true;
env_unsupported(_) ->
    false.

to_cli_string(V) when is_binary(V) -> V;
to_cli_string(V) when is_list(V) -> list_to_binary(V);
to_cli_string(V) when is_atom(V) -> atom_to_binary(V);
to_cli_string(V) when is_integer(V) -> integer_to_binary(V).

to_env_string(V) when is_binary(V) ->
    {ok, binary_to_list(V)};
to_env_string(V) when is_list(V) ->
    case io_lib:printable_unicode_list(V) of
        true -> {ok, V};
        false -> skip
    end;
to_env_string(V) when is_atom(V) ->
    {ok, atom_to_list(V)};
to_env_string(V) when is_integer(V) ->
    {ok, integer_to_list(V)};
to_env_string(V) when is_boolean(V) ->
    {ok, atom_to_list(V)}.

legacy_json_supported_keys() ->
    [OptionKey || {_Legacy, OptionKey} <- legacy_json_supported_pairs()].

legacy_json_supported_pairs() ->
    [
        {maps:get(legacy, Spec), maps:get(option_key, Spec)}
        || Spec <- arweave_config_options_spec:all(),
           maps:get(enabled, Spec, true) =/= false,
           maps:get(legacy, Spec, undefined) =/= undefined
    ].

assert_legacy_cli_surface() ->
    lists:foreach(
        fun({Description, Args, Assert}) ->
            arweave_config:with_test_config(fun() ->
                case arweave_config_format_legacy_cli:parse(Args) of
                    ok -> Assert();
                    Other -> erlang:error({Description, parser_failed, Other, Args})
                end
            end)
        end,
        legacy_cli_cases()).

legacy_cli_cases() ->
    boolean_flag_cases()
        ++ integer_keyword_cases()
        ++ string_keyword_cases()
        ++ enum_keyword_cases()
        ++ accumulating_cases()
        ++ singleton_compound_cases()
        ++ no_op_cases().

assert_eq(Key, Expected) ->
    ?assertEqual(Expected, arweave_config:get(Key)).

assert_peers_eq_unordered(Role, Expected) ->
    Got = arweave_config:get([peers, Role]),
    ?assertEqual(lists:sort(Expected), lists:sort(Got)).

assert_peer_eq(Role, Expected) ->
    ?assertEqual(Expected, arweave_config:get([peers, Role])).

assert_storage_modules_eq(Expected) ->
    ?assertEqual(Expected, arweave_config_options_storage_modules:legacy_list()).

assert_defrag_modules_eq(Expected) ->
    ?assertEqual(Expected, arweave_config_options_storage_modules:legacy_defrags()).

boolean_flag_cases() ->
    [
        {"mine", ["mine"], fun() -> assert_eq([mining, enabled], true) end},
        {"init", ["init"], fun() -> assert_eq([genesis, init], true) end},
        {"sync_from_local_peers_only", ["sync_from_local_peers_only"],
            fun() -> assert_eq([sync, local_peers_only], true) end},
        {"start_from_block_index", ["start_from_block_index"],
            fun() -> assert_eq([join, start_from_latest_state], true) end},
        {"start_from_latest_state", ["start_from_latest_state"],
            fun() -> assert_eq([join, start_from_latest_state], true) end},
        {"no_auto_join", ["no_auto_join"],
            fun() -> assert_eq([join, auto], false) end},
        {"data_sync_request_packed_chunks", ["data_sync_request_packed_chunks"],
            fun() -> assert_eq([sync, request_packed_chunks], true) end},
        {"disable_replica_2_9_device_limit", ["disable_replica_2_9_device_limit"],
            fun() -> assert_eq([disable_device_limit], true) end},
        {"debug", ["debug"], fun() -> assert_eq([debug], true) end},
        {"run_defragmentation", ["run_defragmentation"],
            fun() -> assert_eq([defrag, enabled], true) end},
        {"coordinated_mining", ["coordinated_mining"],
            fun() -> assert_eq([cm, enabled], true) end},
        {"is_pool_server", ["is_pool_server"],
            fun() -> assert_eq([pool, is_server], true) end},
        {"is_pool_client", ["is_pool_client"],
            fun() -> assert_eq([pool, is_client], true) end}
    ].

integer_keyword_cases() ->
    Specs = [
        {"port", "9999", [port], 9999},
        {"repack_batch_size", "42", [packing, repack, batch_size], 42},
        {"polling", "10", [gossip, block, poll_interval], 10},
        {"block_pollers", "20", [gossip, block, pollers], 20},
        {"join_workers", "5", [join, workers], 5},
        {"diff", "42", [genesis, difficulty], 42},
        {"hashing_threads", "8", [mining, hashing_threads], 8},
        {"data_cache_size_limit", "10000", [sync, cache_size_limit], 10000},
        {"packing_cache_size_limit", "20000", [packing, cache_size], 20000},
        {"mining_cache_size_mb", "3", [mining, cache_size], 3},
        {"max_emitters", "4", [gossip, tx, max_emitters], 4},
        {"disk_space_check_frequency", "10", [disk_space_check_frequency], 10000},
        {"max_propagation_peers", "8", [gossip, tx, max_peers], 8},
        {"max_block_propagation_peers", "60", [gossip, block, max_peers], 60},
        {"sync_jobs", "10", [sync, jobs], 10},
        {"header_sync_jobs", "1", [gossip, header_sync_jobs], 1},
        {"post_tx_timeout", "50", [gossip, tx, post_timeout], 50},
        {"max_connections", "512", [network, server, tcp, max_connections], 512},
        {"disk_pool_data_root_expiration_time", "10000",
            [disk_pool, data_root_expiration_time], 10000},
        {"max_disk_pool_buffer_mb", "100000", [disk_pool, max_buffer_size], 100000},
        {"max_disk_pool_data_root_buffer_mb", "100000000",
            [disk_pool, max_data_root_buffer_size], 100000000},
        {"disk_cache_size_mb", "1024", [gossip, header_cache_size], 1024},
        {"packing_workers", "25", [packing, workers], 25},
        {"replica_2_9_workers", "16", [packing, entropy, workers], 16},
        {"replica_2_9_entropy_cache_size_mb", "2000",
            [packing, entropy, cache_size], 2000},
        {"max_vdf_validation_thread_count", "2", [vdf, max_validation_threads], 2},
        {"max_vdf_last_step_validation_thread_count", "3",
            [vdf, max_last_step_validation_threads], 3},
        {"defragmentation_trigger_threshold", "1000", [defrag, threshold], 1000},
        {"block_throttle_by_ip_interval", "5000",
            [gossip, block, throttle_by_ip_interval], 5000},
        {"block_throttle_by_solution_interval", "12000",
            [gossip, block, throttle_by_solution_interval], 12000},
        {"http_api.tcp.idle_timeout_seconds", "15",
            [network, server, transport, idle_timeout], 15000},
        {"cm_poll_interval", "1000", [cm, poll_interval], 1000},
        {"cm_out_batch_timeout", "20", [cm, out_batch_timeout], 20},
        {"rocksdb_flush_interval", "1800", [rocksdb, flush_interval], 1800},
        {"rocksdb_wal_sync_interval", "60", [rocksdb, wal_sync_interval], 60},
        {"network.tcp.connection_timeout", "30",
            [network, server, shutdown_connection_timeout], 30},
        {"http_client.http.keepalive", "30000",
            [network, client, http, keepalive], 30000},
        {"http_client.tcp.linger_timeout", "0",
            [network, client, tcp, linger_timeout], 0},
        {"http_client.tcp.send_timeout", "15000",
            [network, client, tcp, send_timeout], 15000},
        {"http_api.http.active_n", "100", [network, server, http, active_n], 100},
        {"http_api.http.inactivity_timeout", "300000",
            [network, server, http, inactivity_timeout], 300000},
        {"http_api.http.linger_timeout", "1000",
            [network, server, http, linger_timeout], 1000},
        {"http_api.http.request_timeout", "5000",
            [network, server, http, request_timeout], 5000},
        {"http_api.tcp.backlog", "1024", [network, server, tcp, backlog], 1024},
        {"http_api.tcp.linger_timeout", "0",
            [network, server, tcp, linger_timeout], 0},
        {"http_api.tcp.listener_shutdown", "5000",
            [network, server, tcp, listener_shutdown], 5000},
        {"http_api.tcp.num_acceptors", "500",
            [network, server, tcp, num_acceptors], 500},
        {"http_api.tcp.send_timeout", "15000",
            [network, server, tcp, send_timeout], 15000},
        {"chunk_storage_file_size", "2097152000", [chunk_storage_file_size], 2097152000}
    ],
    [{Keyword, [Keyword, Value], fun() -> assert_eq(Key, Expected) end}
        || {Keyword, Value, Key, Expected} <- Specs].

string_keyword_cases() ->
    Specs = [
        {"data_dir", "test_data_dir", [data_dir], "test_data_dir"},
        {"log_dir", "test_log_dir", [log_dir], "test_log_dir"},
        {"start_from_state", "test_state_folder", [join, start_from_state],
            "test_state_folder"},
        {"internal_api_secret", "some_very_long_internal_api_secret",
            [internal_api_secret], <<"some_very_long_internal_api_secret">>},
        {"cm_api_secret", "some_very_long_cm_secret_value",
            [cm, api_secret], <<"some_very_long_cm_secret_value">>},
        {"pool_api_key", "some_pool_api_key", [pool, api_key], <<"some_pool_api_key">>},
        {"pool_server_address", "pool.example.com", [pool, server_address],
            <<"pool.example.com">>},
        {"pool_worker_name", "worker1", [pool, worker_name], <<"worker1">>}
    ],
    [{Keyword, [Keyword, Value], fun() -> assert_eq(Key, Expected) end}
        || {Keyword, Value, Key, Expected} <- Specs].

enum_keyword_cases() ->
    [
        {"verify purge", ["verify", "purge"], fun() -> assert_eq([verify, mode], purge) end},
        {"verify log", ["verify", "log"], fun() -> assert_eq([verify, mode], log) end},
        {"verify_samples all", ["verify_samples", "all"],
            fun() -> assert_eq([verify, samples], all) end},
        {"verify_samples N", ["verify_samples", "100"],
            fun() -> assert_eq([verify, samples], 100) end},
        {"vdf hiopt_m4", ["vdf", "hiopt_m4"],
            fun() -> assert_eq([vdf, algorithm], hiopt_m4) end},
        {"network.socket.backend socket", ["network.socket.backend", "socket"],
            fun() -> assert_eq([network, server, socket_backend], socket) end},
        {"network.tcp.shutdown.mode shutdown",
            ["network.tcp.shutdown.mode", "shutdown"],
            fun() -> assert_eq([network, server, shutdown_mode], shutdown) end},
        {"max_duplicate_data_roots infinity", ["max_duplicate_data_roots", "infinity"],
            fun() -> assert_eq([gossip, data_roots, max_duplicates], infinity) end},
        {"enable_data_roots_syncing true", ["enable_data_roots_syncing", "true"],
            fun() -> assert_eq([gossip, data_roots, syncing_enabled], true) end},
        {"http_client.tcp.delay_send true", ["http_client.tcp.delay_send", "true"],
            fun() -> assert_eq([network, client, tcp, delay_send], true) end},
        {"http_client.tcp.keepalive true", ["http_client.tcp.keepalive", "true"],
            fun() -> assert_eq([network, client, tcp, keepalive], true) end},
        {"http_client.tcp.linger true", ["http_client.tcp.linger", "true"],
            fun() -> assert_eq([network, client, tcp, linger], true) end},
        {"http_client.tcp.nodelay true", ["http_client.tcp.nodelay", "true"],
            fun() -> assert_eq([network, client, tcp, nodelay], true) end},
        {"http_client.tcp.send_timeout_close true",
            ["http_client.tcp.send_timeout_close", "true"],
            fun() -> assert_eq([network, client, tcp, send_timeout_close], true) end},
        {"http_api.tcp.delay_send true", ["http_api.tcp.delay_send", "true"],
            fun() -> assert_eq([network, server, tcp, delay_send], true) end},
        {"http_api.tcp.keepalive false", ["http_api.tcp.keepalive", "false"],
            fun() -> assert_eq([network, server, tcp, keepalive], false) end},
        {"http_api.tcp.linger true", ["http_api.tcp.linger", "true"],
            fun() -> assert_eq([network, server, tcp, linger], true) end},
        {"http_api.tcp.nodelay true", ["http_api.tcp.nodelay", "true"],
            fun() -> assert_eq([network, server, tcp, nodelay], true) end},
        {"http_api.tcp.send_timeout_close true",
            ["http_api.tcp.send_timeout_close", "true"],
            fun() -> assert_eq([network, server, tcp, send_timeout_close], true) end}
    ].

accumulating_cases() ->
    [
        {"peer accumulates", ["peer", "1.2.3.4:1984", "peer", "5.6.7.8:1984"],
            fun() -> assert_peers_eq_unordered(trusted,
                [{5,6,7,8,1984}, {1,2,3,4,1984}]) end},
        {"block_gossip_peer accumulates", ["block_gossip_peer", "1.2.3.4:1984"],
            fun() -> assert_peers_eq_unordered(block_gossip, [{1,2,3,4,1984}]) end},
        {"local_peer accumulates", ["local_peer", "192.168.0.1:1984"],
            fun() -> assert_peers_eq_unordered(local, [{192,168,0,1,1984}]) end},
        {"cm_peer accumulates", ["cm_peer", "1.2.3.4:1984"],
            fun() -> assert_peers_eq_unordered(cm_peer, [{1,2,3,4,1984}]) end},
        {"vdf_client_peer accumulates", ["vdf_client_peer", "2.3.4.5:1984"],
            fun() -> assert_peers_eq_unordered(vdf_client, [{2,3,4,5,1984}]) end},
        {"vdf_server_trusted_peer accumulates", ["vdf_server_trusted_peer", "127.0.0.1"],
            fun() -> assert_peers_eq_unordered(vdf_server, [{127,0,0,1,1984}]) end},
        {"transaction_blacklist accumulates", ["transaction_blacklist", "/list1"],
            fun() -> assert_eq([transactions, blocklist, files], [<<"/list1">>]) end},
        {"transaction_blacklist_url accumulates",
            ["transaction_blacklist_url", "http://example.com/b1"],
            fun() -> assert_eq([transactions, blocklist, urls],
                [<<"http://example.com/b1">>]) end},
        {"transaction_whitelist accumulates", ["transaction_whitelist", "/wl1"],
            fun() -> assert_eq([transactions, allowlist, files], [<<"/wl1">>]) end},
        {"transaction_whitelist_url accumulates",
            ["transaction_whitelist_url", "http://example.com/w1"],
            fun() -> assert_eq([transactions, allowlist, urls],
                [<<"http://example.com/w1">>]) end},
        %% Catalog flag — classified directly into [features, Name].
        {"enable promotes catalog flag", ["enable", "disk_logging"],
            fun() -> assert_eq([features, disk_logging], true) end},
        {"disable promotes catalog flag", ["disable", "miner_logging"],
            fun() -> assert_eq([features, miner_logging], false) end},
        %% Promotion-table flag — classified to its dedicated option_key.
        {"enable promotes randomx_large_pages", ["enable", "randomx_large_pages"],
            fun() -> assert_eq([randomx, large_pages], true) end},
        {"disable promotes randomx_jit", ["disable", "randomx_jit"],
            fun() -> assert_eq([randomx, jit], false) end}
    ].

singleton_compound_cases() ->
    [
        {"cm_exit_peer", ["cm_exit_peer", "1.2.3.4:1984"],
            fun() -> assert_peer_eq(cm_exit, {1,2,3,4,1984}) end},
        {"mining_addr", ["mining_addr", "LKC84RnISouGUw4uMQGCpPS9yDC-tIoqM2UVbUIt-Sw"],
            fun() -> assert_eq([mining, address],
                arweave_util:decode(<<"LKC84RnISouGUw4uMQGCpPS9yDC-tIoqM2UVbUIt-Sw">>)) end},
        {"start_from_block",
            ["start_from_block",
             "lfoR_PyKV6t7Z6Xi2QJZlZ0JWThh0Ke7Zc5Q82CSshUhFGcjiYufP234ph1mVofX"],
            fun() -> assert_eq([join, start_from_block],
                arweave_util:decode(
                    <<"lfoR_PyKV6t7Z6Xi2QJZlZ0JWThh0Ke7Zc5Q82CSshUhFGcjiYufP234ph1mVofX">>)) end},
        {"storage_module unpacked", ["storage_module", "0,unpacked"],
            fun() ->
                PartitionSize = ar_block:partition_size(),
                assert_storage_modules_eq([{PartitionSize, 0, unpacked}]) end},
        {"defragment_module unpacked", ["defragment_module", "0,unpacked"],
            fun() ->
                PartitionSize = ar_block:partition_size(),
                assert_defrag_modules_eq([{PartitionSize, 0, unpacked}]) end}
    ].

no_op_cases() ->
    [
        {"config_file is a no-op", ["config_file", "/some/path"], fun() -> ok end}
    ].

apply_env(Vars, Fun) ->
    Names = [binary_to_list(env_name(Key)) || {Key, _} <- Vars],
    lists:foreach(fun({Key, Value}) ->
        true = os:putenv(binary_to_list(env_name(Key)), Value)
    end, Vars),
    try
        Fun()
    after
        lists:foreach(fun(Name) -> os:unsetenv(Name) end, Names)
    end.

env_name(OptionKey) ->
    {ok, Normalized} = arweave_config_options_spec:normalize_specs(
        arweave_config_options_spec:all()),
    Spec = maps:get(OptionKey, Normalized),
    maps:get(environment, Spec).

dotted_map(ConfigLeafMap) ->
    maps:from_list([
        {dotted_key(Key), Value}
        || {Key, Value} <- maps:to_list(ConfigLeafMap)
    ]).

dotted_key(Key) ->
    iolist_to_binary(lists:join(<<".">>, [dotted_segment(Segment) || Segment <- Key])).

dotted_segment(Segment) when is_atom(Segment) ->
    atom_to_binary(Segment);
dotted_segment(Segment) when is_integer(Segment) ->
    integer_to_binary(Segment);
dotted_segment(Segment) when is_binary(Segment) ->
    <<"[", Segment/binary, "]">>.

maps_to_lists(NestedMap) when is_map(NestedMap) ->
    case consecutive_integer_keys(maps:keys(NestedMap)) of
        {true, Keys} ->
            [maps_to_lists(maps:get(Key, NestedMap)) || Key <- Keys];
        false ->
            maps:map(fun(_Key, Value) -> maps_to_lists(Value) end, NestedMap)
    end;
maps_to_lists(List) when is_list(List) ->
    [maps_to_lists(Value) || Value <- List];
maps_to_lists(Value) ->
    Value.

consecutive_integer_keys([]) ->
    false;
consecutive_integer_keys(Keys) ->
    case lists:all(fun is_integer/1, Keys) of
        true ->
            Sorted = lists:sort(Keys),
            case Sorted =:= lists:seq(1, length(Sorted)) of
                true -> {true, Sorted};
                false -> false
            end;
        false ->
            false
    end.

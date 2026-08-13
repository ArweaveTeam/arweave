-module(arweave_config_convert_SUITE).
-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

suite() ->
    [{timetrap, {seconds, 60}}].

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

init_per_testcase(_TestCase, Config) ->
    file:delete(out_path(Config, "converted.json")),
    file:delete(out_path(Config, "converted.yaml")),
    ok = arweave_config:start(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = arweave_config:stop(),
    ok.

all() ->
    [
     convert_to_json_preserves_legacy_semantics,
     convert_to_yaml_preserves_legacy_semantics,
     converted_json_shape,
     converted_yaml_shape,
     store_left_untouched,
     string_format_accepted,
     unsupported_format_rejected,
     missing_input_rejected,
     empty_local_peers_becomes_empty_array,
     no_implicit_storage_defaults_emitted,
     only_user_semaphores_emitted,
     peer_strings_preserved,
     yaml_string_scalars_and_minimal_quoting,
     custom_bucket_size_storage_module_rejected,
     custom_bucket_size_repack_module_rejected,
     legacy_fixture_custom_bucket_sizes_rejected
    ].

%%====================================================================
%% Test cases
%%====================================================================

convert_to_json_preserves_legacy_semantics(Config) ->
    assert_round_trip(json, fun arweave_config_format_json:parse/1, Config).

convert_to_yaml_preserves_legacy_semantics(Config) ->
    assert_round_trip(yaml, fun arweave_config_format_yaml:parse/1, Config).

%% The address must be written as base64url *text*. A load round-trip
%% cannot see this: the `address' type would decode several spellings
%% back to the same bytes, so only the raw file catches a regression.
converted_json_shape(Config) ->
    Raw = convert(json, Config),
    arweave_config_test_util:assert_nested_json_shape(Raw),
    ?assertNotEqual(nomatch,
        binary:match(Raw, arweave_config_test_util:legacy_mining_address_b64())),
    ok.

converted_yaml_shape(Config) ->
    Raw = convert(yaml, Config),
    arweave_config_test_util:assert_nested_yaml_shape(Raw),
    ?assertNotEqual(nomatch,
        binary:match(Raw, arweave_config_test_util:legacy_mining_address_b64())),
    ok.

%% The converter borrows the global store transiently and must restore
%% it. Each testcase starts with a fresh (empty) store, so it must be
%% empty again afterwards.
store_left_untouched(Config) ->
    Before = lists:sort(arweave_config_store:items_with_prefix([])),
    ok = arweave_config_convert:convert(json, sanitized_legacy_path(Config),
        out_path(Config, "isolation.json")),
    After = lists:sort(arweave_config_store:items_with_prefix([])),
    ?assertEqual(Before, After),
    ok.

%% The CLI passes the format as a string, not an atom. Before the
%% encoder mapped strings explicitly, any string/binary format made
%% encoder/1 recurse on itself forever (caught here by the timetrap).
string_format_accepted(Config) ->
	ok = arweave_config_convert:convert("json", sanitized_legacy_path(Config),
		out_path(Config, "string_format.json")),
	ok = arweave_config_convert:convert(<<"YAML">>, sanitized_legacy_path(Config),
		out_path(Config, "string_format.yaml")),
	?assertMatch({error, {unsupported_format, "xml"}},
		arweave_config_convert:convert("xml", legacy_path(),
			out_path(Config, "unused.out"))),
	ok.

unsupported_format_rejected(Config) ->
    ?assertMatch({error, {unsupported_format, _}},
        arweave_config_convert:convert(xml, legacy_path(),
            out_path(Config, "unused.out"))),
    ok.

missing_input_rejected(Config) ->
    Missing = out_path(Config, "does_not_exist.json"),
    ?assertMatch({error, {read_input, _, enoent}},
        arweave_config_convert:convert(json, Missing,
            out_path(Config, "unused.json"))),
    ok.

%% A legacy `local_peers: []' must convert to an empty array
%% (`"local": []'), not an empty string (`"local": ""'): the peers type
%% rejects `<<>>', so an empty string would fail to load at all.
empty_local_peers_becomes_empty_array(Config) ->
    Input = out_path(Config, "empty_local_peers.json"),
    ok = file:write_file(Input, <<"{\"local_peers\": []}">>),
    Out = out_path(Config, "empty_local_peers_out.json"),
    ok = arweave_config_convert:convert(json, Input, Out),
    {ok, Raw} = file:read_file(Out),
    arweave_config:with_test_config(fun() ->
        {ok, LeafMap} = arweave_config_format_json:parse(Raw),
        ok = arweave_config:load(LeafMap),
        ?assertEqual([], arweave_config:get([peers, local]))
    end),
    ok.

%% Parsing storage_modules used to write an explicit repack_modules []
%% into the store, and every module map carries an implicit
%% `defrag => false' — neither written by the operator, neither should
%% be emitted.
no_implicit_storage_defaults_emitted(Config) ->
    Raw = convert_raw(Config, "storage",
        <<"{\"storage_modules\": [\"0,unpacked\", \"1,unpacked\"]}">>),
    ?assertEqual(nomatch, binary:match(Raw, <<"repack_modules">>)),
    ?assertEqual(nomatch, binary:match(Raw, <<"defrag">>)),
    ?assertNotEqual(nomatch, binary:match(Raw, <<"storage_modules">>)),
    ok.

%% Legacy semaphores parsing used to materialize the full default map;
%% only the operator-written entries may appear in the output.
only_user_semaphores_emitted(Config) ->
    Raw = convert_raw(Config, "semaphores",
        <<"{\"semaphores\": {\"get_chunk\": 1000}}">>),
    ?assertNotEqual(nomatch, binary:match(Raw, <<"get_chunk">>)),
    ?assertEqual(nomatch, binary:match(Raw, <<"post_tx">>)),
    ?assertEqual(nomatch, binary:match(Raw, <<"get_wallet_list">>)),
    ok.

%% Peer strings must survive conversion verbatim: no hostname->IP
%% resolution baked into the file, no default :1984 appended.
peer_strings_preserved(Config) ->
    Raw = convert_raw(Config, "peers",
        <<"{\"peers\": [\"localhost\", \"127.0.0.1:2984\"],"
          " \"local_peers\": [\"127.0.0.1\"]}">>),
    ?assertNotEqual(nomatch, binary:match(Raw, <<"\"localhost\"">>)),
    ?assertNotEqual(nomatch, binary:match(Raw, <<"\"127.0.0.1:2984\"">>)),
    ?assertNotEqual(nomatch, binary:match(Raw, <<"\"127.0.0.1\"">>)),
    ?assertEqual(nomatch, binary:match(Raw, <<"127.0.0.1:1984">>)),
    ok.

%% The legacy parser stores paths as Erlang strings, which the YAML
%% encoder must emit as scalars, not sequences of character codes.
%% Quoting must be minimal and consistent: plain-safe strings
%% (hostnames, paths) stay unquoted; only YAML-special content
%% (host:port colons) is quoted.
yaml_string_scalars_and_minimal_quoting(Config) ->
    Input = out_path(Config, "yaml_scalars_in.json"),
    ok = file:write_file(Input,
        <<"{\"data_dir\": \"/opt/data\","
          " \"peers\": [\"chain-1.arweave.xyz\", \"1.2.3.4:1985\"]}">>),
    Out = out_path(Config, "yaml_scalars_out.yaml"),
    ok = arweave_config_convert:convert(yaml, Input, Out),
    {ok, Raw} = file:read_file(Out),
    ?assertNotEqual(nomatch, binary:match(Raw, <<"data_dir: /opt/data\n">>)),
    ?assertNotEqual(nomatch, binary:match(Raw, <<"- chain-1.arweave.xyz\n">>)),
    ?assertNotEqual(nomatch, binary:match(Raw, <<"- \"1.2.3.4:1985\"\n">>)),
    ok.

%% Custom (non-partition) bucket sizes cannot be converted without
%% renaming the module's on-disk directory, so the converter must
%% refuse them, naming the offending modules.
custom_bucket_size_storage_module_rejected(Config) ->
    Input = out_path(Config, "custom_bucket_in.json"),
    ok = file:write_file(Input,
        <<"{\"storage_modules\": [\"9,unpacked\", \"5,1000000,unpacked\"]}">>),
    Out = out_path(Config, "custom_bucket_out.json"),
    ?assertMatch(
        {error, {unsupported_custom_bucket_sizes, Message}}
            when is_binary(Message),
        arweave_config_convert:convert(json, Input, Out)),
    ?assertNot(filelib:is_regular(Out)),
    ok.

custom_bucket_size_repack_module_rejected(Config) ->
    Input = out_path(Config, "custom_bucket_repack_in.json"),
    ok = file:write_file(Input,
        <<"{\"storage_modules\": "
          "[\"5,1000000,unpacked,repack_in_place,"
          "LKC84RnISouGUw4uMQGCpPS9yDC-tIoqM2UVbUIt-Sw\"]}">>),
    Out = out_path(Config, "custom_bucket_repack_out.json"),
    ?assertMatch(
        {error, {unsupported_custom_bucket_sizes, Message}}
            when is_binary(Message),
        arweave_config_convert:convert(json, Input, Out)),
    ?assertNot(filelib:is_regular(Out)),
    ok.

%% The shared legacy fixture deliberately contains custom bucket
%% sizes (two storage modules and one repack-in-place source);
%% converting it must fail.
legacy_fixture_custom_bucket_sizes_rejected(Config) ->
    ?assertMatch(
        {error, {unsupported_custom_bucket_sizes, Message}}
            when is_binary(Message),
        arweave_config_convert:convert(json, legacy_path(),
            out_path(Config, "fixture_rejected.json"))),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

%% @doc The shared legacy fixture minus its custom-bucket-size
%% entries (which the converter rejects) - the input for the
%% conversion round-trip tests. Partition-sized entries are the
%% 2-part strings and the 4-part repack_in_place strings. The
%% `defragment_modules' list holds the same notation and synthesizes
%% storage modules on load, so it is filtered the same way.
sanitized_legacy_path(Config) ->
    {ok, Raw} = file:read_file(legacy_path()),
    Decoded = jiffy:decode(Raw, [return_maps]),
    Sanitized = Decoded#{
        <<"storage_modules">> =>
            [Entry || Entry <- maps:get(<<"storage_modules">>, Decoded),
                partition_sized(Entry)],
        <<"defragment_modules">> =>
            [Entry || Entry <- maps:get(<<"defragment_modules">>, Decoded),
                partition_sized(Entry)]
    },
    Path = out_path(Config, "sanitized_legacy.json"),
    ok = file:write_file(Path, jiffy:encode(Sanitized)),
    Path.

partition_sized(Entry) ->
    case binary:split(Entry, <<",">>, [global]) of
        [_Partition, _Packing] -> true;
        [_Partition, _Packing, <<"repack_in_place">>, _ToPacking] -> true;
        _ -> false
    end.

convert_raw(Config, Name, LegacyJSON) ->
    Input = out_path(Config, Name ++ "_in.json"),
    ok = file:write_file(Input, LegacyJSON),
    Out = out_path(Config, Name ++ "_out.json"),
    ok = arweave_config_convert:convert(json, Input, Out),
    {ok, Raw} = file:read_file(Out),
    Raw.

%% @doc Loading the converted file must leave the node in exactly the
%% state the legacy file itself produces — converting a config may not
%% change how the node behaves.
assert_round_trip(Format, Parse, Config) ->
    {ok, Sanitized} = file:read_file(sanitized_legacy_path(Config)),
    FromLegacy = arweave_config:with_test_config(fun() ->
        {ok, ok} = arweave_config_format_legacy_json:parse(Sanitized),
        option_values()
    end),
    Raw = convert(Format, Config),
    FromConverted = arweave_config:with_test_config(fun() ->
        {ok, LeafMap} = Parse(Raw),
        ok = arweave_config:load(LeafMap),
        %% Anchor the converted side to concrete expected values. The
        %% comparison below is relative: without this, a conversion that
        %% silently produced nothing would compare equal to a legacy load
        %% that silently produced nothing, and pass.
        arweave_config_test_util:assert_legacy_json_values(
            arweave_config_test_util:partition_sized_legacy_storage_modules()),
        option_values()
    end),
    %% Report the differing keys rather than two ~300-key maps.
    Diff = maps:filter(
        fun(Key, Value) -> maps:get(Key, FromConverted, undefined) =/= Value end,
        FromLegacy),
    ?assertEqual(#{}, maps:map(
        fun(Key, Value) -> {legacy, Value, converted, maps:get(Key, FromConverted)} end,
        Diff)).

convert(Format, Config) ->
    Out = out_path(Config, "converted." ++ atom_to_list(Format)),
    ok = arweave_config_convert:convert(Format, sanitized_legacy_path(Config), Out),
    true = filelib:is_regular(Out),
    {ok, Raw} = file:read_file(Out),
    Raw.

option_values() ->
    arweave_config_test_util:loaded_option_values(dns_dependent_options()).

%% The fixture lists a hostname among the trusted peers, so the loaded
%% value depends on a DNS lookup. A round-trip resolves it twice — once
%% inside the converter, once when loading the legacy file directly — so
%% the two sides can legitimately disagree. Its IP peers are asserted by
%% arweave_config_test_util:assert_legacy_json_values/0 instead.
dns_dependent_options() ->
    [[peers, trusted]].

legacy_path() ->
    arweave_config_test_util:legacy_fixture_path().

out_path(Config, Name) ->
    filename:join(proplists:get_value(priv_dir, Config), Name).

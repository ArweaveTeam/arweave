-module(arweave_config_convert_SUITE).
-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

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
		unsupported_format_rejected,
		missing_input_rejected,
		empty_local_peers_becomes_empty_array
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
	ok = arweave_config_convert:convert(json, legacy_fixture_path(),
		out_path(Config, "isolation.json")),
	After = lists:sort(arweave_config_store:items_with_prefix([])),
	?assertEqual(Before, After),
	ok.

unsupported_format_rejected(Config) ->
	?assertMatch({error, {unsupported_format, _}},
		arweave_config_convert:convert(xml, legacy_fixture_path(),
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

%%====================================================================
%% Helpers
%%====================================================================

%% @doc Loading the converted file must leave the node in exactly the
%% state the legacy file itself produces — converting a config may not
%% change how the node behaves.
assert_round_trip(Format, Parse, Config) ->
	FromLegacy = arweave_config:with_test_config(fun() ->
		{ok, ok} = arweave_config_format_legacy_json:parse(legacy_fixture()),
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
		arweave_config_test_util:assert_legacy_json_values(),
		assert_trusted_peers_converted(),
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
	ok = arweave_config_convert:convert(Format, legacy_fixture_path(), Out),
	true = filelib:is_regular(Out),
	{ok, Raw} = file:read_file(Out),
	Raw.

option_values() ->
	arweave_config_test_util:loaded_option_values(dns_dependent_options()).

%% The fixture lists a hostname among the trusted peers, so the loaded
%% value depends on a DNS lookup. A round-trip resolves it twice — once
%% inside the converter, once when loading the legacy file directly — so
%% the two sides can legitimately disagree. Checked separately below.
dns_dependent_options() ->
	[[peers, trusted]].

assert_trusted_peers_converted() ->
	%% IP peers need no DNS, so this entry is deterministic.
	?assert(lists:member({188,166,200,45,1984},
		arweave_config:get([peers, trusted]))).

legacy_fixture() ->
	arweave_config_test_util:legacy_fixture().

legacy_fixture_path() ->
	arweave_config_test_util:legacy_fixture_path().

out_path(Config, Name) ->
	filename:join(proplists:get_value(priv_dir, Config), Name).

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
		convert_legacy_to_json,
		convert_legacy_to_yaml,
		json_and_yaml_agree_on_scalars,
		store_left_untouched,
		unsupported_format_rejected,
		missing_input_rejected,
		empty_local_peers_becomes_empty_array
	].

%%====================================================================
%% Test cases
%%====================================================================

convert_legacy_to_json(Config) ->
	Out = out_path(Config, "converted.json"),
	ok = arweave_config_convert:convert(json, legacy_fixture_path(), Out),
	true = filelib:is_regular(Out),
	{ok, Raw} = file:read_file(Out),
	{ok, Leaf} = arweave_config_format_json:parse(Raw),
	assert_common_leaves(Leaf),
	%% Addresses are emitted as base64url text, not raw bytes.
	?assertEqual(<<"LKC84RnISouGUw4uMQGCpPS9yDC-tIoqM2UVbUIt-Sw">>,
		maps:get([mining, address], Leaf)),
	?assertNotEqual(nomatch,
		binary:match(Raw, <<"LKC84RnISouGUw4uMQGCpPS9yDC-tIoqM2UVbUIt-Sw">>)),
	ok.

convert_legacy_to_yaml(Config) ->
	Out = out_path(Config, "converted.yaml"),
	ok = arweave_config_convert:convert(yaml, legacy_fixture_path(), Out),
	true = filelib:is_regular(Out),
	{ok, Raw} = file:read_file(Out),
	{ok, Leaf} = arweave_config_format_yaml:parse(Raw),
	assert_common_leaves(Leaf),
	?assertEqual(<<"LKC84RnISouGUw4uMQGCpPS9yDC-tIoqM2UVbUIt-Sw">>,
		maps:get([mining, address], Leaf)),
	ok.

%% Both target formats describe the same configuration, so the scalar
%% leaves must agree after a round-trip through each reader.
json_and_yaml_agree_on_scalars(Config) ->
	JsonOut = out_path(Config, "agree.json"),
	YamlOut = out_path(Config, "agree.yaml"),
	ok = arweave_config_convert:convert(json, legacy_fixture_path(), JsonOut),
	ok = arweave_config_convert:convert(yaml, legacy_fixture_path(), YamlOut),
	{ok, JsonRaw} = file:read_file(JsonOut),
	{ok, YamlRaw} = file:read_file(YamlOut),
	{ok, JsonLeaf} = arweave_config_format_json:parse(JsonRaw),
	{ok, YamlLeaf} = arweave_config_format_yaml:parse(YamlRaw),
	Keys = [
		[port],
		[data_dir],
		[debug],
		[mining, address],
		[semaphores, post_chunk, limit]
	],
	lists:foreach(
		fun(Key) ->
			?assertEqual(maps:get(Key, JsonLeaf), maps:get(Key, YamlLeaf))
		end,
		Keys),
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
%% (`"local": []'), not an empty string (`"local": ""').
empty_local_peers_becomes_empty_array(Config) ->
	Input = out_path(Config, "empty_local_peers.json"),
	ok = file:write_file(Input, <<"{\"local_peers\": []}">>),
	Out = out_path(Config, "empty_local_peers_out.json"),
	ok = arweave_config_convert:convert(json, Input, Out),
	{ok, Raw} = file:read_file(Out),
	%% jiffy decodes an empty JSON array to `[]' and an empty JSON
	%% string to `<<>>', so this distinguishes the two.
	#{<<"peers">> := #{<<"local">> := Local}} =
		jiffy:decode(Raw, [return_maps]),
	?assertEqual([], Local),
	%% And the new-format reader round-trips it back to an empty list.
	{ok, Leaf} = arweave_config_format_json:parse(Raw),
	?assertEqual([], maps:get([peers, local], Leaf)),
	ok.

%%====================================================================
%% Helpers
%%====================================================================

assert_common_leaves(Leaf) ->
	?assertEqual(1985, maps:get([port], Leaf)),
	?assertEqual(<<"some_data_dir">>, maps:get([data_dir], Leaf)),
	?assertEqual(true, maps:get([debug], Leaf)),
	?assertEqual(999, maps:get([semaphores, post_chunk, limit], Leaf)),
	Trusted = maps:get([peers, trusted], Leaf),
	?assert(is_list(Trusted)),
	%% IP peers need no DNS, so this entry is deterministic.
	?assert(lists:member(<<"188.166.200.45:1984">>, Trusted)),
	Modules = maps:get([storage_modules], Leaf),
	?assert(is_list(Modules) andalso Modules =/= []),
	Webhooks = maps:get([webhooks], Leaf),
	?assertEqual(1, length(Webhooks)),
	ok.

legacy_fixture_path() ->
	fixture_path("legacy_config.json").

fixture_path(Name) ->
	BeamPath = case code:which(?MODULE) of
		non_existing -> ?FILE;
		LoadedPath when is_list(LoadedPath) -> LoadedPath
	end,
	filename:join([filename:dirname(BeamPath), "fixtures", Name]).

out_path(Config, Name) ->
	filename:join(proplists:get_value(priv_dir, Config), Name).

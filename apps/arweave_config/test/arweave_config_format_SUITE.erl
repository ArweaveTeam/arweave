%%% @doc Arweave Config Format Test Suite.
-module(arweave_config_format_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

init_per_testcase(_TestCase, Config) ->
	ok = arweave_config:start(),
	Config.

end_per_testcase(_TestCase, _Config) ->
	ok = arweave_config:stop().

all() ->
	[
		json,
		yaml,
		dotted_keys,
		dotted_key_with_map_value_rejected,
		dotted_key_with_list_of_maps_value_rejected,
		nested_dotted_key_kept_literal,
		dotted_peer_role_list_parses,
		yaml_empty_sequence_kept_as_list,
		yaml_encode
	].

%%====================================================================
%% Test cases
%%====================================================================

json(_Config) ->
	{ok, #{}} = arweave_config_format_json:parse(""),
	{ok, #{}} = arweave_config_format_json:parse(<<"">>),
	{ok, #{}} = arweave_config_format_json:parse(<<"{}">>),
	{ok, #{}} = arweave_config_format_json:parse("{}"),
	{error, _} = arweave_config_format_json:parse("--"),
	ok.

yaml(_Config) ->
	{ok, #{}} = arweave_config_format_yaml:parse(""),
	{ok, #{}} = arweave_config_format_yaml:parse(<<"">>),
	{ok, _} = arweave_config_format_yaml:parse(<<"test: 1\n">>),
	{ok, _} = arweave_config_format_yaml:parse("test: 1\n"),
	{error, _} = arweave_config_format_yaml:parse("--[]"),
	{error, _} = arweave_config_format_yaml:parse(<<"---\n---\n">>),
	ok.

%% Test the basic dotted key normalization.  Exhaustive config-file
%% format coverage lives in `arweave_config_full_load_SUITE`.
dotted_keys(_Config) ->
	Expected = #{
		[mining, enabled] => true,
		[mining, hashing_threads] => 4,
		[port] => 1985
	},
	?assertEqual(Expected, parse_json(#{
		<<"mining.enabled">> => true,
		<<"mining.hashing_threads">> => 4,
		<<"port">> => 1985
	})),
	?assertEqual(Expected, parse_yaml(<<"
\"mining.enabled\": true
\"mining.hashing_threads\": 4
port: 1985
">>)),

	MixedExpected = #{
		[mining, enabled] => true,
		[mining, hashing_threads] => 4,
		[port] => 1985
	},
	?assertEqual(MixedExpected, parse_json(#{
		<<"mining.enabled">> => true,
		<<"mining">> => #{ <<"hashing_threads">> => 4 },
		<<"port">> => 1985
	})),
	?assertEqual(MixedExpected, parse_yaml(<<"
\"mining.enabled\": true
mining:
  hashing_threads: 4
port: 1985
">>)),

	?assertEqual(#{ [mining, enabled] => true }, parse_json(#{
		<<"mining.enabled">> => true,
		<<"mining">> => #{ <<"enabled">> => true }
	})),
	?assertMatch(
		{error, #{ reason := conflicting_config_key }},
		arweave_config_format_json:parse(json_encode(#{
			<<"mining.enabled">> => true,
			<<"mining">> => #{ <<"enabled">> => false }
		}))),
	?assertMatch(
		{error, #{ reason := conflicting_config_key }},
		arweave_config_format_yaml:parse(<<"
\"mining.enabled\": true
mining:
  enabled: false
">>)),
	?assertMatch(
		{error, #{ reason := invalid_dotted_config_key }},
		arweave_config_format_json:parse(json_encode(#{
			<<"mining..enabled">> => true
		}))),
	ok.

%% Top-level dotted key must have a scalar (or list-of-scalars)
%% value. Nesting under it is the operator using the wrong shape.
dotted_key_with_map_value_rejected(_Config) ->
	?assertMatch(
		{error, #{ reason := dotted_config_key_has_nested_value }},
		arweave_config_format_json:parse(json_encode(#{
			<<"mining.enabled">> => #{ <<"foo">> => <<"bar">> }
		}))),
	ok.

dotted_key_with_list_of_maps_value_rejected(_Config) ->
	?assertMatch(
		{error, #{ reason := dotted_config_key_has_nested_value }},
		arweave_config_format_json:parse(json_encode(#{
			<<"webhooks.list">> => [
				#{ <<"url">> => <<"http://x">> },
				#{ <<"url">> => <<"http://y">> }
			]
		}))),
	ok.

%% Nested dotted-looking keys aren't expanded. The dotted form is
%% only available at the root.
nested_dotted_key_kept_literal(_Config) ->
	?assertEqual(
		#{[peers, trusted] => [<<"1.2.3.4:1984">>]},
		parse_json(#{
			<<"peers">> => #{
				<<"trusted">> => [<<"1.2.3.4:1984">>]
			}
		})),
	ok.

%% Peer roles are list-valued options, so root dotted keys can set a
%% whole role list directly.
dotted_peer_role_list_parses(_Config) ->
	?assertEqual(
		#{[peers, trusted] => [<<"1.2.3.4:1984">>]},
		parse_json(#{
			<<"peers.trusted">> => [<<"1.2.3.4:1984">>]
		})),
	ok.

%% An empty YAML sequence (`key: []') must stay an empty list, not
%% collapse to `<<>>'. Regression for empty list-valued options such as
%% an empty peer role failing config load with `invalid_peer'.
yaml_empty_sequence_kept_as_list(_Config) ->
	?assertEqual(
		#{
			[peers, block_gossip] => [<<"35.175.1.113:1984">>],
			[peers, local] => []
		},
		parse_yaml(<<"
peers:
  block_gossip:
    - \"35.175.1.113:1984\"
  local: []
">>)),
	ok.


yaml_encode(_Config) ->
	Input = #{
		enabled => true,
		nullable => null,
		with_number_string => <<"123">>,
		with_specials => <<"a:b#c">>,
		with_newline => <<"line1\nline2">>,
		peers => [
			#{ host => <<"1.2.3.4:1984">>, trusted => true }
		]
	},
	{ok, Yaml} = arweave_config_format_yaml:encode(Input),
	?assertEqual(nomatch, binary:match(Yaml, <<"\r">>)),
	?assertMatch({_, _}, binary:match(Yaml, <<"enabled: true\n">>)),
	?assertMatch({_, _}, binary:match(Yaml, <<"nullable: null\n">>)),
	?assertMatch({_, _}, binary:match(Yaml, <<"with_number_string: \"123\"\n">>)),
	?assertMatch({_, _}, binary:match(Yaml, <<"with_specials: \"a:b#c\"\n">>)),
	?assertMatch({_, _}, binary:match(Yaml, <<"with_newline: \"line1\\nline2\"\n">>)),
	?assertMatch({_, _}, binary:match(Yaml, <<"-\n">>)),
	?assertMatch({_, _}, binary:match(Yaml, <<"host: \"1.2.3.4:1984\"\n">>)),
	?assertMatch({_, _}, binary:match(Yaml, <<"trusted: true\n">>)),
	ok.

%%====================================================================
%% Helpers
%%====================================================================

parse_json(Map) ->
	{ok, Parsed} = arweave_config_format_json:parse(json_encode(Map)),
	Parsed.

parse_yaml(Yaml) ->
	{ok, Parsed} = arweave_config_format_yaml:parse(Yaml),
	Parsed.

json_encode(Map) ->
	iolist_to_binary(jiffy:encode(Map)).

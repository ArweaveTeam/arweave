%%% @doc arweave_config_format_cli parser test suite.
-module(arweave_config_format_cli_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

init_per_testcase(_TestCase, _Config) ->
	ok = arweave_config:start(),
	[].

end_per_testcase(_TestCase, _Config) ->
	ok = arweave_config:stop().

all() ->
	[
		parser
	].

%%====================================================================
%% Test cases
%%====================================================================

parser(_Config) ->
	assert_parse_sets([<<"--debug">>], [debug], true),
	assert_parse_sets([<<"--debug">>, <<"true">>], [debug], true),
	assert_parse_sets([<<"--debug">>, <<"false">>], [debug], false),
	assert_parse_sets([<<"--debug">>, <<"True">>], [debug], true),
	assert_parse_sets([<<"--debug">>, <<"TRUE">>], [debug], true),
	assert_parse_sets([<<"--debug">>, <<"FALSE">>], [debug], false),
	assert_parse_sets([<<"--port">>, <<"0">>], [port], 0),
	assert_parse_sets([<<"--port">>, <<"65535">>], [port], 65535),
	assert_parse_sets(["--mining.hashing_threads", 4],
		[mining, hashing_threads], 4),
	assert_parse_sets(
		[<<"--integer">>, <<"-65535">>],
		#{ long_arguments => #{ <<"--integer">> => #{
			type => integer,
			option_key => [storage_modules, <<"test">>, packing, difficulty]
		}}},
		[storage_modules, <<"test">>, packing, difficulty],
		-65535),
	assert_parse_sets(
		[<<"--integer=7">>],
		#{ long_arguments => #{ <<"--integer">> => #{
			type => integer,
			option_key => [storage_modules, <<"test">>, packing, difficulty]
		}}},
		[storage_modules, <<"test">>, packing, difficulty],
		7),

	{error, #{ reason := <<"bad_argument">> }} =
		arweave_config_format_cli:parse([<<"---bad-arg">>]),
	{error, #{ reason := <<"bad_argument">> }} =
		arweave_config_format_cli:parse([<<"----bad-arg">>]),

	{error, #{ reason := <<"unknown argument">> }} =
		arweave_config_format_cli:parse([<<"--unknown">>]),
	{error, #{ reason := <<"missing value">> }} =
		arweave_config_format_cli:parse([<<"--port">>]),
	{error, #{ reason := <<"bad value">> }} =
		arweave_config_format_cli:parse([<<"--port">>, <<"bad">>]),

	ok.

%%====================================================================
%% Helpers
%%====================================================================

assert_parse_sets(Args, Key, Expected) ->
	assert_parse_sets(Args, #{}, Key, Expected).

assert_parse_sets(Args, Opts, Key, Expected) ->
	arweave_config:with_test_config(fun() ->
		{ok, Map} = arweave_config_format_cli:parse(Args, Opts),
		ok = arweave_config:load(Map),
		?assertEqual(Expected, arweave_config:get(Key))
	end).

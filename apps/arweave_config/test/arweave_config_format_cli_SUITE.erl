%%% @doc arweave_config_format_cli parser test suite.
-module(arweave_config_format_cli_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

suite() ->
    [{timetrap, {seconds, 60}}].

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

init_per_testcase(_TestCase, _Config) ->
    ok = arweave_config:start(),
    [].

end_per_testcase(_TestCase, _Config) ->
    ok = arweave_config:stop().

all() ->
    [
        parser,
        typeless_option
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
        [<<"--integer">>, <<"7">>],
        #{ long_arguments => #{ <<"--integer">> => #{
            type => tcp_port,
            option_key => [port]
        }}},
        [port],
        7),
    assert_parse_sets(
        [<<"--integer=7">>],
        #{ long_arguments => #{ <<"--integer">> => #{
            type => tcp_port,
            option_key => [port]
        }}},
        [port],
        7),

    %% JSON-shaped values decode into real terms before type
    %% coercion, so list-typed options are settable from the CLI.
    assert_parse_sets(
        [<<"--peers.trusted">>, <<"[\"5.6.7.8:1984\", \"1.2.3.4:1984\"]">>],
        [peers, trusted],
        [{1, 2, 3, 4, 1984}, {5, 6, 7, 8, 1984}]),
    assert_parse_sets(
        [<<"--peers.trusted=[\"1.2.3.4:1984\"]">>],
        [peers, trusted],
        [{1, 2, 3, 4, 1984}]),
    %% A bare scalar still becomes a singleton list.
    assert_parse_sets(
        [<<"--peers.trusted">>, <<"1.2.3.4:1984">>],
        [peers, trusted],
        [{1, 2, 3, 4, 1984}]),
    %% A JSON-shaped value that fails to decode falls back to the raw
    %% string and is rejected by the option's type, not by JSON.
    {error, #{ reason := <<"bad value">> }} =
        arweave_config_format_cli:parse(
            [<<"--peers.trusted">>, <<"[\"1.2.3.4:1984\"">>]),

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

%% Options registered without a `type' must parse from the CLI and keep
%% their value, not abort boot.
typeless_option(_Config) ->
    assert_parse_sets([<<"--internal_api_secret">>, <<"a_long_enough_secret">>],
        [internal_api_secret], <<"a_long_enough_secret">>),
    assert_parse_sets([<<"--cm.api_secret=a_long_enough_secret">>],
        [cm, api_secret], <<"a_long_enough_secret">>),
    assert_parse_sets([<<"--pool.api_key">>, <<"pool-key">>],
        [pool, api_key], <<"pool-key">>),
    %% A `handle_set' callback converts the value on set.
    assert_parse_sets([<<"--vdf.algorithm">>, <<"hiopt_m4">>],
        [vdf, algorithm], hiopt_m4),
    assert_parse_sets([<<"--vdf.compute">>, <<"true">>], [vdf, compute], true),

    {error, #{ reason := <<"missing value">> }} =
        arweave_config_format_cli:parse([<<"--internal_api_secret">>]),

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

%%%===================================================================
%%% GNU General Public License, version 2 (GPL-2.0)
%%% The GNU General Public License (GPL-2.0)
%%% Version 2, June 1991
%%%
%%% ------------------------------------------------------------------
%%%
%%% @author Arweave Team
%%% @author Mathieu Kerjouan
%%% @copyright 2026 (c) Arweave
%%% @doc arweave configuration arguments parser suite.
%%% @end
%%%===================================================================
-module(arweave_config_arguments_SUITE).
-export([suite/0, description/0]).
-export([init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).
-export([all/0]).
-export([
	default/1,
	parser/1
]).
-include("arweave_config.hrl").
-include_lib("common_test/include/ct.hrl").

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
suite() -> [{userdata, [description()]}].

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
description() -> {description, "arweave config cli arguments interface"}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
init_per_suite(Config) -> Config.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
end_per_suite(_Config) -> ok.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
init_per_testcase(_TestCase, _Config) ->
	ct:pal(info, 1, "start arweave_config"),
	ok = arweave_config:start(),
	[].

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
end_per_testcase(_TestCase, _Config) ->
	ct:pal(info, 1, "stop arweave_config"),
	ok = arweave_config:stop().

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
all() ->
	[
		default,
		parser
	].

%%--------------------------------------------------------------------
%% @doc test `arweave_config_arguments' main interface.
%% @end
%%--------------------------------------------------------------------
default(_Config) ->
	ct:pal(test, 1, "check if arweave_config_arguments is alive"),
	true = is_process_alive(whereis(arweave_config_arguments)),

	ct:pal(test, 1, "send an unsupported message to the process"),
	ok = gen_server:call(arweave_config_arguments, '@random_test', 1000),
	ok = gen_server:cast(arweave_config_arguments, '@random_test'),
	_ = erlang:send(arweave_config_arguments, '@random_test'),

	ct:pal(test, 1, "check the default configuration"),
	[] = arweave_config_arguments:get(),

	ct:pal(test, 1, "by default, no arguments are present"),
	[] = arweave_config_arguments_legacy:get_args(),

	ct:pal(test, 1, "set debug arguments"),
	{ok, _} =
		arweave_config_arguments:set([
			"--debug"
		]),

	ct:pal(test, 1, "raw arguments are returned"),
	["--debug"] =
		arweave_config_arguments:get_args(),

	ct:pal(test, 1, "the configuration has been set"),
	[{#{ parameter_key := [debug] }, [true]}] =
		arweave_config_arguments:get(),

	ct:pal(test, 1, "load into arweave_config"),
	ok = arweave_config_arguments:load(),

	ct:pal(test, 1, "check arweave_config result"),
	{ok, true} = arweave_config:get([debug]),
	{comment, "arguments process tested"}.

%%--------------------------------------------------------------------
%% @doc test `arweave_config_arguments' parser.
%% @end
%%--------------------------------------------------------------------
parser(_Config) ->
	% Create a custom long arguments map
	LongArguments = #{
		<<"--boolean">> => #{
			type => boolean
		},
		<<"--integer">> => #{
			type => integer
		},
		<<"--integer.pos">> => #{
			type => pos_integer
		}
	},

	% create a custom short argmuents map
	ShortArguments = #{
		$b => #{
			type => boolean
		},
		$i => #{
			type => integer
		},
		$I => #{
			type => pos_integer
		}
	},
	Arguments = [
		% long arguments
		<<"--boolean">>,
		<<"--boolean">>, <<"true">>,
		<<"--boolean">>, <<"false">>,
		<<"--boolean">>, <<"True">>,
		<<"--boolean">>, <<"TRUE">>,
		<<"--boolean">>, <<"FALSE">>,
		<<"--integer">>, <<"-65535">>,
		<<"--integer">>, <<"-0">>,
		<<"--integer">>, <<"-65535">>,
		<<"--integer.pos">>, <<"0">>,
		<<"--integer.pos">>, <<"65535">>,

		% short arguments
		<<"-b">>,
		<<"-b">>, <<"true">>,
		<<"-b">>, <<"on">>,
		<<"-b">>, <<"off">>,
		<<"-b">>, <<"false">>,
		<<"-i">>, <<"65535">>,
		<<"-i">>, <<"0">>,
		<<"-i">>, <<"-65535">>,
		<<"-I">>, <<"0">>,
		<<"-I">>, <<"65535">>
	],

	% --peer 127.0.0.1 --vdf --trusted
	% --storage.module 1.unpacked --enabled

	Opts = #{
		long_arguments => LongArguments,
		short_arguments => ShortArguments
	},

	ct:pal(test, 1, "parse ~p", [Arguments]),
	Result = [
		{#{type => boolean},[true]},
		{#{type => boolean},[true]},
		{#{type => boolean},[false]},
		{#{type => boolean},[true]},
		{#{type => boolean},[true]},
		{#{type => boolean},[false]},
		{#{type => integer},[-65535]},
		{#{type => integer},[0]},
		{#{type => integer},[-65535]},
		{#{type => pos_integer},[0]},
		{#{type => pos_integer},[65535]},
		{#{type => boolean},[true]},
		{#{type => boolean},[true]},
		{#{type => boolean},[true]},
		{#{type => boolean},[false]},
		{#{type => boolean},[false]},
		{#{type => integer},[65535]},
		{#{type => integer},[0]},
		{#{type => integer},[-65535]},
		{#{type => pos_integer},[0]},
		{#{type => pos_integer},[65535]}
	],
	{ok, Result} = arweave_config_arguments:parse(Arguments, Opts),

	ct:pal(test, 1, "check bad arguments"),
	{error, #{ reason := <<"bad_argument">> }} =
		arweave_config_arguments:parse([<<"---bad-arg">>]),
	{error, #{ reason := <<"bad_argument">> }} =
		arweave_config_arguments:parse([<<"----bad-arg">>]),

	ct:pal(test, 1, "check unknown argument"),
	{error, #{ reason := <<"unknown argument">> }} =
		arweave_config_arguments:parse([<<"--unknown">>]),

	ct:pal(test, 1, "check missing value"),
	{error, #{ reason := <<"missing value">> }} =
		arweave_config_arguments:parse([<<"--data.directory">>]),

	{comment, "arguments parser tested"}.


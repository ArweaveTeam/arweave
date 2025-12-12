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
%%% @doc arweave configuration legacy parser test suite.
%%% @end
%%%===================================================================
-module(arweave_config_arguments_legacy_SUITE).
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
%% @doc test `arweave_config_arguments_legacy' main interface.
%% @end
%%--------------------------------------------------------------------
default(_Config) ->
	ct:pal(test, 1, "check if arweave_config_arguments is alive"),
	true = is_process_alive(whereis(arweave_config_arguments_legacy)),

	ct:pal(test, 1, "send an unsupported message to the process"),
	ok = gen_server:call(arweave_config_arguments_legacy, '@random_test', 1000),
	ok = gen_server:cast(arweave_config_arguments_legacy, '@random_test'),
	_ = erlang:send(arweave_config_arguments_legacy, '@random_test'),

	ct:pal(test, 1, "check the default configuration"),
	#config{} = arweave_config_arguments_legacy:get(),

	ct:pal(test, 1, "by default, no arguments are present"),
	[] = arweave_config_arguments_legacy:get_args(),

	ct:pal(test, 1, "set debug and init arguments"),
	{ok, _} =
		arweave_config_arguments_legacy:set([
			"debug",
			"init"
		]),

	ct:pal(test, 1, "raw arguments are returned"),
	["debug", "init"] =
		arweave_config_arguments_legacy:get_args(),

	ct:pal(test, 1, "the configuration has been set"),
	#config{
		debug = true,
		init = true
	} = arweave_config_arguments_legacy:get(),

	ct:pal(test, 1, "load into arweave_config_legacy"),
	{ok, _} = arweave_config_arguments_legacy:load(),

	ct:pal(test, 1, "check arweave_config_legacy result"),
	#config{
		debug = true,
		init = true
	} = arweave_config_legacy:get(),

	ct:pal(test, 1, "check arweave_config result"),
	{ok, true} = arweave_config:get([debug]),

	{comment, "arguments process tested"}.

%%--------------------------------------------------------------------
%% @doc test `arweave_config_arguments_legacy' parser interface.
%% @end
%%--------------------------------------------------------------------
parser(_Config) ->
	ct:pal(test, 1, "check parser"),
	{ok, _} = arweave_config_arguments_legacy:parse(["debug"]),
	{error, _} = arweave_config_arguments_legacy:parse(["wrong_arg"]),
	{comment, "arguments parser tested"}.

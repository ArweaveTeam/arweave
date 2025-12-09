%%%===================================================================
%%% GNU General Public License, version 2 (GPL-2.0)
%%% The GNU General Public License (GPL-2.0)
%%% Version 2, June 1991
%%%
%%% ------------------------------------------------------------------
%%%
%%% @author Arweave Team
%%% @author Mathieu Kerjouan
%%% @copyright 2025 (c) Arweave
%%% @doc
%%% @end
%%%===================================================================
-module(arweave_config_SUITE).
-export([suite/0, description/0]).
-export([init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).
-export([all/0]).
-export([arweave_config/1, arweave_config_legacy/1]).
-include("arweave_config.hrl").
-include_lib("common_test/include/ct.hrl").

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
suite() -> [{userdata, [description()]}].

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
description() -> {description, "arweave_config test main interface"}.

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
init_per_testcase(_TestCase, Config) ->
	ct:pal(info, 1, "start arweave_config"),
	ok = arweave_config:start(),
	Config.

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
		arweave_config,
		arweave_config_legacy
	].

%%--------------------------------------------------------------------
%% @doc test `arweave_config' interface
%% @end
%%--------------------------------------------------------------------
arweave_config(_Config) ->
	ct:pal(test, 1, "get an existing parameter"),
	{ok, _} = arweave_config:get([debug]),

	ct:pal(test, 1, "get a missing parameter"),
	{error, _} = arweave_config:get([missing, parameter]),

	ct:pal(test, 1, "get an existing parameter with default value"),
	_ = arweave_config:get([debug], true),

	ct:pal(test, 1, "get a missing parameter with default value"),
	1 = arweave_config:get([missing, parameter], 1),

	ct:pal(test, 1, "set an existing parameter"),
	{ok, DebugValue1, _} = arweave_config:set([debug], true),
	{ok, DebugValue1} = arweave_config:get([debug]),

	ct:pal(test, 1, "unset an existing parameter"),
	{ok, DebugValue2, _} = arweave_config:set([debug], false),
	{ok, DebugValue2} = arweave_config:get([debug]),
	ok.

%%--------------------------------------------------------------------
%% @doc test `arweave_config' legacy interface.
%% @end
%%--------------------------------------------------------------------
arweave_config_legacy(_Config) ->
	% legacy compatible interface, to remove
	ct:pal(test, 1, "init legacy environment"),
	ok = arweave_config:set_env(#config{}),

	% legacy compatible interface, to remove
	ct:pal(test, 1, "get legacy environment"),
	{ok, Config1} = arweave_config:get_env(),
	false = Config1#config.init,

	% legacy compatible interface, to remove
	ct:pal(test, 1, "set legacy environment"),
	ok = arweave_config:set_env(#config{ init = true }),
	{ok, Config2} = arweave_config:get_env(),
	true = Config2#config.init,

	% check runtime mode
	false = arweave_config:is_runtime(),

	ct:pal(test, 1, "switch to runtime mode"),
	ok = arweave_config:runtime(),
	true = arweave_config:is_runtime(),

	{comment, "arweave_config interface tested"}.


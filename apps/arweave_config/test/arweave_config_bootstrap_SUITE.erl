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
-module(arweave_config_bootstrap_SUITE).
-export([suite/0, description/0]).
-export([init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).
-export([all/0]).
-export([
	legacy_mode/1,
	new_mode/1
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
description() -> {description, "arweave config parameters bootstrap"}.

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
init_per_testcase(new_mode, _Config) ->
	ct:pal(info, 1, "start arweave_config"),
	ok = arweave_config:start(),
	ct:pal(test, 1, "set AR_CONFIG_MODE environment"),
	os:putenv("AR_CONFIG_MODE", "new"),
	[];
init_per_testcase(_TestCase, _Config) ->
	ct:pal(info, 1, "start arweave_config"),
	ok = arweave_config:start(),
	ct:pal(test, 1, "ensure ARWEAVE_CONFIG_MODE is unset"),
	os:putenv("AR_CONFIG_MODE", ""),
	[].

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
end_per_testcase(new_mode, _Config) ->
	ct:pal(info, 1, "stop arweave_config"),
	ok = arweave_config:stop(),
	ct:pal(test, 1, "reset AR_CONFIG_MODE environment"),
	os:putenv("AR_CONFIG_MODE", "");
end_per_testcase(_TestCase, _Config) ->
	ct:pal(info, 1, "stop arweave_config"),
	ok = arweave_config:stop().

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
all() ->
	[
		legacy_mode,
		new_mode
	].

%%--------------------------------------------------------------------
%% @doc complete legacy mode test.
%% @end
%%--------------------------------------------------------------------
legacy_mode(_Config) ->
	ct:pal(test, 1, "legacy arguments must succeed"),
	{ok, Valid} = arweave_config_bootstrap:start(["init"]),
	% true = arweave_config:get([]),
	#config{ init = true } = Valid,

	{comment, ""}.

%%--------------------------------------------------------------------
%% @doc complete new mode test.
%% @end
%%--------------------------------------------------------------------
new_mode(_Config) ->
	ct:pal(test, 1, "new arguments must succeed (debug true)"),
	{ok, Valid1} = arweave_config_bootstrap:start(["--debug"]),
	{ok, true} = arweave_config:get([debug]),
	#config{ debug = true } = Valid1,

	ct:pal(test, 1, "new arguments must succeed (debug false)"),
	{ok, Valid2} = arweave_config_bootstrap:start(["--debug", "false"]),
	{ok, false} = arweave_config:get([debug]),
	#config{ debug = false } = Valid2,

	ct:pal(test, 1, "legacy arguments must fail"),
	{error, _Invalid} = arweave_config_bootstrap:start(["debug"]),

	{comment, ""}.

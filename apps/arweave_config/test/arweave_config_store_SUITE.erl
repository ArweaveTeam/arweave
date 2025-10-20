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
-module(arweave_config_store_SUITE).
-export([suite/0, description/0]).
-export([init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).
-export([all/0]).
-export([arweave_config_store/1]).
-include("arweave_config.hrl").
-include_lib("common_test/include/ct.hrl").

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
suite() -> [{userdata, [description()]}].

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
description() -> {description, "arweave configuration store interface"}.

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
	ct:pal(info, 1, "start arweave_config_store"),
	{ok, Pid} = arweave_config_store:start_link(),
	[{arweave_config_store, Pid}|Config].

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
end_per_testcase(_TestCase, _Config) ->
	ct:pal(info, 1, "stop arweave_config_store"),
	ok = arweave_config_store:stop().

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
all() ->
	[ arweave_config_store ].

%%--------------------------------------------------------------------
%% @doc test `arweave_config_store' storage interface.
%% @end
%%--------------------------------------------------------------------
arweave_config_store(_Config) ->
	ct:pal(test, 1, "check undefined parameter"),
	{error, undefined} = arweave_config_store:get("test"),

	ct:pal(test, 1, "try to delete an undefined parameter"),
	{error, undefined} = arweave_config_store:delete("test"),

	ct:pal(test, 1, "ensure default parameter is working"),
	default = arweave_config_store:get("test", default),

	ct:pal(test, 1, "set a new parameter"),
	{ok, {[test], data}} = arweave_config_store:set("test", data),

	ct:pal(test, 1, "get an existing parameter"),
	{ok, data} = arweave_config_store:get("test"),

	ct:pal(test, 1, "delete an existing parameter"),
	{ok, {[test], data}} = arweave_config_store:delete("test"),

	ct:pal(test, 1, "ensure the paramater was removed"),
	{error, undefined} = arweave_config_store:get("test"),

	{comment, "arweave_config_store process tested"}.
